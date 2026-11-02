import {
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  S3Client,
  ServiceOutputTypes,
  UploadPartCommand,
} from '@aws-sdk/client-s3';
import { Kysely } from 'kysely';
import { PostgresJSDialect } from 'kysely-postgres-js';
import { createHash, randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import { lstat, mkdir, open, realpath } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { Readable, Transform, addAbortSignal } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import postgres from 'postgres';
import { ObjectCatalogRepository } from 'src/repositories/object-catalog.repository.js';
import { closeObjectStreams } from 'src/repositories/object-store.repository.js';
import { S3ObjectStoreRepository } from 'src/repositories/s3-object-store.repository.js';
import { DB } from 'src/schema/index.js';
import { up } from 'src/schema/migrations/1793500000000-AddObjectCatalog.js';
import { up as upMultipart } from 'src/schema/migrations/1793500001000-AddS3MultipartJournal.js';
import { ObjectCatalogService } from 'src/services/object-catalog.service.js';
import { QualificationConfig, parseQualificationArgs, qualificationPlan } from 'src/utils/b2-qualification.js';

export interface CatalogQualificationConfig extends QualificationConfig {
  pgSocket: string;
  pgPort: number;
  disposableDatabaseOptIn: boolean;
}

export const catalogQualificationPlan = (config: CatalogQualificationConfig) => {
  const plan = qualificationPlan(config);
  if (
    !config.disposableDatabaseOptIn ||
    !path.isAbsolute(config.pgSocket) ||
    path.resolve(config.pgSocket) !== config.pgSocket ||
    !Number.isSafeInteger(config.pgPort) ||
    config.pgPort < 1024 ||
    config.pgPort > 65_535 ||
    config.maxRequests !== undefined ||
    config.maxBytes !== undefined ||
    config.maxRuntimeMs !== undefined
  )
    throw new Error('InvalidCatalogConfig');
  return { ...plan, maxRequests: 24, maxBytes: 64 * 1024 ** 2, maxRuntimeMs: 180_000 };
};

export const parseCatalogQualificationArgs = (args: string[]): CatalogQualificationConfig => {
  const base: string[] = [];
  let pgSocket = '';
  let pgPort = 0;
  let disposableDatabaseOptIn = false;
  const seen = new Set<string>();
  for (let i = 0; i < args.length; i++) {
    const flag = args[i];
    if (['--disposable-local-pg-opt-in', '--pg-socket', '--pg-port'].includes(flag)) {
      if (seen.has(flag)) throw new Error('InvalidArguments');
      seen.add(flag);
      if (flag === '--disposable-local-pg-opt-in') disposableDatabaseOptIn = true;
      else {
        const value = args[++i];
        if (!value || value.startsWith('--')) throw new Error('InvalidArguments');
        if (flag === '--pg-socket') pgSocket = value;
        else {
          if (!/^[1-9]\d{3,4}$/.test(value)) throw new Error('InvalidArguments');
          pgPort = Number(value);
        }
      }
    } else base.push(flag);
  }
  const config = { ...parseQualificationArgs(base), pgSocket, pgPort, disposableDatabaseOptIn };
  catalogQualificationPlan(config);
  return config;
};

type AuditEvent = Record<string, unknown>;
interface Dependencies {
  client?: S3Client;
  signal?: AbortSignal;
  beforePersist?: (event: Readonly<AuditEvent>) => void | Promise<void>;
  beforeObservationPersist?: () => void | Promise<void>;
}

interface ReceivedObservation {
  key: string;
  receivedCreateUploadId?: string;
  receivedCompleteVersionId?: string;
}

type Command =
  | CreateMultipartUploadCommand
  | UploadPartCommand
  | CompleteMultipartUploadCommand
  | GetObjectCommand
  | DeleteObjectCommand;
const opaque = (value: unknown): value is string =>
  typeof value === 'string' && value !== 'null' && /^[\u{21}-\u{7E}]{1,1024}$/u.test(value);
const requireCondition: (value: unknown) => asserts value = (value) => {
  if (!value) throw new Error('CatalogQualificationInvariant');
};

/** A one-object protocol fence. No abort/list/retry/recovery surface is exposed. */
export class CatalogQualificationGuard {
  private observation?: ReceivedObservation;
  private observationPersisted = false;

  get receivedObservation() {
    return this.observation ? { ...this.observation, persisted: this.observationPersisted } : undefined;
  }

  private key?: string;
  private uploadId?: string;
  private versionId?: string;
  private created = false;
  private completed = false;
  private read = false;
  private deletionAllowed = false;
  private deleted = false;
  private absenceRead = false;
  private frozen = false;
  private parts: { PartNumber: number; ETag: string }[] = [];
  private streams: Readable[] = [];
  private pumping: Promise<void>[] = [];
  requests = 0;
  bytes = 0;
  readonly client: S3Client;

  constructor(
    private config: CatalogQualificationConfig,
    client: S3Client,
    private append: (event: AuditEvent) => Promise<void>,
    private signal: AbortSignal,
    private preserveObservation?: (observation: Readonly<ReceivedObservation>) => Promise<void>,
  ) {
    catalogQualificationPlan(config);
    this.config = Object.freeze({ ...config });
    // Narrow composition leaves the actual SDK serialization/signing/transport intact.
    this.client = new Proxy(client, {
      get: (target, property) =>
        property === 'send'
          ? (command: Command, options?: { abortSignal?: AbortSignal }) => this.send(target, command, options)
          : Reflect.get(target, property, target),
    });
  }

  async persist(event: AuditEvent) {
    if (this.frozen) throw new Error('ManifestFrozen');
    try {
      await this.append(event);
    } catch (error) {
      this.frozen = true;
      throw error;
    }
  }

  async enroll(key: string) {
    if (
      this.key ||
      !key.startsWith(this.config.prefix) ||
      key.split('/').length !== 4 ||
      !/^[0-9a-f-]{36}\/[0-9a-f-]{36}$/.test(key.slice(this.config.prefix.length))
    )
      throw new Error('KeyScope');
    await this.persist({ type: 'allow-key', key });
    this.key = key;
  }

  async allowDeletion() {
    requireCondition(this.read && this.versionId && !this.frozen);
    await this.persist({ type: 'delete-authorized', key: this.key, versionId: this.versionId });
    this.deletionAllowed = true;
  }

  private async send(client: S3Client, command: Command, options?: { abortSignal?: AbortSignal }) {
    if (this.frozen) throw new Error('ManifestFrozen');
    this.signal.throwIfAborted();
    const input = command.input;
    requireCondition(this.key && input.Bucket === this.config.bucket && input.Key === this.key);
    const name = command.constructor.name;
    if (command instanceof CreateMultipartUploadCommand) requireCondition(!this.created && !this.uploadId);
    else if (command instanceof UploadPartCommand) {
      requireCondition(!this.completed && this.uploadId && command.input.UploadId === this.uploadId);
      const expectedSize = this.parts.length < 2 ? 8 * 1024 ** 2 : 1024 ** 2;
      requireCondition(
        this.parts.length < 3 &&
          command.input.PartNumber === this.parts.length + 1 &&
          command.input.ContentLength === expectedSize &&
          Buffer.isBuffer(command.input.Body) &&
          command.input.Body.length === expectedSize,
      );
    } else if (command instanceof CompleteMultipartUploadCommand) {
      requireCondition(
        !this.completed &&
          this.uploadId &&
          command.input.UploadId === this.uploadId &&
          this.parts.length === 3 &&
          JSON.stringify(command.input.MultipartUpload?.Parts) === JSON.stringify(this.parts),
      );
    } else if (command instanceof GetObjectCommand) {
      requireCondition(
        this.versionId &&
          command.input.VersionId === this.versionId &&
          ((!this.read && !this.deleted && command.input.Range === undefined) ||
            (this.deleted && !this.absenceRead && command.input.Range === 'bytes=0-0')),
      );
    } else if (command instanceof DeleteObjectCommand) {
      requireCondition(
        this.deletionAllowed && !this.deleted && this.versionId && command.input.VersionId === this.versionId,
      );
    } else throw new Error('CommandDenied');
    // Explicit reserve: upload/verification cannot consume the last two requests or last 1MiB.
    const cleanup = command instanceof DeleteObjectCommand || this.deleted;
    requireCondition(this.requests < (cleanup ? 24 : 22));
    const payloadBytes = command instanceof UploadPartCommand ? command.input.ContentLength! : 0;
    requireCondition(this.bytes + payloadBytes <= (cleanup ? 64 : 63) * 1024 ** 2);
    await this.persist({
      type: 'remote-intent',
      command: name,
      key: this.key,
      uploadId: this.uploadId,
      versionId: this.versionId,
      partNumber: 'PartNumber' in input ? input.PartNumber : undefined,
    });
    this.signal.throwIfAborted();
    this.requests++;
    this.bytes += payloadBytes;
    if (command instanceof CreateMultipartUploadCommand) this.created = true;
    if (command instanceof CompleteMultipartUploadCommand) this.completed = true;
    if (command instanceof GetObjectCommand) {
      if (this.deleted) this.absenceRead = true;
      else this.read = true;
    }
    const signal = options?.abortSignal ? AbortSignal.any([options.abortSignal, this.signal]) : this.signal;
    let result: ServiceOutputTypes;
    try {
      /* eslint-disable unicorn/no-duplicate-if-branches -- SDK send overloads require concrete command narrowing. */
      // Concrete overloads avoid weakening the SDK's command typing at this boundary.
      if (command instanceof CreateMultipartUploadCommand) result = await client.send(command, { abortSignal: signal });
      else if (command instanceof UploadPartCommand) result = await client.send(command, { abortSignal: signal });
      else if (command instanceof CompleteMultipartUploadCommand)
        result = await client.send(command, { abortSignal: signal });
      else if (command instanceof GetObjectCommand) result = await client.send(command, { abortSignal: signal });
      else result = await client.send(command, { abortSignal: signal });
      /* eslint-enable unicorn/no-duplicate-if-branches */
    } catch (error) {
      const missing =
        command instanceof GetObjectCommand &&
        this.deleted &&
        (error as { $metadata?: { httpStatusCode?: number } }).$metadata?.httpStatusCode === 404 &&
        ['NoSuchVersion', 'NoSuchKey'].includes((error as { name: string }).name);
      await this.persist({ type: missing ? 'absence-receipt' : 'remote-uncertain', command: name });
      if (!missing) this.frozen = true;
      throw error;
    }
    const body = 'Body' in result && result.Body instanceof Readable ? result.Body : undefined;
    if (body) {
      // Own errors before the first await: ACK persistence is itself a failure/cancellation barrier.
      body.on('error', () => {});
      addAbortSignal(signal, body);
      this.streams.push(body);
    }
    try {
      // IDs are observations until fsync succeeds. Only then can they authorize another call.
      const ack = {
        type: 'remote-ack',
        command: name,
        uploadId: 'UploadId' in result ? result.UploadId : undefined,
        versionId: 'VersionId' in result ? result.VersionId : undefined,
        etag: 'ETag' in result ? result.ETag : undefined,
      };
      if (command instanceof CreateMultipartUploadCommand || command instanceof CompleteMultipartUploadCommand) {
        const received = command instanceof CreateMultipartUploadCommand ? ack.uploadId : ack.versionId;
        requireCondition(opaque(received));
        this.observation = {
          ...this.observation,
          key: this.key!,
          ...(command instanceof CreateMultipartUploadCommand
            ? { receivedCreateUploadId: received }
            : { receivedCompleteVersionId: received }),
        };
        this.observationPersisted = false;
        if (this.preserveObservation) {
          try {
            await this.preserveObservation(Object.freeze({ ...this.observation }));
            this.observationPersisted = true;
          } catch (error) {
            this.frozen = true;
            throw error;
          }
        }
      }
      await this.persist(ack);
      if (command instanceof CreateMultipartUploadCommand) {
        requireCondition(opaque(ack.uploadId));
        this.uploadId = ack.uploadId;
      }
      if (command instanceof UploadPartCommand) {
        requireCondition(opaque(ack.etag));
        this.parts.push({ PartNumber: command.input.PartNumber!, ETag: ack.etag });
      }
      if (command instanceof CompleteMultipartUploadCommand) {
        requireCondition(opaque(ack.versionId) && opaque(ack.etag));
        this.versionId = ack.versionId;
      }
      if (command instanceof DeleteObjectCommand) this.deleted = true;
      if (command instanceof GetObjectCommand) {
        requireCondition(body);
        const limit = this.deleted ? 1 : 17 * 1024 ** 2;
        let count = 0;
        const bounded = new Transform({
          transform: (chunk: Buffer, _encoding, callback) => {
            count += chunk.length;
            this.bytes += chunk.length;
            callback(
              count > limit || chunk.length > 1024 ** 2 || this.bytes > 63 * 1024 ** 2 ? new Error('ReadBudget') : null,
              chunk,
            );
          },
        });
        this.streams.push(bounded);
        const pumping = pipeline(body, bounded).catch(() => {});
        this.pumping.push(pumping);
        // SDK output remains otherwise unchanged; the real provider consumes this bounded stream.
        Object.assign(result, { Body: bounded });
      }
      return result;
    } catch (error) {
      if (body) await closeObjectStreams(body);
      throw error;
    }
  }

  async settle() {
    await closeObjectStreams(...this.streams);
    await Promise.all(this.pumping);
  }
}

async function trustedDirectory(directory: string, privateOnly: boolean) {
  requireCondition((await realpath(directory)) === directory);
  const info = await lstat(directory);
  requireCondition(
    info.isDirectory() &&
      info.uid === process.getuid!() &&
      (privateOnly ? (info.mode & 0o777) === 0o700 : (info.mode & 0o022) === 0),
  );
}

export const runCatalogQualification = async (config: CatalogQualificationConfig, deps: Dependencies = {}) => {
  const plan = catalogQualificationPlan(config);
  if (!plan.live) return { status: 'dry-plan', plan };
  requireCondition(deps.client);
  deps.signal?.throwIfAborted();
  await trustedDirectory(config.pgSocket, true);
  const socket = await lstat(path.join(config.pgSocket, `.s.PGSQL.${config.pgPort}`));
  requireCondition(socket.isSocket() && socket.uid === process.getuid!());
  await trustedDirectory(path.dirname(config.workspace), false);
  await mkdir(config.workspace, { mode: 0o700 });
  const manifest = await open(
    path.join(config.workspace, 'manifest.jsonl'),
    constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_APPEND | constants.O_NOFOLLOW,
    0o600,
  );
  let observations: Awaited<ReturnType<typeof open>> | undefined;
  let db: Kysely<DB> | undefined;
  let admin: ReturnType<typeof postgres> | undefined;
  let guard: CatalogQualificationGuard | undefined;
  let phase = 'setup';
  let sequence = 0;
  const controller = new AbortController();
  const signal = deps.signal ? AbortSignal.any([controller.signal, deps.signal]) : controller.signal;
  const timer = setTimeout(() => controller.abort(new Error('RuntimeBudget')), plan.maxRuntimeMs);
  const append = async (event: AuditEvent) => {
    await deps.beforePersist?.(Object.freeze({ ...event }));
    await manifest.appendFile(`${JSON.stringify({ sequence: ++sequence, ...event })}\n`);
    await manifest.sync();
  };
  const syncDirectory = async (directory: string) => {
    const handle = await open(directory, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
    try {
      await handle.sync();
    } finally {
      await handle.close();
    }
  };
  try {
    observations = await open(
      path.join(config.workspace, 'received-observations.jsonl'),
      constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_APPEND | constants.O_NOFOLLOW,
      0o600,
    );
    await observations.sync();
    await syncDirectory(config.workspace);
    await syncDirectory(path.dirname(config.workspace));
    guard = new CatalogQualificationGuard(config, deps.client, append, signal, async (observation) => {
      await deps.beforeObservationPersist?.();
      await observations!.appendFile(`${JSON.stringify(observation)}\n`);
      await observations!.sync();
    });
    requireCondition((await deps.client.config.maxAttempts()) === 1);
    requireCondition((await deps.client.config.requestChecksumCalculation()) === 'WHEN_REQUIRED');
    requireCondition((await deps.client.config.responseChecksumValidation()) === 'WHEN_REQUIRED');
    const database = `immich_catalog_live_test_${randomUUID().replaceAll('-', '')}`;
    await guard.persist({ type: 'database-intent', database, socket: config.pgSocket, port: config.pgPort });
    const options = {
      host: config.pgSocket,
      port: config.pgPort,
      user: os.userInfo().username,
      max: 1,
      connect_timeout: 5,
      idle_timeout: 5,
      connection: { statement_timeout: 10_000 },
    };
    admin = postgres({ ...options, database: 'postgres' });
    signal.throwIfAborted();
    await admin.unsafe(`CREATE DATABASE "${database}"`);
    db = new Kysely<DB>({ dialect: new PostgresJSDialect({ postgres: postgres({ ...options, database }) }) });
    await up(db);
    await upMultipart(db);
    await guard.persist({ type: 'database-ready', database });
    const root = path.join(config.workspace, 'stage');
    await mkdir(root, { mode: 0o700 });
    const source = path.join(config.workspace, 'source');
    const file = await open(
      source,
      constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW,
      0o600,
    );
    const hash = createHash('sha256');
    try {
      const chunk = Buffer.alloc(1024 ** 2, 29);
      for (let index = 0; index < 17; index++) {
        signal.throwIfAborted();
        await file.writeFile(chunk);
        hash.update(chunk);
      }
      await file.chmod(0o400);
      await file.sync();
    } finally {
      await file.close();
    }
    await syncDirectory(config.workspace);
    const repository = new ObjectCatalogRepository(db, { objectKeyPrefix: config.prefix.slice(0, -1) });
    const service = new ObjectCatalogService(repository, root);
    const stage = await service.stage(source);
    requireCondition(stage.hash === hash.digest('hex') && Number(stage.size) === plan.sampleSize);
    const store = new S3ObjectStoreRepository(guard.client, {
      bucket: config.bucket,
      multipart: {
        threshold: plan.partSize,
        partSize: plan.partSize,
        maxObjectSize: plan.sampleSize,
        cleanupTimeoutMs: 1000,
      },
    });
    const binding = { provider: 's3' as const, storeId: config.bucket, store };
    const intent = await repository.prepare(stage.id, binding.provider, binding.storeId);
    requireCondition(intent.key === `${config.prefix}${stage.id}/${intent.id}`);
    await guard.enroll(intent.key);
    await guard.persist({ type: 'prepared', stageId: stage.id, intentId: intent.id, hash: stage.hash });
    phase = 'upload';
    const version = await service.sendPrepared(intent.id, binding);
    const [journal] = await repository.listMultipart(stage.id);
    const acknowledgedIntent = await repository.getIntent(intent.id);
    requireCondition(
      acknowledgedIntent.state === 'acknowledged' &&
        journal.state === 'received' &&
        journal.key === intent.key &&
        journal.response?.versionId === version.versionId &&
        journal.manifest.length === 3 &&
        version.verification !== 'verified',
    );
    await repository.releaseStage(stage.id);
    requireCondition((await service.cleanup(stage.id)) === false);
    await guard.persist({ type: 'cleanup-blocked', stageId: stage.id });
    phase = 'verify';
    requireCondition(await service.verify(stage.id, version.id, binding));
    const [verifiedVersion] = await repository.listVersions(stage.id);
    requireCondition(verifiedVersion.verification === 'verified');
    signal.throwIfAborted();
    phase = 'stage-cleanup';
    await guard.persist({ type: 'stage-cleanup-intent', stageId: stage.id });
    requireCondition(await service.cleanup(stage.id));
    const deletedStage = await repository.getStage(stage.id);
    requireCondition(deletedStage?.state === 'deleted');
    phase = 'exact-delete';
    await guard.allowDeletion();
    await store.delete({ key: version.key, versionId: version.versionId! });
    phase = 'absence';
    const remaining = await store.get(
      { key: version.key, versionId: version.versionId! },
      { range: { start: 0, end: 0 }, signal },
    );
    if (remaining) {
      await closeObjectStreams(remaining.stream);
      throw new Error('ExactVersionStillPresent');
    }
    await guard.persist({ type: 'qualified', database, stageId: stage.id });
    return { status: 'catalog-synthetic-qualified', requests: guard.requests, bytes: guard.bytes };
  } catch {
    // No cleanup on uncertainty. Neither raw SDK errors nor opaque handles leave the private evidence store.
    await guard?.persist({ type: 'held', phase }).catch(() => {});
    return {
      status: 'held',
      phase,
      requests: guard?.requests ?? 0,
      bytes: guard?.bytes ?? 0,
      receivedObservation: guard?.receivedObservation,
    };
  } finally {
    clearTimeout(timer);
    controller.abort();
    await guard?.settle();
    await db?.destroy();
    await admin?.end({ timeout: 5 });
    await observations?.close();
    await manifest.close();
  }
};
