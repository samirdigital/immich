import {
  AbortMultipartUploadCommand,
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  type GetObjectCommandOutput,
  HeadBucketCommand,
  HeadObjectCommand,
  ListPartsCommand,
  S3Client,
  UploadPartCommand,
} from '@aws-sdk/client-s3';
import { createHash } from 'node:crypto';
import { constants } from 'node:fs';
import { type FileHandle, mkdir, open, realpath } from 'node:fs/promises';
import { Agent } from 'node:https';
import path from 'node:path';
import { Readable } from 'node:stream';

export interface QualificationConfig {
  bucket: string;
  endpoint: string;
  region: string;
  prefix: string;
  workspace: string;
  syntheticTestOptIn?: boolean;
  maxRequests?: number;
  maxBytes?: number;
  maxRuntimeMs?: number;
}

export const qualificationPlan = (config: QualificationConfig) => {
  if (
    !/^[a-z0-9][a-z0-9-]{4,61}[a-z0-9]$/.test(config.bucket) ||
    !/^[a-z]{2}-[a-z]+-\d{3}$/.test(config.region) ||
    config.endpoint !== `https://s3.${config.region}.backblazeb2.com` ||
    !/^immich-b2-qualification\/[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\/$/.test(
      config.prefix,
    ) ||
    !path.isAbsolute(config.workspace) ||
    path.resolve(config.workspace) !== config.workspace
  ) {
    throw new Error('InvalidConfig');
  }
  return {
    live: config.syntheticTestOptIn === true,
    prefix: config.prefix,
    partSize: 8 * 1024 ** 2,
    sampleSize: 17 * 1024 ** 2,
  };
};

type ProbeCommand =
  | AbortMultipartUploadCommand
  | CompleteMultipartUploadCommand
  | CreateMultipartUploadCommand
  | DeleteObjectCommand
  | GetObjectCommand
  | HeadObjectCommand
  | HeadBucketCommand
  | ListPartsCommand
  | UploadPartCommand;

interface Credentials {
  accessKeyId: string;
  secretAccessKey: string;
}

export const parseQualificationArgs = (args: string[]): QualificationConfig => {
  const values: Record<string, string | boolean> = {};
  const allowed = new Set(['bucket', 'endpoint', 'region', 'prefix', 'workspace']);
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--synthetic-test-opt-in' && values.syntheticTestOptIn === undefined) {
      values.syntheticTestOptIn = true;
      continue;
    }
    const key = args[i].slice(2);
    if (
      !args[i].startsWith('--') ||
      !allowed.has(key) ||
      values[key] !== undefined ||
      !args[i + 1] ||
      args[i + 1].startsWith('--')
    )
      throw new Error('InvalidArguments');
    values[key] = args[++i];
  }
  if ([...allowed].some((key) => typeof values[key] !== 'string')) throw new Error('InvalidArguments');
  const config = values as unknown as QualificationConfig;
  qualificationPlan(config);
  return config;
};

/** Consume one bounded JSON line (or EOF payload), then close input immediately. */
export const readQualificationCredentials = async (input: Readable): Promise<Credentials> => {
  const timeout = setTimeout(() => input.destroy(new Error('InvalidCredentials')), 10_000);
  let payload = '';
  try {
    for await (const chunk of input) {
      payload += Buffer.isBuffer(chunk) ? chunk.toString('utf8') : String(chunk);
      if (Buffer.byteLength(payload) > 8192) throw new Error('InvalidCredentials');
      if (payload.includes('\n')) {
        const [line, ...tail] = payload.split('\n');
        if (tail.join('').trim()) throw new Error('InvalidCredentials');
        payload = line;
        break;
      }
    }
    const parsed = JSON.parse(payload) as Record<string, unknown>;
    if (
      !parsed ||
      Object.keys(parsed).sort().join(',') !== 'accessKeyId,secretAccessKey' ||
      typeof parsed.accessKeyId !== 'string' ||
      typeof parsed.secretAccessKey !== 'string' ||
      !/^[\u{21}-\u{7E}]{1,256}$/u.test(parsed.accessKeyId) ||
      !/^[\u{21}-\u{7E}]{1,256}$/u.test(parsed.secretAccessKey)
    )
      throw new Error('InvalidCredentials');
    return { accessKeyId: parsed.accessKeyId, secretAccessKey: parsed.secretAccessKey };
  } catch {
    throw new Error('InvalidCredentials');
  } finally {
    clearTimeout(timeout);
    input.destroy();
  }
};

export const createQualificationClient = (config: QualificationConfig, credentials: Credentials) => {
  qualificationPlan(config);
  return new S3Client({
    endpoint: config.endpoint,
    region: config.region,
    credentials,
    maxAttempts: 1,
    followRegionRedirects: false,
    forcePathStyle: true,
    ignoreConfiguredEndpointUrls: true,
    useFipsEndpoint: false,
    useDualstackEndpoint: false,
    requestChecksumCalculation: 'WHEN_REQUIRED',
    responseChecksumValidation: 'WHEN_REQUIRED',
    requestHandler: {
      httpsAgent: new Agent({ rejectUnauthorized: true, maxSockets: 1 }),
      connectionTimeout: 10_000,
      requestTimeout: 30_000,
      socketTimeout: 30_000,
    },
  });
};

interface Handle {
  key: string;
  uploadId?: string;
  versionId?: string;
  unknown?: string;
}
type Event = Record<string, unknown>;
interface Dependencies {
  client: Pick<S3Client, 'send'>;
  signal?: AbortSignal;
  beforePersist?: (event: Readonly<Event>) => Promise<void> | void;
}

const missing = (error: unknown, names: string[]) => {
  const e = error as { name?: string; $metadata?: { httpStatusCode?: number } };
  return e?.$metadata?.httpStatusCode === 404 && names.includes(e.name ?? '');
};
const opaque = (value: unknown): value is string =>
  typeof value === 'string' && value !== 'null' && /^[\u{21}-\u{7E}]{1,1024}$/u.test(value);
const hash = (data: Buffer) => createHash('sha256').update(data).digest('hex');

/** One-shot protocol probe, deliberately not a resumable cleanup/replay engine. */
export const runQualification = async (config: QualificationConfig, deps: Dependencies) => {
  const plan = qualificationPlan(config);
  if (!plan.live) return { status: 'dry-plan', remaining: [] as Handle[], plan };
  const maxRequests = config.maxRequests ?? 24;
  const maxBytes = config.maxBytes ?? 64 * 1024 ** 2;
  const maxRuntimeMs = config.maxRuntimeMs ?? 180_000;
  if (
    !Number.isSafeInteger(maxRequests) ||
    maxRequests < 20 ||
    maxRequests > 40 ||
    !Number.isSafeInteger(maxBytes) ||
    maxBytes < 50 * 1024 ** 2 ||
    maxBytes > 128 * 1024 ** 2 ||
    !Number.isSafeInteger(maxRuntimeMs) ||
    maxRuntimeMs < 10_000 ||
    maxRuntimeMs > 300_000
  )
    throw new Error('Budget');
  deps.signal?.throwIfAborted();
  const parent = path.dirname(config.workspace);
  if ((await realpath(parent)) !== parent) throw new Error('UnsafeWorkspace');
  await mkdir(config.workspace, { mode: 0o700 }); // Exclusive: no resume, reuse, or symlink targets.
  let file: FileHandle | undefined;
  let directory: FileHandle | undefined;
  let parentDirectory: FileHandle | undefined;
  const remaining: Handle[] = [];
  let durable = true;
  let phase = 'workspace';
  let requests = 0;
  let bytes = 0;
  const started = performance.now();
  let sequence = 0;
  const persist = async (event: Event) => {
    try {
      await deps.beforePersist?.(Object.freeze({ ...event }));
      await file!.appendFile(`${JSON.stringify({ sequence: ++sequence, ...event })}\n`);
      await file!.sync();
    } catch {
      durable = false;
      throw new Error('Journal');
    }
  };
  const keys = new Set([`${config.prefix}sample`, `${config.prefix}incomplete`]);
  const scope = (input: { Bucket?: string; Key?: string; UploadId?: string; VersionId?: string }) => {
    if (
      input.Bucket !== config.bucket ||
      (input.Key !== undefined && !keys.has(input.Key)) ||
      (input.UploadId !== undefined &&
        remaining.every((h) => !(h.key === input.Key && h.uploadId === input.UploadId))) ||
      (input.VersionId !== undefined &&
        remaining.every((h) => !(h.key === input.Key && h.versionId === input.VersionId)))
    )
      throw new Error('Scope');
  };
  const remote = async <T>(
    name: string,
    command: ProbeCommand,
    transfer = 0,
    cleanup = false,
    absent?: string[],
    receive?: (result: T) => void,
  ) => {
    phase = name;
    if (!durable) throw new Error('Journal');
    const input = command.input as { Bucket?: string; Key?: string; UploadId?: string; VersionId?: string };
    scope(input);
    const left = maxRuntimeMs - (performance.now() - started);
    if (
      requests + 1 > maxRequests - (cleanup ? 0 : 4) ||
      bytes + transfer > maxBytes - (cleanup ? 0 : 1024 ** 2) ||
      left <= (cleanup ? 0 : 5000)
    )
      throw new Error('Budget');
    if (!cleanup) deps.signal?.throwIfAborted();
    await persist({
      kind: 'intent',
      phase,
      bucket: input.Bucket,
      key: input.Key,
      uploadId: input.UploadId,
      versionId: input.VersionId,
      partNumber: 'PartNumber' in command.input ? command.input.PartNumber : undefined,
      range: 'Range' in command.input ? command.input.Range : undefined,
      transfer,
    });
    const available = maxRuntimeMs - (performance.now() - started) - (cleanup ? 0 : 5000);
    if (available <= 0) throw new Error('Budget');
    const timeout = AbortSignal.timeout(Math.max(1, Math.floor(Math.min(available, 30_000))));
    const signal = !cleanup && deps.signal ? AbortSignal.any([deps.signal, timeout]) : timeout;
    signal.throwIfAborted();
    requests++;
    bytes += transfer;
    try {
      const result = (await (
        deps.client.send as unknown as (
          command: ProbeCommand,
          options: { abortSignal: AbortSignal },
        ) => Promise<unknown>
      )(command, { abortSignal: signal })) as T;
      receive?.(result);
      const identifiers = result as { UploadId?: unknown; VersionId?: unknown; ETag?: unknown };
      const owned = remaining.find((handle) => handle.key === input.Key);
      if (command instanceof CreateMultipartUploadCommand && owned && opaque(identifiers?.UploadId)) {
        owned.uploadId = identifiers.UploadId;
      }
      if (
        command instanceof CompleteMultipartUploadCommand &&
        owned?.uploadId === input.UploadId &&
        owned &&
        opaque(identifiers?.VersionId)
      ) {
        owned.versionId = identifiers.VersionId;
      }
      await persist({
        kind: 'ack',
        phase,
        key: input.Key,
        uploadId: opaque(identifiers?.UploadId) ? identifiers.UploadId : undefined,
        versionId: opaque(identifiers?.VersionId) ? identifiers.VersionId : undefined,
        etag: opaque(identifiers?.ETag) ? identifiers.ETag : undefined,
      });
      return result;
    } catch (error) {
      if (absent && missing(error, absent)) {
        await persist({ kind: 'absence', phase });
        return;
      }
      if (durable) await persist({ kind: 'unknown', phase });
      // Deliberately omit cause: SDK errors may embed credentials or signed headers.
      // eslint-disable-next-line preserve-caught-error
      throw new Error('Remote');
    }
  };
  const read = async (handle: Handle, expected: Buffer, range?: string, cleanup = false) => {
    let body: Readable | undefined;
    let closed: Promise<void> | undefined;
    let sourceFailure: Error | undefined;
    const observe = (error: Error) => {
      sourceFailure ??= error;
    };
    try {
      const output = await remote<GetObjectCommandOutput>(
        'get',
        new GetObjectCommand({ Bucket: config.bucket, Key: handle.key, VersionId: handle.versionId, Range: range }),
        expected.length,
        cleanup,
        cleanup ? ['NoSuchKey', 'NoSuchVersion'] : undefined,
        (result) => {
          if (!(result?.Body instanceof Readable)) return;
          body = result.Body;
          body.on('error', observe);
          closed = body.closed ? Promise.resolve() : new Promise<void>((resolve) => body!.once('close', resolve));
        },
      );
      if (sourceFailure) throw sourceFailure;
      if (cleanup) {
        if (output) throw new Error('StillPresent');
        return;
      }
      if (!output || !body) throw new Error('Response');
      const stream = body;
      const timer = setTimeout(
        () => stream.destroy(new Error('Deadline')),
        Math.max(1, Math.min(30_000, maxRuntimeMs - (performance.now() - started) - 5000)),
      );
      const cancel = () => stream.destroy(new Error('Cancelled'));
      deps.signal?.addEventListener('abort', cancel, { once: true });
      let releaseReader: (() => void) | undefined;
      try {
        deps.signal?.throwIfAborted();
        if (maxRuntimeMs - (performance.now() - started) <= 5000) throw new Error('Budget');
        const match = range?.match(/^bytes=(\d+)-(\d+)$/);
        if (
          output.ServerSideEncryption !== 'AES256' ||
          output.VersionId !== handle.versionId ||
          output.ContentLength !== expected.length ||
          (range &&
            (output.$metadata.httpStatusCode !== 206 ||
              output.ContentRange !== `bytes ${match![1]}-${match![2]}/${plan.sampleSize}`))
        )
          throw new Error('Response');
        const digest = createHash('sha256');
        let received = 0;
        await new Promise<void>((resolve, reject) => {
          const data = (chunk: Buffer) => {
            received += chunk.length;
            if (received > expected.length) reject(new Error('Response'));
            else digest.update(chunk);
          };
          const end = () => resolve();
          const error = (error: Error) => reject(error);
          const close = () => {
            if (!stream.readableEnded) reject(new Error('Response'));
          };
          stream.on('data', data);
          stream.once('end', end);
          stream.once('error', error);
          stream.once('close', close);
          releaseReader = () => {
            stream.removeListener('data', data);
            stream.removeListener('end', end);
            stream.removeListener('error', error);
            stream.removeListener('close', close);
          };
          if (stream.errored) reject(stream.errored);
          else if (stream.closed) close();
        });
        if (sourceFailure) throw sourceFailure;
        if (received !== expected.length || digest.digest('hex') !== hash(expected)) throw new Error('Integrity');
        await persist({ kind: 'verified', phase: range ? 'range' : 'full-hash' });
      } finally {
        releaseReader?.();
        clearTimeout(timer);
        deps.signal?.removeEventListener('abort', cancel);
      }
    } finally {
      if (body) {
        body.destroy();
        await closed;
        body.removeListener('error', observe);
      }
    }
    if (sourceFailure) throw sourceFailure;
  };
  const cleanupAttempted = new Set<Handle>();
  const cleanupHandle = async (handle: Handle) => {
    if (handle.unknown || cleanupAttempted.has(handle)) return;
    cleanupAttempted.add(handle);
    if (handle.versionId) {
      await remote(
        'delete-version',
        new DeleteObjectCommand({ Bucket: config.bucket, Key: handle.key, VersionId: handle.versionId }),
        0,
        true,
      );
      await read(handle, Buffer.alloc(1), 'bytes=0-0', true);
    } else if (handle.uploadId) {
      await remote(
        'abort',
        new AbortMultipartUploadCommand({ Bucket: config.bucket, Key: handle.key, UploadId: handle.uploadId }),
        0,
        true,
      );
      const output = await remote(
        'verify-abort',
        new ListPartsCommand({ Bucket: config.bucket, Key: handle.key, UploadId: handle.uploadId, MaxParts: 1 }),
        0,
        true,
        ['NoSuchUpload'],
      );
      if (output !== undefined) throw new Error('StillPresent');
    } else return;
    await persist({ kind: 'cleaned', ...handle });
    remaining.splice(remaining.indexOf(handle), 1);
  };
  try {
    directory = await open(config.workspace, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
    parentDirectory = await open(parent, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
    const stats = await directory.stat();
    if ((stats.mode & 0o777) !== 0o700 || stats.uid !== process.getuid!()) throw new Error('UnsafeWorkspace');
    file = await open(
      path.join(config.workspace, 'manifest.jsonl'),
      constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW,
      0o600,
    );
    await persist({
      kind: 'run',
      bucket: config.bucket,
      endpoint: config.endpoint,
      region: config.region,
      prefix: config.prefix,
      keys: [...keys],
      maxRequests,
      maxBytes,
      maxRuntimeMs,
    });
    await directory.sync();
    await parentDirectory.sync();
    await remote('head-bucket', new HeadBucketCommand({ Bucket: config.bucket }));
    for (const key of keys) {
      const existing = await remote(
        'key-unused',
        new HeadObjectCommand({ Bucket: config.bucket, Key: key }),
        0,
        false,
        ['NotFound', 'NoSuchKey'],
      );
      if (existing !== undefined) throw new Error('ExistingKey');
    }
    const sample = Buffer.alloc(plan.sampleSize);
    for (let i = 0; i < sample.length; i++) sample[i] = (i * 31 + Math.floor(i / 65_536)) % 251;
    await persist({ kind: 'sample', size: sample.length, sha256: hash(sample), partSize: plan.partSize });
    for (const key of keys) {
      const handle: Handle = { key, unknown: 'create-outcome' };
      remaining.push(handle);
      await persist({ kind: 'held', ...handle });
      const created = await remote<{ UploadId?: string }>(
        'create',
        new CreateMultipartUploadCommand({ Bucket: config.bucket, Key: key, ServerSideEncryption: 'AES256' }),
      );
      if (!opaque(created?.UploadId)) throw new Error('Response');
      handle.uploadId = created.UploadId;
      delete handle.unknown;
      await persist({ kind: 'owned-upload', ...handle });
      const parts: { PartNumber: number; ETag: string }[] = [];
      const incomplete = key.endsWith('incomplete');
      for (let start = 0; start < (incomplete ? plan.partSize : sample.length); start += plan.partSize) {
        const data = sample.subarray(start, Math.min(start + plan.partSize, sample.length));
        const part = await remote<{ ETag?: string }>(
          'upload-part',
          new UploadPartCommand({
            Bucket: config.bucket,
            Key: key,
            UploadId: handle.uploadId,
            PartNumber: parts.length + 1,
            Body: data,
            ContentLength: data.length,
          }),
          data.length,
        );
        if (!opaque(part?.ETag)) throw new Error('Response');
        parts.push({ PartNumber: parts.length + 1, ETag: part.ETag });
      }
      if (!incomplete) {
        handle.unknown = 'complete-outcome';
        await persist({ kind: 'held', ...handle });
        const completed = await remote<{ VersionId?: string }>(
          'complete',
          new CompleteMultipartUploadCommand({
            Bucket: config.bucket,
            Key: key,
            UploadId: handle.uploadId,
            MultipartUpload: { Parts: parts },
          }),
        );
        if (!opaque(completed?.VersionId)) throw new Error('Response');
        handle.versionId = completed.VersionId;
        delete handle.unknown;
        await persist({ kind: 'owned-version', ...handle });
        await read(handle, sample);
        for (const boundary of [plan.partSize, plan.partSize * 2])
          await read(handle, sample.subarray(boundary - 1, boundary + 1), `bytes=${boundary - 1}-${boundary}`);
      }
      await cleanupHandle(handle);
    }
    await persist({ kind: 'finished', requests, bytes });
    return { status: 'synthetic-qualified', remaining, requests, bytes };
  } catch {
    const failedPhase = phase;
    if (durable && file) {
      for (const handle of remaining) {
        try {
          await cleanupHandle(handle);
        } catch {
          /* Exact handles remain for operator reconciliation; never replay. */
        }
      }
      try {
        await persist({ kind: 'failed', phase: failedPhase, remaining });
      } catch {
        /* Existing durable intents remain authoritative. */
      }
    }
    return {
      status: 'failed',
      phase: failedPhase,
      errorClass: durable ? 'QualificationFailure' : 'JournalFailure',
      remaining,
      requests,
      bytes,
    };
  } finally {
    await file?.close();
    await directory?.close();
    await parentDirectory?.close();
  }
};
