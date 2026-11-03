import { Kysely, sql } from 'kysely';
import { randomUUID } from 'node:crypto';
import { ObjectWriteResult } from 'src/repositories/object-store.repository.js';
import { S3MultipartJournalEvent } from 'src/repositories/s3-object-store.repository.js';
import { DB } from 'src/schema/index.js';
import { VerificationObservation } from 'src/schema/tables/object-version.table.js';
import { StageIdentity } from 'src/schema/tables/staged-object.table.js';

const classify = (value: unknown) => (value instanceof Error ? 'error' : 'non-error');

export interface MultipartBinding {
  intentId: string;
  storeId: string;
  bucket: string;
  key: string;
  token: string;
}

export class ObjectCatalogRepository {
  private readonly objectKeyPrefix: string;

  constructor(
    private db: Kysely<DB>,
    options: { readonly objectKeyPrefix?: string } = {},
  ) {
    const prefix = options.objectKeyPrefix === undefined ? 'catalog' : options.objectKeyPrefix;
    if (
      typeof prefix !== 'string' ||
      Buffer.byteLength(prefix) > 950 ||
      !/^[A-Za-z0-9_-][A-Za-z0-9._-]*(\/[A-Za-z0-9_-][A-Za-z0-9._-]*)*$/.test(prefix)
    ) {
      throw new Error('Invalid catalog object key prefix');
    }
    this.objectKeyPrefix = prefix;
  }

  withDatabase(db: Kysely<DB>) {
    return new ObjectCatalogRepository(db, { objectKeyPrefix: this.objectKeyPrefix });
  }

  listMultipart(stageId: string) {
    return this.db
      .selectFrom('s3_multipart_journal')
      .innerJoin('object_upload_intent', 'object_upload_intent.id', 's3_multipart_journal.intentId')
      .selectAll('s3_multipart_journal')
      .where('object_upload_intent.stageId', '=', stageId)
      .execute();
  }

  async recordMultipart(binding: MultipartBinding, event: S3MultipartJournalEvent) {
    if (event.bucket !== binding.bucket || event.key !== binding.key || event.attemptToken !== binding.token) {
      throw new Error('Multipart binding mismatch');
    }
    if (event.phase === 'failure' || event.phase === 'abortResult') {
      await this.db.transaction().execute(async (db) => {
        const intent = await db
          .selectFrom('object_upload_intent')
          .selectAll()
          .where('id', '=', binding.intentId)
          .where('provider', '=', 's3')
          .where('storeId', '=', binding.storeId)
          .where('key', '=', binding.key)
          .where('token', '=', binding.token)
          .where('state', 'in', ['sending', 'ambiguous'])
          .forUpdate()
          .executeTakeFirstOrThrow();
        const journal = await db
          .selectFrom('s3_multipart_journal')
          .selectAll()
          .where('intentId', '=', intent.id)
          .executeTakeFirstOrThrow();
        if (
          journal.bucket !== binding.bucket ||
          journal.token !== binding.token ||
          (event.uploadId !== undefined &&
            (event.uploadId.length === 0 ||
              Buffer.byteLength(event.uploadId) > 4096 ||
              (journal.uploadId !== null && journal.uploadId !== event.uploadId)))
        ) {
          throw new Error('Multipart evidence binding mismatch');
        }
        if (event.phase === 'abortResult' && !['creating', 'uploading'].includes(journal.state)) {
          throw new Error('Unexpected multipart abort evidence');
        }
        if (event.phase === 'failure') {
          const states = {
            beforeCreate: ['creating'],
            create: ['creating'],
            uploadCreated: ['creating', 'uploading'],
            parts: ['uploading'],
            beforeComplete: ['uploading', 'completing'],
            complete: ['completing'],
            receivedResult: ['completing', 'received'],
          };
          if (
            !states[event.at]?.includes(journal.state) ||
            event.completeDispatched !== ['complete', 'receivedResult'].includes(event.at) ||
            (journal.uploadId !== null && event.uploadId !== journal.uploadId) ||
            (event.receivedResult !== undefined && event.at !== 'receivedResult')
          ) {
            throw new Error('Unexpected multipart failure evidence state');
          }
        }
        const response =
          event.phase === 'failure' && event.receivedResult
            ? {
                ...(typeof event.receivedResult.versionId === 'string' &&
                  Buffer.byteLength(event.receivedResult.versionId) <= 4096 && {
                    versionId: event.receivedResult.versionId,
                  }),
                ...(typeof event.receivedResult.etag === 'string' &&
                  Buffer.byteLength(event.receivedResult.etag) <= 4096 && { etag: event.receivedResult.etag }),
              }
            : undefined;
        const observation =
          event.phase === 'abortResult'
            ? {
                phase: event.phase,
                uploadId: event.uploadId,
                ok: event.outcome.ok,
                ...(!event.outcome.ok && { reason: classify(event.outcome.reason) }),
              }
            : {
                phase: event.phase,
                at: event.at,
                uploadId: event.uploadId,
                completeDispatched: event.completeDispatched,
                reason: classify(event.reason),
                secondary: event.secondary.slice(0, 8).map((value) => classify(value)),
              };
        const evidence = [...journal.evidence.slice(0, 31), observation];
        while (Buffer.byteLength(JSON.stringify(evidence)) > 30_000 && evidence.length > 2) {
          evidence.splice(-2, 1);
        }
        await db
          .updateTable('s3_multipart_journal')
          .set({ evidence: sql`${evidence}::jsonb`, ...(response && journal.response === null && { response }) })
          .where('intentId', '=', intent.id)
          .execute();
      });
      return;
    }
    await this.db.transaction().execute(async (db) => {
      const intent = await db
        .selectFrom('object_upload_intent')
        .selectAll()
        .where('id', '=', binding.intentId)
        .where('provider', '=', 's3')
        .where('storeId', '=', binding.storeId)
        .where('key', '=', binding.key)
        .where('token', '=', binding.token)
        .where('state', '=', 'sending')
        .where('leaseUntil', '>', sql<Date>`clock_timestamp()`)
        .forUpdate()
        .executeTakeFirstOrThrow();
      const advance = async () => {
        if (event.phase !== 'beforeCreate') {
          const journal = await db
            .selectFrom('s3_multipart_journal')
            .selectAll()
            .where('intentId', '=', intent.id)
            .executeTakeFirstOrThrow();
          if (
            journal.bucket !== binding.bucket ||
            journal.token !== binding.token ||
            journal.storeId !== binding.storeId ||
            journal.key !== binding.key
          ) {
            throw new Error('Multipart journal binding mismatch');
          }
          const validString = (value: unknown): value is string =>
            typeof value === 'string' && value.length > 0 && Buffer.byteLength(value) <= 4096;
          if (
            !('uploadId' in event) ||
            !validString(event.uploadId) ||
            (journal.uploadId !== null && journal.uploadId !== event.uploadId)
          ) {
            throw new Error('Multipart UploadId mismatch');
          }
          if (event.phase === 'uploadCreated' && journal.state === 'creating') {
            await db
              .updateTable('s3_multipart_journal')
              .set({ state: 'uploading', uploadId: event.uploadId })
              .where('intentId', '=', intent.id)
              .execute();
            return;
          }
          if (
            event.phase === 'beforeComplete' &&
            journal.state === 'uploading' &&
            event.parts.length === journal.partCount &&
            event.parts.every((part, index) => part.PartNumber === index + 1 && validString(part.ETag))
          ) {
            await db
              .updateTable('s3_multipart_journal')
              .set({ state: 'completing', manifest: event.parts.map((part) => ({ ...part })) })
              .where('intentId', '=', intent.id)
              .execute();
            return;
          }
          if (
            event.phase === 'receivedResult' &&
            journal.state === 'completing' &&
            (event.versionId === undefined || validString(event.versionId)) &&
            (event.etag === undefined || validString(event.etag))
          ) {
            await db
              .updateTable('s3_multipart_journal')
              .set({ state: 'received', response: { versionId: event.versionId, etag: event.etag } })
              .where('intentId', '=', intent.id)
              .execute();
            return;
          }
          throw new Error('Unexpected multipart event or state');
        }
        const stage = await db
          .selectFrom('staged_object')
          .selectAll()
          .where('id', '=', intent.stageId)
          .executeTakeFirstOrThrow();
        if (
          event.size !== Number(stage.size) ||
          event.expectedHash.algorithm !== 'sha256' ||
          event.expectedHash.hex !== stage.hash ||
          event.partCount !== Math.ceil(event.size / event.partSize)
        ) {
          throw new Error('Multipart source mismatch');
        }
        await db
          .insertInto('s3_multipart_journal')
          .values({
            intentId: intent.id,
            provider: 's3',
            storeId: binding.storeId,
            bucket: binding.bucket,
            key: binding.key,
            token: binding.token,
            state: 'creating',
            size: String(event.size),
            hash: stage.hash!,
            partSize: event.partSize,
            partCount: event.partCount,
          })
          .execute();
      };
      await advance();
      await db
        .selectFrom('object_upload_intent')
        .select('id')
        .where('id', '=', intent.id)
        .where('state', '=', 'sending')
        .where('token', '=', binding.token)
        .where('leaseUntil', '>', sql<Date>`clock_timestamp()`)
        .executeTakeFirstOrThrow();
    });
  }

  async withCleanupLock<T>(id: string, action: (catalog: ObjectCatalogRepository) => Promise<T>) {
    return this.db.connection().execute(async (db) => {
      const key = sql`hashtextextended(${`object-catalog-cleanup:${id}`}::text, 0)`;
      try {
        await sql`SELECT pg_advisory_lock(${key})`.execute(db);
        return await action(new ObjectCatalogRepository(db, { objectKeyPrefix: this.objectKeyPrefix }));
      } finally {
        await sql`SELECT pg_advisory_unlock(${key})`.execute(db);
      }
    });
  }

  async reserveOwnedStage(sourceId: string, ownerId: string, root: string, publicationId?: string) {
    return this.db.transaction().execute(async (db) => {
      const publication = publicationId
        ? await db
            .selectFrom('upload_publication')
            .selectAll()
            .where('id', '=', publicationId)
            .where('sourceId', '=', sourceId)
            .where('ownerId', '=', ownerId)
            .forUpdate()
            .executeTakeFirstOrThrow()
        : undefined;
      const associate = async (stageId: string) => {
        if (!publication) {
          return;
        }

        if (publication.stageId && publication.stageId !== stageId) {
          throw new Error('Publication stage mismatch');
        }
        await db.updateTable('upload_publication').set({ stageId }).where('id', '=', publication.id).execute();
      };
      const source = await db
        .selectFrom('source_receipt')
        .selectAll()
        .where('id', '=', sourceId)
        .where('ownerId', '=', ownerId)
        .forUpdate()
        .executeTakeFirstOrThrow();
      if (
        source.state !== 'sealed' ||
        source.role !== 'original' ||
        !source.identity ||
        !source.sha256 ||
        source.size === null
      ) {
        throw new Error('Source is not a sealed owned original');
      }
      const existing = await db
        .selectFrom('owned_stage_receipt')
        .selectAll()
        .where('sourceId', '=', source.id)
        .where('ownerId', '=', ownerId)
        .executeTakeFirst();
      if (existing) {
        const stage = await db
          .selectFrom('staged_object')
          .selectAll()
          .where('id', '=', existing.stageId)
          .forUpdate()
          .executeTakeFirstOrThrow();
        if (stage.root !== root) {
          throw new Error('Reserved stage root mismatch');
        }
        await associate(stage.id);
        return { source, stage, created: false };
      }
      const stage = await db
        .insertInto('staged_object')
        .values({ id: randomUUID(), root, state: 'creating' })
        .returningAll()
        .executeTakeFirstOrThrow();
      await db
        .insertInto('owned_stage_receipt')
        .values({
          stageId: stage.id,
          sourceId: source.id,
          ownerId,
          sourceState: 'sealed',
          hash: source.sha256,
          size: source.size,
          sourceIdentity: source.identity,
        })
        .execute();
      await associate(stage.id);
      return { source, stage, created: true };
    });
  }

  createStage(id: string, root: string) {
    return this.db
      .insertInto('staged_object')
      .values({ id, root, state: 'creating' })
      .returningAll()
      .executeTakeFirstOrThrow();
  }

  getStage(id: string) {
    return this.db.selectFrom('staged_object').selectAll().where('id', '=', id).executeTakeFirst();
  }

  async pin(id: string) {
    return this.db.transaction().execute(async (db) => {
      const stage = await db
        .selectFrom('staged_object')
        .selectAll()
        .where('id', '=', id)
        .forUpdate()
        .executeTakeFirstOrThrow();
      if (stage.state !== 'ready') {
        throw new Error('Stage cannot be pinned');
      }
      const token = randomUUID();
      await db
        .updateTable('staged_object')
        .set({ pins: sql`${[...stage.pins, token]}::jsonb` })
        .where('id', '=', id)
        .execute();
      return token;
    });
  }

  async unpin(id: string, token: string) {
    await this.db.transaction().execute(async (db) => {
      const stage = await db
        .selectFrom('staged_object')
        .selectAll()
        .where('id', '=', id)
        .forUpdate()
        .executeTakeFirstOrThrow();
      await db
        .updateTable('staged_object')
        .set({ pins: sql`${stage.pins.filter((pin) => pin !== token)}::jsonb` })
        .where('id', '=', id)
        .execute();
    });
  }

  releaseStage(id: string) {
    return this.db
      .updateTable('staged_object')
      .set({ released: true })
      .where('id', '=', id)
      .where('state', '=', 'ready')
      .returningAll()
      .executeTakeFirstOrThrow();
  }

  async claimCleanup(id: string) {
    return this.db.transaction().execute(async (db) => {
      const stage = await db
        .selectFrom('staged_object')
        .selectAll()
        .where('id', '=', id)
        .forUpdate()
        .executeTakeFirstOrThrow();
      const owned = await db
        .selectFrom('owned_stage_receipt')
        .select('stageId')
        .where('stageId', '=', id)
        .executeTakeFirst();
      if (owned || stage.state !== 'ready' || !stage.released || stage.pins.length > 0) {
        return;
      }
      const intents = await db.selectFrom('object_upload_intent').selectAll().where('stageId', '=', id).execute();
      if (intents.length === 0 || intents.some((intent) => intent.state !== 'acknowledged')) {
        return;
      }
      const versions = await db
        .selectFrom('object_version')
        .selectAll()
        .where(
          'intentId',
          'in',
          intents.map((intent) => intent.id),
        )
        .execute();
      if (versions.length !== intents.length || versions.some((version) => version.verification !== 'verified')) {
        return;
      }
      return db
        .updateTable('staged_object')
        .set({ state: 'deleting' })
        .where('id', '=', id)
        .where('state', '=', 'ready')
        .returningAll()
        .executeTakeFirstOrThrow();
    });
  }

  finishCleanup(id: string) {
    return this.db
      .updateTable('staged_object')
      .set({ state: 'deleted' })
      .where('id', '=', id)
      .where('state', 'in', ['deleting', 'deleted'])
      .returningAll()
      .executeTakeFirstOrThrow();
  }

  observeVersion(id: string, observation: VerificationObservation) {
    return this.db
      .updateTable('object_version')
      .set({
        verification: observation.outcome,
        observations: sql`observations || jsonb_build_array(${observation}::jsonb || jsonb_build_object('observedAt', clock_timestamp()))`,
      })
      .where('id', '=', id)
      .returningAll()
      .executeTakeFirstOrThrow();
  }

  listVersions(stageId: string) {
    return this.db
      .selectFrom('object_version')
      .innerJoin('object_upload_intent', 'object_upload_intent.id', 'object_version.intentId')
      .selectAll('object_version')
      .where('object_upload_intent.stageId', '=', stageId)
      .execute();
  }

  holdAttempt(id: string, token: string, evidence: string) {
    return this.db
      .updateTable('object_upload_intent')
      .set({ state: 'ambiguous', evidence: sql`concat_ws(E'\\n', evidence, ${evidence}::text)` })
      .where('id', '=', id)
      .where('token', '=', token)
      .where('state', 'in', ['sending', 'ambiguous'])
      .execute();
  }

  expireSending(stageId: string) {
    return this.db
      .updateTable('object_upload_intent')
      .set({
        state: 'ambiguous',
        evidence: sql`concat_ws(E'\\n', evidence, 'Sending lease expired: outcome unknown; hold, never retry automatically')`,
      })
      .where('stageId', '=', stageId)
      .where('state', '=', 'sending')
      .where('leaseUntil', '<=', sql<Date>`clock_timestamp()`)
      .execute();
  }

  getIntent(id: string) {
    return this.db.selectFrom('object_upload_intent').selectAll().where('id', '=', id).executeTakeFirstOrThrow();
  }

  listIntents(stageId: string) {
    return this.db.selectFrom('object_upload_intent').selectAll().where('stageId', '=', stageId).execute();
  }

  async prepareOwned(publicationId: string, ownerId: string, storeId: string) {
    return this.db.transaction().execute(async (db) => {
      const publication = await db
        .selectFrom('upload_publication')
        .selectAll()
        .where('id', '=', publicationId)
        .where('ownerId', '=', ownerId)
        .where('storeId', '=', storeId)
        .forUpdate()
        .executeTakeFirstOrThrow();
      if (publication.result || !publication.stageId) {
        throw new Error('Publication unavailable');
      }
      if (publication.intentId) {
        return db
          .selectFrom('object_upload_intent')
          .selectAll()
          .where('id', '=', publication.intentId)
          .where('stageId', '=', publication.stageId)
          .where('provider', '=', 's3')
          .where('storeId', '=', storeId)
          .executeTakeFirstOrThrow();
      }
      const stage = await db
        .selectFrom('staged_object')
        .selectAll()
        .where('id', '=', publication.stageId)
        .forUpdate()
        .executeTakeFirstOrThrow();
      const owned = await db
        .selectFrom('owned_stage_receipt')
        .selectAll()
        .where('stageId', '=', stage.id)
        .where('sourceId', '=', publication.sourceId)
        .where('ownerId', '=', ownerId)
        .executeTakeFirstOrThrow();
      if (stage.state !== 'ready' || stage.released || stage.hash !== owned.hash || stage.size !== owned.size) {
        throw new Error('Owned stage unavailable');
      }
      const existing = await db
        .selectFrom('object_upload_intent')
        .select('id')
        .where('stageId', '=', stage.id)
        .executeTakeFirst();
      if (existing) {
        throw new Error('Owned stage already has an intent');
      }
      const id = randomUUID();
      const intent = await db
        .insertInto('object_upload_intent')
        .values({
          id,
          stageId: stage.id,
          provider: 's3',
          storeId,
          key: `${this.objectKeyPrefix}/${stage.id}/${id}`,
          state: 'prepared',
        })
        .returningAll()
        .executeTakeFirstOrThrow();
      await db
        .updateTable('upload_publication')
        .set({ intentId: intent.id, state: 'prepared' })
        .where('id', '=', publication.id)
        .execute();
      return intent;
    });
  }

  async authorizeOwnedSend(intentId: string, publicationId?: string) {
    const intent = await this.getIntent(intentId);
    const owned = await this.db
      .selectFrom('owned_stage_receipt')
      .select('stageId')
      .where('stageId', '=', intent.stageId)
      .executeTakeFirst();
    if (owned) {
      if (!publicationId) {
        throw new Error('Owned intent requires publication permit');
      }
      await this.db
        .selectFrom('upload_publication')
        .select('id')
        .where('id', '=', publicationId)
        .where('stageId', '=', intent.stageId)
        .where('intentId', '=', intentId)
        .where('storeId', '=', intent.storeId)
        .where('result', 'is', null)
        .executeTakeFirstOrThrow();
    } else if (publicationId) {
      throw new Error('Unexpected publication permit');
    }
  }

  async prepare(stageId: string, provider: 'local' | 's3', storeId: string) {
    return this.db.transaction().execute(async (db) => {
      const stage = await db
        .selectFrom('staged_object')
        .selectAll()
        .where('id', '=', stageId)
        .forUpdate()
        .executeTakeFirstOrThrow();
      const owned = await db
        .selectFrom('owned_stage_receipt')
        .select('stageId')
        .where('stageId', '=', stage.id)
        .executeTakeFirst();
      if (owned) {
        throw new Error('Receipt-owned stage requires an owned intent');
      }
      if (stage.state !== 'ready' || stage.released) {
        throw new Error('Stage is not available');
      }
      const pending = await db
        .selectFrom('object_upload_intent')
        .select('id')
        .where('stageId', '=', stageId)
        .where('state', '!=', 'acknowledged')
        .executeTakeFirst();
      if (pending) {
        throw new Error('Unresolved intent pins stage');
      }
      const id = randomUUID();
      return db
        .insertInto('object_upload_intent')
        .values({
          id,
          stageId: stage.id,
          provider,
          storeId,
          key: `${this.objectKeyPrefix}/${stage.id}/${id}`,
          state: 'prepared',
        })
        .returningAll()
        .executeTakeFirstOrThrow();
    });
  }

  async renewSending(id: string, token: string, leaseMs: number) {
    if (!Number.isSafeInteger(leaseMs) || leaseMs < 1 || leaseMs > 3_600_000) {
      throw new Error('Invalid lease duration');
    }
    return this.db.transaction().execute(async (db) => {
      await sql`SELECT set_config('statement_timeout', ${String(Math.min(leaseMs, 5000))}, true)`.execute(db);
      await db.selectFrom('object_upload_intent').select('id').where('id', '=', id).forUpdate().executeTakeFirst();
      const renewed = await db
        .updateTable('object_upload_intent')
        .set({ leaseUntil: sql`clock_timestamp() + ${leaseMs} * interval '1 millisecond'` })
        .where('id', '=', id)
        .where('token', '=', token)
        .where('state', '=', 'sending')
        .where('leaseUntil', '>', sql<Date>`clock_timestamp()`)
        .returning('id')
        .executeTakeFirst();
      return !!renewed;
    });
  }

  async startSending(id: string, leaseMs: number, publicationId?: string) {
    if (!Number.isSafeInteger(leaseMs) || leaseMs < 1 || leaseMs > 3_600_000) {
      throw new Error('Invalid lease duration');
    }
    await this.authorizeOwnedSend(id, publicationId);
    if (publicationId) {
      return this.db.transaction().execute(async (db) => {
        const publication = await db
          .selectFrom('upload_publication')
          .selectAll()
          .where('id', '=', publicationId)
          .where('intentId', '=', id)
          .where('result', 'is', null)
          .forUpdate()
          .executeTakeFirstOrThrow();
        const claim = await new ObjectCatalogRepository(db).startSendingRaw(id, leaseMs);
        if (claim) {
          await db
            .updateTable('upload_publication')
            .set({ exactAttempt: claim.token })
            .where('id', '=', publication.id)
            .execute();
        }
        return claim;
      });
    }
    return this.startSendingRaw(id, leaseMs);
  }

  private startSendingRaw(id: string, leaseMs: number) {
    return this.db
      .updateTable('object_upload_intent')
      .set({
        state: 'sending',
        token: randomUUID(),
        leaseUntil: sql`clock_timestamp() + ${leaseMs} * interval '1 millisecond'`,
      })
      .where('id', '=', id)
      .where('state', '=', 'prepared')
      .returningAll()
      .executeTakeFirst();
  }

  async acknowledge(id: string, token: string, result: ObjectWriteResult) {
    return this.db.transaction().execute(async (db) => {
      await db
        .selectFrom('object_upload_intent')
        .select('id')
        .where('id', '=', id)
        .forUpdate()
        .executeTakeFirstOrThrow();
      const intent = await db
        .updateTable('object_upload_intent')
        .set({ state: 'acknowledged' })
        .where('id', '=', id)
        .where('token', '=', token)
        .where('state', '=', 'sending')
        .where('leaseUntil', '>', sql<Date>`clock_timestamp()`)
        .returningAll()
        .executeTakeFirstOrThrow();
      const journal = await db
        .selectFrom('s3_multipart_journal')
        .selectAll()
        .where('intentId', '=', id)
        .executeTakeFirst();
      if (
        journal &&
        (journal.state !== 'received' ||
          journal.token !== token ||
          journal.response?.versionId !== result.object.versionId ||
          journal.response?.etag !== result.etag)
      ) {
        throw new Error('Multipart response does not match acknowledgement');
      }
      if (
        result.object.key !== intent.key ||
        (intent.provider === 's3'
          ? !result.object.versionId || result.object.versionId === 'null'
          : result.object.versionId !== undefined)
      ) {
        throw new Error('Provider returned an invalid exact identity');
      }
      const version = await db
        .insertInto('object_version')
        .values({
          id: randomUUID(),
          intentId: id,
          provider: intent.provider,
          storeId: intent.storeId,
          key: intent.key,
          versionId: result.object.versionId ?? null,
          etag: result.etag ?? null,
        })
        .returningAll()
        .executeTakeFirstOrThrow();
      await db
        .selectFrom('object_upload_intent')
        .select('id')
        .where('id', '=', id)
        .where('token', '=', token)
        .where('state', '=', 'acknowledged')
        .where('leaseUntil', '>', sql<Date>`clock_timestamp()`)
        .executeTakeFirstOrThrow();
      return version;
    });
  }

  listStages(root: string) {
    return this.db.selectFrom('staged_object').selectAll().where('root', '=', root).execute();
  }

  quarantine(id: string, evidence: string) {
    return this.db
      .updateTable('staged_object')
      .set({ state: 'quarantined', evidence })
      .where('id', '=', id)
      .where('state', '!=', 'deleted')
      .execute();
  }

  sealStage(id: string, identity: StageIdentity, hash: string) {
    return this.db
      .updateTable('staged_object')
      .set({ state: 'ready', identity, hash, size: identity.size })
      .where('id', '=', id)
      .where('state', '=', 'creating')
      .returningAll()
      .executeTakeFirstOrThrow();
  }
}
