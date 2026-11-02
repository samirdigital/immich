import { Kysely, sql } from 'kysely';
import { randomUUID } from 'node:crypto';
import { ObjectWriteResult } from 'src/repositories/object-store.repository.js';
import { DB } from 'src/schema/index.js';
import { VerificationObservation } from 'src/schema/tables/object-version.table.js';
import { StageIdentity } from 'src/schema/tables/staged-object.table.js';

export class ObjectCatalogRepository {
  constructor(private db: Kysely<DB>) {}

  async withCleanupLock<T>(id: string, action: (catalog: ObjectCatalogRepository) => Promise<T>) {
    return this.db.connection().execute(async (db) => {
      const key = sql`hashtextextended(${`object-catalog-cleanup:${id}`}::text, 0)`;
      try {
        await sql`SELECT pg_advisory_lock(${key})`.execute(db);
        return await action(new ObjectCatalogRepository(db));
      } finally {
        await sql`SELECT pg_advisory_unlock(${key})`.execute(db);
      }
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
      if (stage.state !== 'ready' || !stage.released || stage.pins.length > 0) {
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
      .set({ state: 'ambiguous', evidence: 'Sending lease expired: outcome unknown; hold, never retry automatically' })
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

  async prepare(stageId: string, provider: 'local' | 's3', storeId: string) {
    return this.db.transaction().execute(async (db) => {
      const stage = await db
        .selectFrom('staged_object')
        .selectAll()
        .where('id', '=', stageId)
        .forUpdate()
        .executeTakeFirstOrThrow();
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
        .values({ id, stageId, provider, storeId, key: `catalog/${stageId}/${id}`, state: 'prepared' })
        .returningAll()
        .executeTakeFirstOrThrow();
    });
  }

  startSending(id: string, leaseMs: number) {
    if (!Number.isSafeInteger(leaseMs) || leaseMs < 1 || leaseMs > 3_600_000) {
      throw new Error('Invalid lease duration');
    }
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
      const intent = await db
        .updateTable('object_upload_intent')
        .set({ state: 'acknowledged' })
        .where('id', '=', id)
        .where('token', '=', token)
        .where('state', '=', 'sending')
        .where('leaseUntil', '>', sql<Date>`clock_timestamp()`)
        .returningAll()
        .executeTakeFirstOrThrow();
      if (
        result.object.key !== intent.key ||
        (intent.provider === 's3'
          ? !result.object.versionId || result.object.versionId === 'null'
          : result.object.versionId !== undefined)
      ) {
        throw new Error('Provider returned an invalid exact identity');
      }
      return db
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
