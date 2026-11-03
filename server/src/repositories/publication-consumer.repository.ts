import { Injectable } from '@nestjs/common';
import { Kysely, Transaction, sql } from 'kysely';
import { InjectKysely } from 'nestjs-kysely';
import { randomUUID } from 'node:crypto';
import { JobStatus, WorkflowResult } from 'src/enum.js';
import { DB } from 'src/schema/index.js';
import { withPublicationLease } from 'src/utils/publication-consumer-lease.js';

export type ConsumerKind = 'metadata' | 'event' | 'workflow' | 'metadata-event';
export type ConsumerState = 'pending' | 'claimed' | 'done' | 'held';
export type PublicationIdentity = { effectId: string; receiptId: string; assetId: string; ownerId: string };
export type Consumer = {
  id: string;
  originId: string;
  receiptId: string;
  assetId: string;
  ownerId: string;
  kind: ConsumerKind;
  state: ConsumerState;
  token: string | null;
  expiresAt: Date | null;
  result: Record<string, unknown> | null;
};

@Injectable()
export class PublicationConsumerRepository {
  constructor(@InjectKysely() private readonly db: Kysely<DB>) {}

  async discover() {
    await sql`INSERT INTO publication_consumer (id,"originId","receiptId","assetId","ownerId",kind)
      SELECT o.id,o.id,o."receiptId",p."publishedAssetId",p."ownerId",o.kind
      FROM upload_publication_outbox o JOIN upload_publication p ON p.id=o."receiptId"
      WHERE p.state='published' AND o.payload=jsonb_build_object('assetId',p."publishedAssetId",'ownerId',p."ownerId",'quotaAlreadyCharged',true)
      ON CONFLICT DO NOTHING`.execute(this.db);
  }

  async takeDispatch(limit = 32) {
    await sql`UPDATE publication_consumer SET state=CASE WHEN kind='metadata' THEN 'held' ELSE 'pending' END,token=NULL,"expiresAt"=NULL
      WHERE state='claimed' AND "expiresAt"<=clock_timestamp()`.execute(this.db);
    const resolved1 = await sql<Consumer>`WITH pending AS (
      SELECT id FROM publication_consumer WHERE state='pending' AND ("dispatchUntil" IS NULL OR "dispatchUntil"<=clock_timestamp())
      ORDER BY id LIMIT ${limit} FOR UPDATE SKIP LOCKED
    ) UPDATE publication_consumer c SET "dispatchUntil"=clock_timestamp()+interval '30 seconds'
      FROM pending WHERE c.id=pending.id RETURNING c.*`.execute(this.db);
    return resolved1.rows;
  }

  async takeRunDispatch(limit = 32) {
    await sql`UPDATE publication_workflow_run SET state='held',token=NULL,"expiresAt"=NULL,result='{"reason":"expired ambiguous execution"}'::jsonb
      WHERE state='claimed' AND "expiresAt"<=clock_timestamp()`.execute(this.db);
    const resolved2 = await sql<Consumer & { runId: string; workflowId: string }>`WITH pending AS (
      SELECT id FROM publication_workflow_run WHERE state='pending' AND ("dispatchUntil" IS NULL OR "dispatchUntil"<=clock_timestamp())
      ORDER BY id LIMIT ${limit} FOR UPDATE SKIP LOCKED
    ), dispatched AS (
      UPDATE publication_workflow_run r SET "dispatchUntil"=clock_timestamp()+interval '30 seconds'
      FROM pending WHERE r.id=pending.id RETURNING r.*
    ) SELECT c.*,r.id AS "runId",r."workflowId" FROM dispatched r JOIN publication_consumer c ON c.id=r."effectId"`.execute(
      this.db,
    );
    return resolved2.rows;
  }

  async acknowledgeCompleted() {
    await sql`UPDATE publication_consumer SET "acknowledgedAt"=clock_timestamp() WHERE state='done' AND "acknowledgedAt" IS NULL`.execute(
      this.db,
    );
  }

  async list() {
    const resolved3 = await sql<Consumer>`SELECT * FROM publication_consumer ORDER BY id`.execute(this.db);
    return resolved3.rows;
  }

  async get(id: string) {
    const resolved4 = await sql<Consumer>`SELECT * FROM publication_consumer WHERE id=${id}::uuid`.execute(this.db);
    return resolved4.rows[0];
  }

  async claim(id: string, leaseMs: number) {
    return this.db.transaction().execute(async (db) => {
      const resolved5 = await sql<Consumer>`SELECT * FROM publication_consumer WHERE id=${id}::uuid FOR UPDATE`.execute(
        db,
      );
      const row = resolved5.rows[0];
      if (!row) {
        throw new Error('Unknown publication consumer');
      }
      if (row.state === 'claimed') {
        await sql`UPDATE publication_consumer SET state='held',token=NULL,"expiresAt"=NULL,result='{"reason":"expired ambiguous execution"}'::jsonb
          WHERE id=${id}::uuid AND "expiresAt"<=clock_timestamp()`.execute(db);
        const resolved6 = await sql<Consumer>`SELECT * FROM publication_consumer WHERE id=${id}::uuid`.execute(db);
        const current = resolved6.rows[0];
        return { ...current, acquired: false };
      }
      if (row.state !== 'pending') {
        return { ...row, acquired: false };
      }
      const token = randomUUID();
      const resolved7 =
        await sql<Consumer>`UPDATE publication_consumer SET state='claimed',token=${token}::uuid,"expiresAt"=clock_timestamp()+${leaseMs}*interval '1 millisecond'
        WHERE id=${id}::uuid RETURNING *`.execute(db);
      const claimed = resolved7.rows[0];
      return { ...claimed, acquired: true };
    });
  }

  async renew(id: string, token: string, leaseMs = 300_000, run = false) {
    const table = sql.table(run ? 'publication_workflow_run' : 'publication_consumer');
    await this.db.transaction().execute(async (db) => {
      await sql`SELECT id FROM ${table} WHERE id=${id}::uuid FOR UPDATE`.execute(db);
      const { rows } = await sql`UPDATE ${table} SET "expiresAt"=clock_timestamp()+${leaseMs}*interval '1 millisecond'
        WHERE id=${id}::uuid AND state='claimed' AND token=${token}::uuid AND "expiresAt">clock_timestamp() RETURNING id`.execute(
        db,
      );
      if (rows.length !== 1) {
        throw new Error('Lost publication lease');
      }
    });
  }

  private async fence(db: Transaction<DB>, id: string, token: string) {
    await sql`SELECT id FROM publication_consumer WHERE id=${id}::uuid FOR UPDATE`.execute(db);
    const resolved8 =
      await sql`SELECT id FROM publication_consumer WHERE id=${id}::uuid AND token=${token}::uuid AND state='claimed' AND "expiresAt">clock_timestamp()`.execute(
        db,
      );
    const valid = resolved8.rows;
    if (valid.length !== 1) {
      throw new Error('Lost publication lease');
    }
  }

  async runs(effectId: string) {
    const resolved9 = await sql<{
      id: string;
      effectId: string;
      workflowId: string;
      state: ConsumerState;
      token: string | null;
    }>`SELECT * FROM publication_workflow_run WHERE "effectId"=${effectId}::uuid ORDER BY id`.execute(this.db);
    return resolved9.rows;
  }

  async freezeFanout(id: string, token?: string) {
    await this.db.transaction().execute(async (db) => {
      const resolved10 =
        await sql<Consumer>`SELECT * FROM publication_consumer WHERE id=${id}::uuid FOR UPDATE`.execute(db);
      const row = resolved10.rows[0];
      if (!row || !['workflow', 'metadata-event'].includes(row.kind)) {
        throw new Error('Invalid fanout consumer');
      }
      if (row.state === 'done') {
        return;
      }
      if (token) {
        await this.fence(db, id, token);
      } else if (row.state !== 'pending') {
        return;
      }
      const trigger = row.kind === 'workflow' ? 'AssetCreate' : 'AssetMetadataExtraction';
      await sql`INSERT INTO publication_workflow_run ("effectId","workflowId")
        SELECT ${id}::uuid,id FROM workflow WHERE "ownerId"=${row.ownerId}::uuid AND trigger=${trigger} AND enabled=true
        ON CONFLICT DO NOTHING`.execute(db);
      if (token) {
        await this.complete(db, id, token, { cohortFrozen: true });
      } else {
        await sql`UPDATE publication_consumer SET state='done',token=NULL,"expiresAt"=NULL,result='{"cohortFrozen":true}'::jsonb WHERE id=${id}::uuid`.execute(
          db,
        );
      }
    });
  }

  async finishMetadata(id: string, token: string) {
    await this.db.transaction().execute(async (db) => {
      await this.fence(db, id, token);
      const resolved11 =
        await sql<Consumer>`SELECT * FROM publication_consumer WHERE id=${id}::uuid AND kind='metadata'`.execute(db);
      const row = resolved11.rows[0];
      if (!row) {
        throw new Error('Invalid metadata consumer');
      }
      await sql`INSERT INTO asset_job_status ("assetId","metadataExtractedAt") VALUES (${row.assetId}::uuid,clock_timestamp())
        ON CONFLICT ("assetId") DO UPDATE SET "metadataExtractedAt"=excluded."metadataExtractedAt"`.execute(db);
      await sql`INSERT INTO publication_consumer (id,"originId","receiptId","assetId","ownerId",kind)
        VALUES (${randomUUID()}::uuid,${row.originId}::uuid,${row.receiptId}::uuid,${row.assetId}::uuid,${row.ownerId}::uuid,'metadata-event') ON CONFLICT DO NOTHING`.execute(
        db,
      );
      await this.complete(db, id, token, { metadataExtracted: true });
    });
  }

  async consumeRun(
    runId: string,
    identity: PublicationIdentity,
    workflowId: string,
    assetId: string,
    work: (runId: string) => Promise<unknown>,
  ) {
    const effect = await this.get(identity.effectId);
    const resolved12 = await sql<{
      workflowId: string;
    }>`SELECT "workflowId" FROM publication_workflow_run WHERE id=${runId}::uuid AND "effectId"=${identity.effectId}::uuid`.execute(
      this.db,
    );
    const run = resolved12.rows[0];
    if (
      !effect ||
      !run ||
      run.workflowId !== workflowId ||
      effect.receiptId !== identity.receiptId ||
      effect.ownerId !== identity.ownerId ||
      effect.assetId !== identity.assetId ||
      assetId !== effect.assetId
    ) {
      throw new Error('Invalid publication workflow identity');
    }
    const claim = await this.db.transaction().execute(async (db) => {
      const resolved13 = await sql<{
        state: ConsumerState;
      }>`SELECT state FROM publication_workflow_run WHERE id=${runId}::uuid FOR UPDATE`.execute(db);
      const row = resolved13.rows[0];
      if (row.state === 'claimed') {
        await sql`UPDATE publication_workflow_run SET state='held',token=NULL,"expiresAt"=NULL,result='{"reason":"expired ambiguous execution"}'::jsonb
          WHERE id=${runId}::uuid AND "expiresAt"<=clock_timestamp()`.execute(db);
        return;
      }
      if (row.state !== 'pending') {
        return;
      }
      const token = randomUUID();
      await sql`UPDATE publication_workflow_run SET state='claimed',token=${token}::uuid,"expiresAt"=clock_timestamp()+interval '5 minutes' WHERE id=${runId}::uuid`.execute(
        db,
      );
      return token;
    });
    if (!claim) {
      return JobStatus.Skipped;
    }
    try {
      const result = await withPublicationLease(
        () => work(runId),
        () => this.renew(runId, claim, 300_000, true),
      );
      if (
        ![WorkflowResult.Completed, WorkflowResult.Halted, JobStatus.Skipped].includes(
          result as WorkflowResult | JobStatus.Skipped,
        )
      ) {
        throw new Error('Ambiguous workflow result');
      }
      await this.db.transaction().execute(async (db) => {
        await sql`SELECT id FROM publication_workflow_run WHERE id=${runId}::uuid FOR UPDATE`.execute(db);
        const resolved14 =
          await sql`UPDATE publication_workflow_run SET state='done',token=NULL,"expiresAt"=NULL,result=${{ result }}::jsonb
          WHERE id=${runId}::uuid AND state='claimed' AND token=${claim}::uuid AND "expiresAt">clock_timestamp() RETURNING id`.execute(
            db,
          );
        const resultRows = resolved14.rows;
        if (resultRows.length !== 1) {
          throw new Error('Lost publication run lease');
        }
      });
      return JobStatus.Success;
    } catch (error) {
      await this.db.transaction().execute(async (db) => {
        await sql`SELECT id FROM publication_workflow_run WHERE id=${runId}::uuid FOR UPDATE`.execute(db);
        await sql`UPDATE publication_workflow_run SET state='held',token=NULL,"expiresAt"=NULL,result='{"reason":"ambiguous workflow execution"}'::jsonb WHERE id=${runId}::uuid AND state='claimed' AND token=${claim}::uuid`.execute(
          db,
        );
      });
      if (error instanceof Error && error.message === 'Ambiguous workflow result') {
        return JobStatus.Failed;
      }
      throw error;
    }
  }

  async consumeMetadata(assetId: string, identity: PublicationIdentity, work: () => Promise<unknown>) {
    const row = await this.get(identity.effectId);
    if (
      !row ||
      row.kind !== 'metadata' ||
      row.receiptId !== identity.receiptId ||
      row.assetId !== identity.assetId ||
      row.ownerId !== identity.ownerId ||
      assetId !== row.assetId
    ) {
      throw new Error('Invalid publication metadata identity');
    }
    const claim = await this.claim(row.id, 300_000);
    if (!claim.acquired) {
      return JobStatus.Skipped;
    }
    try {
      const result = await withPublicationLease(work, () => this.renew(row.id, claim.token!));
      if (result === JobStatus.Success) {
        await this.finishMetadata(row.id, claim.token!);
        return JobStatus.Success;
      }
      if (result === JobStatus.Skipped) {
        await this.finish(row.id, claim.token!, { skipped: 'asset missing or deleted' });
        return JobStatus.Skipped;
      }
      throw new Error('Metadata did not report success');
    } catch (error) {
      await this.hold(row.id, claim.token!, 'Ambiguous partial metadata execution');
      throw error;
    }
  }

  async hold(id: string, token: string, reason: string) {
    await this.db.transaction().execute(async (db) => {
      await sql`SELECT id FROM publication_consumer WHERE id=${id}::uuid FOR UPDATE`.execute(db);
      await sql`UPDATE publication_consumer SET state='held',token=NULL,"expiresAt"=NULL,result=${{ reason }}::jsonb WHERE id=${id}::uuid AND state='claimed' AND token=${token}::uuid`.execute(
        db,
      );
    });
  }

  private async complete(db: Transaction<DB>, id: string, token: string, result: Record<string, unknown>) {
    const { rows } =
      await sql`UPDATE publication_consumer SET state='done',token=NULL,"expiresAt"=NULL,result=${result}::jsonb
      WHERE id=${id}::uuid AND state='claimed' AND token=${token}::uuid AND "expiresAt">clock_timestamp() RETURNING id`.execute(
        db,
      );
    if (rows.length !== 1) {
      throw new Error('Lost publication lease');
    }
  }

  async finish(id: string, token: string, result: Record<string, unknown>) {
    await this.db.transaction().execute(async (db) => {
      await this.fence(db, id, token);
      await this.complete(db, id, token, result);
    });
  }
}
