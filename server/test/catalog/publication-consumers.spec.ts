import { WorkflowTrigger } from '@immich/plugin-sdk';
import { schemaFromCode } from '@immich/sql-tools';
import { Kysely, sql } from 'kysely';
import { PostgresJSDialect } from 'kysely-postgres-js';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { stat } from 'node:fs/promises';
import { createServer } from 'node:http';
import os from 'node:os';
import path from 'node:path';
import postgres from 'postgres';
import { JobName, JobStatus, QueueName, WorkflowResult, WorkflowType } from 'src/enum.js';
import { PublicationConsumerRepository } from 'src/repositories/publication-consumer.repository.js';
import { DB, ImmichDatabase } from 'src/schema/index.js';
import { up as catalogUp } from 'src/schema/migrations/1793500000000-AddObjectCatalog.js';
import { up as multipartUp } from 'src/schema/migrations/1793500001000-AddS3MultipartJournal.js';
import { up as originalUp } from 'src/schema/migrations/1793500002000-AddAssetOriginal.js';
import { up as sourceUp } from 'src/schema/migrations/1793500003000-AddUploadSourceReceipt.js';
import { up as publicationUp } from 'src/schema/migrations/1793500004000-AddUploadPublication.js';
import { down as consumerDown, up as consumerUp } from 'src/schema/migrations/1793500005000-AddPublicationConsumers.js';
import { JobService } from 'src/services/job.service.js';
import { MetadataService } from 'src/services/metadata.service.js';
import { PublicationConsumerService } from 'src/services/publication-consumer.service.js';
import { UserService } from 'src/services/user.service.js';
import { WorkflowExecutionService } from 'src/services/workflow-execution.service.js';
import { setupConsumerHttp } from 'test/catalog/publication-consumer-http-fixture.js';
import { publicationFixture } from 'test/catalog/upload-publication-fixture.js';
import { AssetFactory } from 'test/factories/asset.factory.js';
import { getForMetadataExtraction } from 'test/mappers.js';
import { newTestService } from 'test/utils.js';

const socket = process.env.CATALOG_TEST_SOCKET;
if (process.env.CATALOG_TEST_DISPOSABLE !== 'YES' || !socket || !path.isAbsolute(socket)) {
  throw new Error('Requires disposable private PostgreSQL socket');
}
const database = `immich_consumers_${randomUUID().replaceAll('-', '')}`;
const options = { host: socket, port: Number(process.env.CATALOG_TEST_PORT || 55_441), user: os.userInfo().username };
const admin = postgres({ ...options, database: 'postgres', max: 1 });
let db: Kysely<DB>;
beforeEach(async () => {
  await admin.unsafe(`CREATE DATABASE "${database}"`);
  db = new Kysely<DB>({ dialect: new PostgresJSDialect({ postgres: postgres({ ...options, database }) }) });
  await sql`CREATE TABLE "user" (id uuid PRIMARY KEY, "deletedAt" timestamptz, status varchar NOT NULL DEFAULT 'active', "quotaSizeInBytes" bigint, "quotaUsageInBytes" bigint NOT NULL DEFAULT 0);
    CREATE TABLE asset (id uuid PRIMARY KEY, "ownerId" uuid NOT NULL REFERENCES "user"(id), "originalPath" varchar NOT NULL, checksum bytea NOT NULL, "checksumAlgorithm" varchar NOT NULL DEFAULT 'sha1', "libraryId" uuid, type varchar NOT NULL, "fileCreatedAt" timestamptz NOT NULL, "fileModifiedAt" timestamptz NOT NULL, "localDateTime" timestamptz NOT NULL, "originalFileName" varchar NOT NULL, "isFavorite" boolean NOT NULL DEFAULT false, visibility varchar NOT NULL DEFAULT 'timeline', duration integer, "livePhotoVideoId" uuid REFERENCES asset(id), "deletedAt" timestamptz);
    CREATE UNIQUE INDEX "UQ_assets_owner_checksum" ON asset ("ownerId", checksum) WHERE "libraryId" IS NULL;
    CREATE TABLE asset_exif ("assetId" uuid PRIMARY KEY REFERENCES asset(id), "fileSizeInByte" bigint NOT NULL, description varchar);
    CREATE TABLE asset_metadata ("assetId" uuid NOT NULL REFERENCES asset(id), key varchar NOT NULL, value jsonb NOT NULL, PRIMARY KEY ("assetId",key));
    CREATE TABLE asset_file (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), "assetId" uuid NOT NULL REFERENCES asset(id), type varchar NOT NULL, path varchar NOT NULL);
    CREATE TABLE session (id uuid PRIMARY KEY, "userId" uuid NOT NULL REFERENCES "user"(id), "expiresAt" timestamptz);
    CREATE TABLE api_key (id uuid PRIMARY KEY, "userId" uuid NOT NULL REFERENCES "user"(id), permissions varchar[] NOT NULL);
    CREATE TABLE album (id uuid PRIMARY KEY, "deletedAt" timestamptz);
    CREATE TABLE album_user ("albumId" uuid REFERENCES album(id), "userId" uuid REFERENCES "user"(id), role varchar NOT NULL, PRIMARY KEY ("albumId", "userId"));
    CREATE TABLE shared_link (id uuid PRIMARY KEY, "userId" uuid NOT NULL REFERENCES "user"(id), type varchar NOT NULL, "albumId" uuid REFERENCES album(id), "expiresAt" timestamptz, "allowUpload" boolean NOT NULL);
    CREATE TABLE shared_link_asset ("sharedLinkId" uuid REFERENCES shared_link(id), "assetId" uuid REFERENCES asset(id), PRIMARY KEY ("sharedLinkId","assetId"));
    CREATE TABLE album_asset ("albumId" uuid REFERENCES album(id), "assetId" uuid REFERENCES asset(id), PRIMARY KEY ("albumId","assetId"));`.execute(
    db,
  );
  await sql`CREATE TABLE workflow (id uuid PRIMARY KEY, "ownerId" uuid NOT NULL REFERENCES "user"(id), trigger varchar NOT NULL, enabled boolean NOT NULL DEFAULT true);
    CREATE TABLE asset_job_status ("assetId" uuid PRIMARY KEY REFERENCES asset(id), "metadataExtractedAt" timestamptz)`.execute(
    db,
  );
  await catalogUp(db);
  await multipartUp(db);
  await originalUp(db);
  await sourceUp(db);
  await publicationUp(db);
  await consumerUp(db);
  await consumerDown(db);
  await consumerUp(db);
});
afterEach(async () => {
  await db?.destroy();
  await admin.unsafe(`DROP DATABASE "${database}"`);
});
afterAll(async () => {
  await admin.end();
});
it('installs durable consumer ledger independently of immutable pending outbox', async () => {
  const result = await sql<{
    exists: boolean;
  }>`SELECT to_regclass('publication_consumer') IS NOT NULL AS exists`.execute(db);
  expect(result.rows[0].exists).toBe(true);
});

it('seeds exact immutable identities and claims once across concurrent workers', async () => {
  const repository = new PublicationConsumerRepository(db);
  const f = await publicationFixture(db);
  try {
    await f.service.publish(f.input);
    await Promise.all([repository.discover(), repository.discover()]);
    const rows = await repository.list();
    const effects = rows.filter((row: any) => row.ownerId === f.ownerId);
    expect(effects).toHaveLength(3);
    const event = effects.find((row) => row.kind === 'event')!;
    await expect(
      sql`UPDATE publication_consumer SET "acknowledgedAt"=clock_timestamp() WHERE id=${event.id}::uuid`.execute(db),
    ).rejects.toThrow();
    const claims = await Promise.all([repository.claim(event.id, 30_000), repository.claim(event.id, 30_000)]);
    expect(claims.filter((row: any) => row.acquired)).toHaveLength(1);
    const claimed = claims.find((row) => row.acquired)!;
    await expect(repository.finish(event.id, randomUUID(), { delivered: true })).rejects.toThrow(
      'Lost publication lease',
    );
    await repository.finish(event.id, claimed.token!, { delivered: true });
    expect(await repository.claim(event.id, 30_000)).toMatchObject({ acquired: false, state: 'done' });
    const resolved1 = await db
      .selectFrom('upload_publication_outbox')
      .selectAll()
      .where('id', '=', event.id)
      .executeTakeFirstOrThrow();
    expect(resolved1.state).toBe('pending');
    await expect(
      sql`UPDATE publication_consumer SET "ownerId"=${randomUUID()}::uuid WHERE id=${event.id}::uuid`.execute(db),
    ).rejects.toThrow();
  } finally {
    await f.close();
  }
});

it('binds receipt, originating effect, immutable asset and owner even when all IDs exist', async () => {
  const repository = new PublicationConsumerRepository(db);
  const first = await publicationFixture(db);
  const second = await publicationFixture(db);
  try {
    await first.service.publish(first.input);
    await second.service.publish(second.input);
    await repository.discover();
    const rows = await repository.list();
    const a = rows.find((row) => row.ownerId === first.ownerId)!;
    const b = rows.find((row) => row.ownerId === second.ownerId)!;
    await expect(
      sql`UPDATE publication_consumer SET "receiptId"=${b.receiptId}::uuid WHERE id=${a.id}::uuid`.execute(db),
    ).rejects.toThrow();
    await expect(
      sql`UPDATE publication_consumer SET "assetId"=${b.assetId}::uuid,"ownerId"=${b.ownerId}::uuid WHERE id=${a.id}::uuid`.execute(
        db,
      ),
    ).rejects.toThrow();
  } finally {
    await first.close();
    await second.close();
  }
});

it('freezes enabled owned workflow cohort and completion atomically including empty cohorts', async () => {
  const repository = new PublicationConsumerRepository(db);
  const f = await publicationFixture(db);
  try {
    await f.service.publish(f.input);
    await repository.discover();
    const resolved2 = await repository.list();
    const effect = resolved2.find((row) => row.ownerId === f.ownerId && row.kind === 'workflow')!;
    const workflowId = randomUUID();
    await sql`INSERT INTO workflow (id,"ownerId",trigger) VALUES (${workflowId}::uuid,${f.ownerId}::uuid,'AssetCreate')`.execute(
      db,
    );
    await Promise.all([repository.freezeFanout(effect.id), repository.freezeFanout(effect.id)]);
    const runs = await repository.runs(effect.id);
    expect(runs).toHaveLength(1);
    const runId = runs[0].id;
    await sql`INSERT INTO workflow (id,"ownerId",trigger) VALUES (${randomUUID()}::uuid,${f.ownerId}::uuid,'AssetCreate')`.execute(
      db,
    );
    await repository.freezeFanout(effect.id);
    const resolved3 = await repository.runs(effect.id);
    expect(resolved3.map((row) => row.id)).toEqual([runId]);
    expect(await repository.get(effect.id)).toMatchObject({ state: 'done' });
    const resolved4 = await repository.list();
    const metadata = resolved4.find((row) => row.ownerId === f.ownerId && row.kind === 'metadata')!;
    const claim = await repository.claim(metadata.id, 30_000);
    await repository.finishMetadata(metadata.id, claim.token!);
    const resolved5 = await repository.list();
    const derived = resolved5.find((row) => row.originId === metadata.id && row.kind === 'metadata-event')!;
    await repository.freezeFanout(derived.id);
    await sql`INSERT INTO workflow (id,"ownerId",trigger) VALUES (${randomUUID()}::uuid,${f.ownerId}::uuid,'AssetMetadataExtraction')`.execute(
      db,
    );
    await repository.freezeFanout(derived.id);
    expect(await repository.runs(derived.id)).toEqual([]);
    expect(await repository.get(derived.id)).toMatchObject({ state: 'done' });
    const resolved6 = await sql`SELECT * FROM asset_job_status WHERE "assetId"=${metadata.assetId}::uuid`.execute(db);
    expect(resolved6.rows).toHaveLength(1);
  } finally {
    await f.close();
  }
});

it('actual metadata handler commits status and derived effect once and skips committed replay', async () => {
  const repository = new PublicationConsumerRepository(db);
  const f = await publicationFixture(db);
  const { sut, mocks } = newTestService(MetadataService);
  Object.assign(sut, { publicationConsumers: repository });
  try {
    const result = await f.service.publish(f.input);
    await repository.discover();
    const resolved7 = await repository.list();
    const effect = resolved7.find((row) => row.ownerId === f.ownerId && row.kind === 'metadata')!;
    const asset = AssetFactory.create({
      id: result.id,
      ownerId: f.ownerId,
      originalPath: path.join(f.root, f.source.name),
    });
    mocks.assetJob.getForMetadataExtraction.mockResolvedValue(getForMetadataExtraction(asset));
    mocks.metadata.readTags.mockResolvedValue({ ImageDescription: 'native fixture metadata' });
    mocks.storage.stat.mockImplementation(stat as any);
    mocks.asset.upsertExif.mockImplementation(async ({ exif }) => {
      await sql`UPDATE asset_exif SET "fileSizeInByte"=${exif.fileSizeInByte},description=${exif.description} WHERE "assetId"=${exif.assetId}::uuid`.execute(
        db,
      );
    });
    const publication = {
      effectId: effect.id,
      receiptId: effect.receiptId,
      assetId: effect.assetId,
      ownerId: effect.ownerId,
    };
    await sut.handleMetadataExtraction({ id: effect.assetId, publication } as any);
    expect(await repository.get(effect.id)).toMatchObject({ state: 'done' });
    expect(mocks.asset.upsertJobStatus).not.toHaveBeenCalled();
    expect(mocks.event.emit).not.toHaveBeenCalledWith('AssetMetadataExtracted', expect.anything());
    const resolved8 = await repository.list();
    const derived = resolved8.filter((row) => row.originId === effect.id && row.kind === 'metadata-event');
    expect(derived).toHaveLength(1);
    await sut.handleMetadataExtraction({ id: effect.assetId, publication } as any);
    expect(mocks.asset.upsertExif).toHaveBeenCalledTimes(1);
    const resolved9 = await repository.list();
    expect(resolved9.filter((row) => row.originId === effect.id && row.kind === 'metadata-event')).toEqual(derived);
  } finally {
    await sut.onShutdown();
    await f.close();
  }
});

it('holds partial metadata failures and does not turn undefined missing assets into successful extraction', async () => {
  const repository = new PublicationConsumerRepository(db);
  const f = await publicationFixture(db);
  const { sut, mocks } = newTestService(MetadataService);
  Object.assign(sut, { publicationConsumers: repository });
  try {
    await f.service.publish(f.input);
    await repository.discover();
    const resolved10 = await repository.list();
    const effect = resolved10.find((row) => row.ownerId === f.ownerId && row.kind === 'metadata')!;
    const asset = AssetFactory.create({
      id: effect.assetId,
      ownerId: f.ownerId,
      originalPath: path.join(f.root, f.source.name),
    });
    mocks.assetJob.getForMetadataExtraction.mockResolvedValue(getForMetadataExtraction(asset));
    mocks.metadata.readTags.mockResolvedValue({});
    mocks.storage.stat.mockImplementation(stat as any);
    mocks.asset.upsertExif.mockRejectedValue(new Error('partial tag write'));
    const publication = {
      effectId: effect.id,
      receiptId: effect.receiptId,
      assetId: effect.assetId,
      ownerId: effect.ownerId,
    };
    const jobs = newTestService(JobService);
    jobs.mocks.job.run.mockImplementation(
      async () => (await sut.handleMetadataExtraction({ id: effect.assetId, publication })) ?? JobStatus.Failed,
    );
    await jobs.sut.onJobRun(QueueName.MetadataExtraction, {
      name: JobName.AssetExtractMetadata,
      data: { id: effect.assetId, publication },
    });
    expect(jobs.mocks.event.emit).toHaveBeenCalledWith('JobComplete', QueueName.MetadataExtraction, expect.anything());
    expect(await repository.get(effect.id)).toMatchObject({ state: 'held' });
    await sut.handleMetadataExtraction({ id: effect.assetId, publication } as any);
    expect(mocks.asset.upsertExif).toHaveBeenCalledTimes(1);
    const resolved11 = await repository.list();
    expect(resolved11.filter((row) => row.originId === effect.id && row.kind === 'metadata-event')).toEqual([]);
  } finally {
    await sut.onShutdown();
    await f.close();
  }
});

it('uses persisted run UUID in actual workflow steps and holds ambiguous plugin failures without fresh retry', async () => {
  const repository = new PublicationConsumerRepository(db);
  const f = await publicationFixture(db);
  const { sut, mocks } = newTestService(WorkflowExecutionService);
  Object.assign(sut, { publicationConsumers: repository });
  try {
    await f.service.publish(f.input);
    await repository.discover();
    const resolved12 = await repository.list();
    const effect = resolved12.find((row) => row.ownerId === f.ownerId && row.kind === 'workflow')!;
    const workflowId = randomUUID();
    await sql`INSERT INTO workflow (id,"ownerId",trigger) VALUES (${workflowId}::uuid,${f.ownerId}::uuid,'AssetCreate')`.execute(
      db,
    );
    await repository.freezeFanout(effect.id);
    const [run] = await repository.runs(effect.id);
    mocks.workflow.getForWorkflowRun.mockResolvedValue({
      id: workflowId,
      name: 'controlled plugin',
      trigger: WorkflowTrigger.AssetCreate,
      logging: true,
      steps: [
        {
          id: randomUUID(),
          config: {},
          pluginId: randomUUID(),
          methodName: 'externalHttp',
          types: [WorkflowType.AssetV1],
          hostFunctions: true,
          allowedHosts: [],
        },
      ],
    } as any);
    mocks.workflow.getForAssetV1.mockResolvedValue({ id: effect.assetId, ownerId: effect.ownerId } as any);
    let externalCalls = 0;
    const pluginEndpoint = createServer((_request, response) => {
      externalCalls++;
      response.writeHead(200);
      response.end('accepted');
    });
    onTestFinished(async () => {
      pluginEndpoint.closeAllConnections();
      await new Promise<void>((resolve) => pluginEndpoint.close(() => resolve()));
    });
    pluginEndpoint.listen(0, '127.0.0.1');
    await once(pluginEndpoint, 'listening');
    const endpoint = `http://127.0.0.1:${(pluginEndpoint.address() as { port: number }).port}`;
    mocks.workflow.log.mockResolvedValue([]);
    mocks.crypto.signJwt.mockReturnValue('fixture-token');
    mocks.plugin.callMethod.mockImplementation(async () => {
      await fetch(endpoint);
      throw new Error('crashed after external HTTP accepted');
    });
    const publication = {
      effectId: effect.id,
      receiptId: effect.receiptId,
      assetId: effect.assetId,
      ownerId: effect.ownerId,
    };
    const job = {
      workflowId,
      assetId: effect.assetId,
      trigger: WorkflowTrigger.AssetCreate,
      publication,
      publicationRunId: run.id,
    };
    await Promise.all([sut.handleAssetTrigger(job), sut.handleAssetTrigger(job)]);
    expect(externalCalls).toBe(1);
    const resolved13 = await repository.runs(effect.id);
    expect(resolved13[0]).toMatchObject({ id: run.id, state: 'held' });
    expect(mocks.workflow.log).toHaveBeenCalledWith(expect.objectContaining({ runId: run.id }));
    await sut.handleAssetTrigger(job);
    expect(externalCalls).toBe(1);
    const resolved14 = await repository.runs(effect.id);
    expect(resolved14.map((row) => row.id)).toEqual([run.id]);
  } finally {
    await f.close();
  }
});

it('drains actual POST publication through registered worker ticks, SQL redispatch and actual service handlers', async () => {
  const http = await setupConsumerHttp(db);
  const f = http.f;
  const repository = new PublicationConsumerRepository(db);
  const metadata = newTestService(MetadataService);
  const workflow = newTestService(WorkflowExecutionService);
  const user = newTestService(UserService);
  Object.assign(metadata.sut, { publicationConsumers: repository });
  Object.assign(workflow.sut, { publicationConsumers: repository });
  let lost = true;
  const jobs: any[] = [];
  const queue = {
    queue: vi.fn(async (job: any) => {
      jobs.push(job);
      if (lost) {
        return;
      }
      if (job.name === JobName.AssetExtractMetadata) {
        await metadata.sut.handleMetadataExtraction(job.data);
      } else {
        await workflow.sut.handleAssetTrigger(job.data);
      }
    }),
  };
  const events = {
    emit: vi.fn(async (name: string, data: any) => {
      if (name === 'AssetCreate') {
        expect(data).not.toHaveProperty('file');
        await user.sut.onAssetCreate(data);
        await workflow.sut.onAssetCreate(data);
      } else {
        await workflow.sut.onAssetMetadataExtracted(data);
      }
    }),
  };
  const worker = new PublicationConsumerService(repository, queue as any, events as any, { error: vi.fn() } as any);
  try {
    const response = await http.post();
    expect(response.status).toBe(201);
    const id = response.body.id;
    const rows = await db
      .selectFrom('upload_publication_outbox')
      .selectAll()
      .where('payload', '@>', { assetId: id } as any)
      .execute();
    expect(rows).toHaveLength(3);
    const workflowId = randomUUID();
    await sql`INSERT INTO workflow (id,"ownerId",trigger) VALUES (${workflowId}::uuid,${f.ownerId}::uuid,'AssetCreate')`.execute(
      db,
    );
    metadata.mocks.assetJob.getForMetadataExtraction.mockImplementation(async () => {
      const asset = await db.selectFrom('asset').selectAll().where('id', '=', id).executeTakeFirstOrThrow();
      return getForMetadataExtraction(AssetFactory.create({ ...asset, checksum: Buffer.from(asset.checksum) } as any));
    });
    metadata.mocks.metadata.readTags.mockResolvedValue({ ImageDescription: 'retained source processed' });
    metadata.mocks.storage.stat.mockImplementation(stat as any);
    metadata.mocks.asset.upsertExif.mockImplementation(async ({ exif }) => {
      await sql`UPDATE asset_exif SET "fileSizeInByte"=${exif.fileSizeInByte},description=${exif.description} WHERE "assetId"=${exif.assetId}::uuid`.execute(
        db,
      );
    });
    workflow.mocks.workflow.getForWorkflowRun.mockResolvedValue({
      id: workflowId,
      name: 'fixture',
      trigger: WorkflowTrigger.AssetCreate,
      logging: true,
      steps: [
        {
          id: randomUUID(),
          config: {},
          pluginId: randomUUID(),
          methodName: 'process',
          types: [WorkflowType.AssetV1],
          hostFunctions: false,
          allowedHosts: [],
        },
      ],
    } as any);
    workflow.mocks.workflow.getForAssetV1.mockResolvedValue({ id, ownerId: f.ownerId } as any);
    workflow.mocks.workflow.log.mockResolvedValue([]);
    workflow.mocks.crypto.signJwt.mockReturnValue('fixture');
    workflow.mocks.plugin.callMethod.mockResolvedValue({});
    const ack = vi.spyOn(repository, 'acknowledgeCompleted').mockRejectedValueOnce(new Error('dispatch ACK lost'));
    await expect(worker.tick()).rejects.toThrow('dispatch ACK lost');
    expect(ack).toHaveBeenCalledTimes(1);
    expect(metadata.mocks.asset.upsertExif).not.toHaveBeenCalled();
    const resolved15 = await repository.list();
    expect(resolved15.find((row) => row.assetId === id && row.kind === 'metadata')).toMatchObject({
      state: 'pending',
    });
    lost = false;
    await sql`UPDATE publication_consumer SET "dispatchUntil"=clock_timestamp()-interval '1 second' WHERE "ownerId"=${f.ownerId}::uuid`.execute(
      db,
    );
    await sql`UPDATE publication_workflow_run SET "dispatchUntil"=clock_timestamp()-interval '1 second'`.execute(db);
    ack.mockRejectedValueOnce(new Error('metadata ACK lost'));
    await expect(worker.tick()).rejects.toThrow('metadata ACK lost');
    await worker.tick();
    const resolved16 = await repository.list();
    const effects = resolved16.filter((row) => row.assetId === id);
    expect(effects).toHaveLength(4);
    expect(effects.every((row) => row.state === 'done')).toBe(true);
    const origin = effects.find((row) => row.kind === 'workflow')!;
    const runs = await repository.runs(origin.id);
    expect(runs).toHaveLength(1);
    expect(runs[0]).toMatchObject({ state: 'done', result: { result: WorkflowResult.Completed } });
    const extracted = await sql<{
      description: string;
    }>`SELECT description FROM asset_exif WHERE "assetId"=${id}::uuid`.execute(db);
    expect(extracted.rows[0].description).toBe('retained source processed');
    expect(workflow.mocks.workflow.log).toHaveBeenCalledWith(expect.objectContaining({ runId: runs[0].id }));
    expect(metadata.mocks.asset.upsertExif).toHaveBeenCalledTimes(1);
    expect(workflow.mocks.plugin.callMethod).toHaveBeenCalledTimes(1);
    expect(user.mocks.user.updateUsage).not.toHaveBeenCalled();
    expect(await f.usage()).toBe(String(Buffer.byteLength('actual multipart bytes')));
    expect(events.emit).toHaveBeenCalledTimes(2);
    expect(jobs.filter((job) => job.name === JobName.AssetExtractMetadata)).toHaveLength(2);
    await worker.tick();
    expect(metadata.mocks.asset.upsertExif).toHaveBeenCalledTimes(1);
  } finally {
    await worker.onShutdown();
    await metadata.sut.onShutdown();
    await http.close();
  }
});

it('rechecks database expiry after row lock waits and fences renewal and stale completion tokens', async () => {
  const repository = new PublicationConsumerRepository(db);
  const f = await publicationFixture(db);
  try {
    await f.service.publish(f.input);
    await repository.discover();
    const resolved17 = await repository.list();
    const effect = resolved17.find((row) => row.kind === 'metadata')!;
    const claim = await repository.claim(effect.id, 30_000);
    await expect(repository.renew(effect.id, randomUUID(), 30_000)).rejects.toThrow('Lost publication lease');
    await repository.renew(effect.id, claim.token!, 100);
    const acquired = Promise.withResolvers<void>();
    const gate = Promise.withResolvers<void>();
    const blocker = db.transaction().execute(async (tx) => {
      await sql`SELECT id FROM publication_consumer WHERE id=${effect.id}::uuid FOR UPDATE`.execute(tx);
      acquired.resolve();
      await gate.promise;
    });
    await acquired.promise;
    const finish = repository.finishMetadata(effect.id, claim.token!);
    const outcome = finish.then(() => 'unexpected success').catch((error: Error) => error.message);
    await sql`SELECT pg_sleep(0.2)`.execute(db);
    gate.resolve();
    await blocker;
    expect(await outcome).toBe('Lost publication lease');
    const resolved18 = await sql`SELECT * FROM asset_job_status`.execute(db);
    expect(resolved18.rows).toEqual([]);
    expect(await repository.claim(effect.id, 30_000)).toMatchObject({ state: 'held', acquired: false });
    await expect(repository.renew(effect.id, claim.token!, 30_000)).rejects.toThrow('Lost publication lease');
  } finally {
    await f.close();
  }
});

it('rolls metadata status, completion and derived event back together if derived persistence fails', async () => {
  const repository = new PublicationConsumerRepository(db);
  const f = await publicationFixture(db);
  try {
    await f.service.publish(f.input);
    await repository.discover();
    const resolved19 = await repository.list();
    const effect = resolved19.find((row) => row.kind === 'metadata')!;
    const claim = await repository.claim(effect.id, 30_000);
    await sql`CREATE FUNCTION reject_derived() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.kind='metadata-event' THEN RAISE EXCEPTION 'derived write failed'; END IF; RETURN NEW; END $$;
      CREATE TRIGGER reject_derived BEFORE INSERT ON publication_consumer FOR EACH ROW EXECUTE FUNCTION reject_derived()`.execute(
      db,
    );
    await expect(repository.finishMetadata(effect.id, claim.token!)).rejects.toThrow('derived write failed');
    expect(await repository.get(effect.id)).toMatchObject({ state: 'claimed', token: claim.token });
    const resolved20 = await sql`SELECT * FROM asset_job_status`.execute(db);
    expect(resolved20.rows).toEqual([]);
    const resolved21 = await repository.list();
    expect(resolved21.filter((row) => row.kind === 'metadata-event')).toEqual([]);
  } finally {
    await f.close();
  }
});

it('rejects workflow enrollment after frozen fanout and cross-owner workflows even with valid foreign keys', async () => {
  const repository = new PublicationConsumerRepository(db);
  const f = await publicationFixture(db);
  const other = await publicationFixture(db);
  try {
    await f.service.publish(f.input);
    await repository.discover();
    const resolved22 = await repository.list();
    const effect = resolved22.find((row) => row.kind === 'workflow')!;
    const wrongOwner = randomUUID();
    await sql`INSERT INTO workflow (id,"ownerId",trigger) VALUES (${wrongOwner}::uuid,${other.ownerId}::uuid,'AssetCreate')`.execute(
      db,
    );
    await expect(
      sql`INSERT INTO publication_workflow_run ("effectId","workflowId") VALUES (${effect.id}::uuid,${wrongOwner}::uuid)`.execute(
        db,
      ),
    ).rejects.toThrow();
    await repository.freezeFanout(effect.id);
    const late = randomUUID();
    await sql`INSERT INTO workflow (id,"ownerId",trigger) VALUES (${late}::uuid,${f.ownerId}::uuid,'AssetCreate')`.execute(
      db,
    );
    await expect(
      sql`INSERT INTO publication_workflow_run ("effectId","workflowId") VALUES (${effect.id}::uuid,${late}::uuid)`.execute(
        db,
      ),
    ).rejects.toThrow();
  } finally {
    await f.close();
    await other.close();
  }
});

it('matches consumer decorators, columns, constraints, indexes and restrictive foreign keys', async () => {
  expect(new ImmichDatabase().tables.map((table) => table.name)).toContain('PublicationConsumerTable');
  const tables = schemaFromCode({ databaseName: 'immich' }).tables.filter((table) =>
    ['publication_consumer', 'publication_workflow_run'].includes(table.name),
  );
  expect(tables).toHaveLength(2);
  for (const table of tables) {
    const columns = await sql<{
      name: string;
      type: string;
      notnull: boolean;
    }>`SELECT attname AS name, format_type(atttypid, atttypmod) AS type, attnotnull AS notnull FROM pg_attribute WHERE attrelid=${table.name}::regclass AND attnum>0 AND NOT attisdropped`.execute(
      db,
    );
    expect(columns.rows.toSorted((a, b) => a.name.localeCompare(b.name))).toEqual(
      table.columns
        .map((column) => ({ name: column.name, type: column.type, notnull: !column.nullable }))
        .toSorted((a, b) => a.name.localeCompare(b.name)),
    );
    const constraints = await sql<{
      name: string;
    }>`SELECT conname AS name FROM pg_constraint WHERE conrelid=${table.name}::regclass AND contype<>'n'`.execute(db);
    expect(constraints.rows.map((row) => row.name).sort()).toEqual(
      table.constraints.map((constraint) => constraint.name).sort(),
    );
    const indexes = await sql<{
      name: string;
    }>`SELECT c.relname AS name FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE i.indrelid=${table.name}::regclass AND NOT EXISTS (SELECT 1 FROM pg_constraint p WHERE p.conindid=i.indexrelid)`.execute(
      db,
    );
    expect(indexes.rows.map((row) => row.name).sort()).toEqual(table.indexes.map((index) => index.name).sort());
    const fks = await sql<{
      del: string;
      upd: string;
    }>`SELECT confdeltype AS del,confupdtype AS upd FROM pg_constraint WHERE conrelid=${table.name}::regclass AND contype='f'`.execute(
      db,
    );
    expect(fks.rows.length).toBeGreaterThan(0);
    expect(fks.rows.every((fk) => fk.del === 'r' && fk.upd === 'r')).toBe(true);
  }
});

it('rolls final metadata transaction back when status-row lock wait crosses lease expiry', async () => {
  const repository = new PublicationConsumerRepository(db);
  const f = await publicationFixture(db);
  try {
    await f.service.publish(f.input);
    await repository.discover();
    const resolved23 = await repository.list();
    const effect = resolved23.find((row) => row.kind === 'metadata')!;
    const claim = await repository.claim(effect.id, 100);
    const locked = Promise.withResolvers<void>();
    const gate = Promise.withResolvers<void>();
    const blocker = db.transaction().execute(async (tx) => {
      await sql`INSERT INTO asset_job_status ("assetId","metadataExtractedAt") VALUES (${effect.assetId}::uuid,'2000-01-01')`.execute(
        tx,
      );
      locked.resolve();
      await gate.promise;
    });
    await locked.promise;
    const outcome = repository
      .finishMetadata(effect.id, claim.token!)
      .then(() => 'incorrect success')
      .catch((error: Error) => error.message);
    await sql`SELECT pg_sleep(0.2)`.execute(db);
    gate.resolve();
    await blocker;
    expect(await outcome).toBe('Lost publication lease');
    const row = await repository.get(effect.id);
    expect(row).toMatchObject({ state: 'claimed' });
    const effects = await repository.list();
    expect(effects.filter((item) => item.kind === 'metadata-event')).toEqual([]);
  } finally {
    await f.close();
  }
});

it('durably skips missing metadata assets and rejects mismatched trusted job identities before work', async () => {
  const repository = new PublicationConsumerRepository(db);
  const f = await publicationFixture(db);
  const { sut, mocks } = newTestService(MetadataService);
  Object.assign(sut, { publicationConsumers: repository });
  try {
    await f.service.publish(f.input);
    await repository.discover();
    const effects = await repository.list();
    const row = effects.find((item) => item.kind === 'metadata')!;
    const publication = { effectId: row.id, receiptId: row.receiptId, assetId: row.assetId, ownerId: row.ownerId };
    await expect(
      sut.handleMetadataExtraction({ id: row.assetId, publication: { ...publication, ownerId: randomUUID() } }),
    ).rejects.toThrow('Invalid publication metadata identity');
    expect(mocks.assetJob.getForMetadataExtraction).not.toHaveBeenCalled();
    mocks.assetJob.getForMetadataExtraction.mockResolvedValue(undefined);
    expect(await sut.handleMetadataExtraction({ id: row.assetId, publication })).toBe(JobStatus.Skipped);
    expect(await repository.get(row.id)).toMatchObject({
      state: 'done',
      result: { skipped: 'asset missing or deleted' },
    });
    expect(mocks.asset.upsertJobStatus).not.toHaveBeenCalled();
    const after = await repository.list();
    expect(after.filter((item) => item.kind === 'metadata-event')).toEqual([]);
  } finally {
    await sut.onShutdown();
    await f.close();
  }
});

it.each([JobStatus.Failed, undefined])(
  'does not acknowledge metadata callback result %s as successful work',
  async (status) => {
    const repository = new PublicationConsumerRepository(db);
    const f = await publicationFixture(db);
    try {
      await f.service.publish(f.input);
      await repository.discover();
      const effects = await repository.list();
      const row = effects.find((item) => item.kind === 'metadata')!;
      const publication = { effectId: row.id, receiptId: row.receiptId, assetId: row.assetId, ownerId: row.ownerId };
      await expect(repository.consumeMetadata(row.assetId, publication, () => Promise.resolve(status))).rejects.toThrow(
        'Metadata did not report success',
      );
      expect(await repository.get(row.id)).toMatchObject({ state: 'held' });
    } finally {
      await f.close();
    }
  },
);

it('never resumes an expired started workflow or accepts stale completion and retains its UUID', async () => {
  const repository = new PublicationConsumerRepository(db);
  const f = await publicationFixture(db);
  try {
    await f.service.publish(f.input);
    await repository.discover();
    const effects = await repository.list();
    const row = effects.find((item) => item.kind === 'workflow')!;
    const workflowId = randomUUID();
    await sql`INSERT INTO workflow (id,"ownerId",trigger) VALUES (${workflowId}::uuid,${f.ownerId}::uuid,'AssetCreate')`.execute(
      db,
    );
    await repository.freezeFanout(row.id);
    const [run] = await repository.runs(row.id);
    const started = Promise.withResolvers<void>();
    const gate = Promise.withResolvers<void>();
    const work = vi.fn(async () => {
      started.resolve();
      await gate.promise;
      return WorkflowResult.Completed;
    });
    const publication = { effectId: row.id, receiptId: row.receiptId, assetId: row.assetId, ownerId: row.ownerId };
    const first = repository
      .consumeRun(run.id, publication, workflowId, row.assetId, work)
      .then(() => 'incorrect success')
      .catch((error: Error) => error.message);
    await started.promise;
    await sql`UPDATE publication_workflow_run SET "expiresAt"=clock_timestamp()-interval '1 second' WHERE id=${run.id}::uuid`.execute(
      db,
    );
    expect(await repository.consumeRun(run.id, publication, workflowId, row.assetId, work)).toBe(JobStatus.Skipped);
    gate.resolve();
    expect(await first).toBe('Lost publication run lease');
    expect(work).toHaveBeenCalledTimes(1);
    const persisted = await repository.runs(row.id);
    expect(persisted).toHaveLength(1);
    expect(persisted[0]).toMatchObject({ id: run.id, state: 'held' });
  } finally {
    await f.close();
  }
});
