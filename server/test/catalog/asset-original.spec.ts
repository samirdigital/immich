import { schemaFromCode } from '@immich/sql-tools';
import { Kysely, sql } from 'kysely';
import { PostgresJSDialect } from 'kysely-postgres-js';
import { randomUUID } from 'node:crypto';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import postgres from 'postgres';
import { AssetOriginalRepository } from 'src/repositories/asset-original.repository.js';
import { ObjectCatalogRepository } from 'src/repositories/object-catalog.repository.js';
import { DB, ImmichDatabase } from 'src/schema/index.js';
import { up as catalogUp } from 'src/schema/migrations/1793500000000-AddObjectCatalog.js';
import { up as multipartUp } from 'src/schema/migrations/1793500001000-AddS3MultipartJournal.js';
import { down, up } from 'src/schema/migrations/1793500002000-AddAssetOriginal.js';
import { ObjectCatalogService } from 'src/services/object-catalog.service.js';

const socket = process.env.CATALOG_TEST_SOCKET;
if (process.env.CATALOG_TEST_DISPOSABLE !== 'YES' || !socket || !path.isAbsolute(socket)) {
  throw new Error('Requires disposable private PostgreSQL socket');
}
const database = `immich_original_${randomUUID().replaceAll('-', '')}`;
const options = { host: socket, port: Number(process.env.CATALOG_TEST_PORT || 55_441), user: os.userInfo().username };
const admin = postgres({ ...options, database: 'postgres', max: 1 });
let db: Kysely<DB>;
let originals: AssetOriginalRepository;
let catalog: ObjectCatalogRepository;
let ownerId: string;
let assetId: string;
let stageId: string;
let directory: string;
let service: ObjectCatalogService;
beforeAll(async () => {
  await admin.unsafe(`CREATE DATABASE "${database}"`);
  db = new Kysely<DB>({ dialect: new PostgresJSDialect({ postgres: postgres({ ...options, database }) }) });
  await sql`CREATE TABLE "user" (id uuid PRIMARY KEY); CREATE TABLE asset (id uuid PRIMARY KEY, "ownerId" uuid NOT NULL REFERENCES "user"(id))`.execute(
    db,
  );
  await catalogUp(db);
  await multipartUp(db);
  await up(db);
  originals = new AssetOriginalRepository(db);
  catalog = new ObjectCatalogRepository(db);
});
afterAll(async () => {
  await db?.destroy();
  await admin.unsafe(`DROP DATABASE "${database}"`);
  await admin.end();
});
beforeEach(async () => {
  await originals.registerStore({
    storeId: 'immutable-store',
    endpoint: 'https://s3.example.invalid',
    bucket: 'fixture',
  });
  ownerId = randomUUID();
  assetId = randomUUID();
  stageId = randomUUID();
  await sql`INSERT INTO "user" VALUES (${ownerId}::uuid)`.execute(db);
  await sql`INSERT INTO asset (id,"ownerId") VALUES (${assetId}::uuid,${ownerId}::uuid)`.execute(db);
  directory = await mkdtemp(path.join(os.tmpdir(), 'original-pg-'));
  await mkdir(path.join(directory, 'private'), { mode: 0o700 });
  await writeFile(path.join(directory, 'source'), 'abcdef');
  service = new ObjectCatalogService(catalog, path.join(directory, 'private'));
  ({ id: stageId } = await service.stage(path.join(directory, 'source')));
});
afterEach(async () => rm(directory, { recursive: true, force: true }));
const version = async (stage = stageId) => {
  const intent = await catalog.prepare(stage, 's3', 'immutable-store');
  const claim = await catalog.startSending(intent.id, 60_000);
  const result = await catalog.acknowledge(intent.id, claim!.token!, {
    object: { key: intent.key, versionId: randomUUID() },
  });
  await catalog.observeVersion(result.id, { outcome: 'verified', detail: 'synthetic verified receipt' });
  return result;
};
it('defaults to local, records ownership before intent, atomically publishes exact verified identity', async () => {
  expect(await originals.resolve(assetId)).toEqual({ authority: 'local' });
  const claim = await originals.registerClaim({ assetId, ownerId, stageId });
  const remote = await version();
  await originals.bind({ claimId: claim.id, catalogVersionId: remote.id });
  expect(await originals.resolve(assetId)).toMatchObject({
    authority: 'object',
    mapping: {
      key: remote.key,
      versionId: remote.versionId,
      storeId: 'immutable-store',
      size: '6',
      hash: 'bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721',
    },
  });
  await expect(originals.bind({ claimId: claim.id, catalogVersionId: remote.id })).rejects.toThrow();
  await expect(originals.registerClaim({ assetId, ownerId, stageId })).rejects.toThrow();
});
it('rejects first-use ownership after any intent and cross-owner claims', async () => {
  await expect(originals.registerClaim({ assetId, ownerId: randomUUID(), stageId })).rejects.toThrow();
  await version();
  await expect(originals.registerClaim({ assetId, ownerId, stageId })).rejects.toThrow();
});
it('rejects unverified and other-stage versions without changing local authority', async () => {
  const claim = await originals.registerClaim({ assetId, ownerId, stageId });
  const remote = await version();
  await catalog.observeVersion(remote.id, { outcome: 'failed', detail: 'failed' });
  await expect(originals.bind({ claimId: claim.id, catalogVersionId: remote.id })).rejects.toThrow();
  expect(await originals.resolve(assetId)).toEqual({ authority: 'local' });
  const { id: otherStage } = await service.stage(path.join(directory, 'source'));
  const otherVersion = await version(otherStage);
  await expect(originals.bind({ claimId: claim.id, catalogVersionId: otherVersion.id })).rejects.toThrow();
  expect(await originals.resolve(assetId)).toEqual({ authority: 'local' });
});
it('one concurrent publisher wins and identity evidence cannot be erased or changed', async () => {
  const claim = await originals.registerClaim({ assetId, ownerId, stageId });
  const remote = await version();
  const results = await Promise.allSettled([
    originals.bind({ claimId: claim.id, catalogVersionId: remote.id }),
    originals.bind({ claimId: claim.id, catalogVersionId: remote.id }),
  ]);
  expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(1);
  await expect(
    db
      .updateTable('original_store_identity')
      .set({ bucket: 'retargeted' })
      .where('storeId', '=', 'immutable-store')
      .execute(),
  ).rejects.toThrow();
  await expect(
    db.deleteFrom('original_store_identity').where('storeId', '=', 'immutable-store').execute(),
  ).rejects.toThrow();
  await expect(
    db.updateTable('object_version').set({ versionId: 'other-version' }).where('id', '=', remote.id).execute(),
  ).rejects.toThrow();
  await expect(
    db
      .updateTable('staged_object')
      .set({ hash: 'c'.repeat(64) })
      .where('id', '=', stageId)
      .execute(),
  ).rejects.toThrow();
  await expect(db.deleteFrom('object_version').where('id', '=', remote.id).execute()).rejects.toThrow();
  await expect(
    catalog.observeVersion(remote.id, { outcome: 'failed', detail: 'cannot downgrade bound evidence' }),
  ).rejects.toThrow();
  await expect(db.deleteFrom('staged_object').where('id', '=', stageId).execute()).rejects.toThrow();
  await expect(
    db.updateTable('asset').set({ ownerId: randomUUID() }).where('id', '=', assetId).execute(),
  ).rejects.toThrow();
});
it('missing mapping retains explicit object authority', async () => {
  const claim = await originals.registerClaim({ assetId, ownerId, stageId });
  const remote = await version();
  await originals.bind({ claimId: claim.id, catalogVersionId: remote.id });
  await db.deleteFrom('asset_original').where('assetId', '=', assetId).execute();
  expect(await originals.resolve(assetId)).toEqual({ authority: 'object', mapping: undefined });
});
it('durably rejects remapping a stable store identity including concurrent first use', async () => {
  const storeId = randomUUID();
  const input = { storeId, endpoint: 'https://s3.example.invalid', bucket: 'fixture' };
  await Promise.all([originals.registerStore(input), originals.registerStore(input)]);
  await expect(originals.registerStore({ ...input, bucket: 'different' })).rejects.toThrow();
  await expect(originals.registerStore({ ...input, endpoint: 'https://other.invalid' })).rejects.toThrow();
});
it('rolls binding back when the authority update fails inside the publication transaction', async () => {
  const claim = await originals.registerClaim({ assetId, ownerId, stageId });
  const remote = await version();
  await sql`CREATE FUNCTION reject_authority() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic authority failure'; END $$`.execute(
    db,
  );
  await sql`CREATE TRIGGER reject_authority BEFORE UPDATE OF "originalAuthority" ON asset FOR EACH ROW EXECUTE FUNCTION reject_authority()`.execute(
    db,
  );
  try {
    await expect(originals.bind({ claimId: claim.id, catalogVersionId: remote.id })).rejects.toThrow(
      'synthetic authority failure',
    );
    expect(await db.selectFrom('asset_original').selectAll().where('assetId', '=', assetId).execute()).toEqual([]);
    expect(await originals.resolve(assetId)).toEqual({ authority: 'local' });
  } finally {
    await sql`DROP TRIGGER reject_authority ON asset`.execute(db);
    await sql`DROP FUNCTION reject_authority()`.execute(db);
  }
  await originals.bind({ claimId: claim.id, catalogVersionId: remote.id });
});
it('composite database FKs reject cross-owner, cross-stage and unverified links even outside the binder', async () => {
  const claim = await originals.registerClaim({ assetId, ownerId, stageId });
  const remote = await version();
  await originals.bind({ claimId: claim.id, catalogVersionId: remote.id });
  const row = await db
    .selectFrom('asset_original')
    .selectAll()
    .where('assetId', '=', assetId)
    .executeTakeFirstOrThrow();
  await db.deleteFrom('asset_original').where('assetId', '=', assetId).execute();
  const otherAsset = randomUUID();
  await sql`INSERT INTO asset (id,"ownerId") VALUES (${otherAsset}::uuid,${ownerId}::uuid)`.execute(db);
  const { id: otherStage } = await service.stage(path.join(directory, 'source'));
  const otherClaim = await originals.registerClaim({ assetId: otherAsset, ownerId, stageId: otherStage });
  await expect(
    db
      .insertInto('asset_original')
      .values({ ...row, ownerId: randomUUID() })
      .execute(),
  ).rejects.toMatchObject({ code: '23503', constraint_name: 'asset_original_claim_fkey' });
  await expect(
    db
      .insertInto('asset_original')
      .values({ ...row, assetId: otherAsset, claimId: otherClaim.id, stageId: otherStage })
      .execute(),
  ).rejects.toMatchObject({ code: '23503', constraint_name: 'asset_original_intent_fkey' });
  await catalog.observeVersion(remote.id, { outcome: 'failed', detail: 'synthetic downgrade of unbound evidence' });
  await expect(db.insertInto('asset_original').values(row).execute()).rejects.toMatchObject({
    code: '23503',
    constraint_name: 'asset_original_version_fkey',
  });
});
it('snapshots ownership claim inputs before yielding to database work', async () => {
  const { id: otherStage } = await service.stage(path.join(directory, 'source'));
  await catalog.prepare(otherStage, 's3', 'immutable-store');
  const input = { assetId, ownerId, stageId };
  const registration = originals.registerClaim(input);
  input.stageId = otherStage;
  const claim = await registration;
  expect(claim.stageId).toBe(stageId);
});
it('only one concurrent first-use ownership registration succeeds', async () => {
  const results = await Promise.allSettled([
    originals.registerClaim({ assetId, ownerId, stageId }),
    originals.registerClaim({ assetId, ownerId, stageId }),
  ]);
  expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(1);
});
it('decorator tables match actual migration columns, constraint names and FK indexes', async () => {
  expect(new ImmichDatabase().tables.map((table) => table.name)).toContain('AssetOriginalTable');
  const names = new Set(['asset_original', 'asset_original_claim', 'original_store_identity']);
  const tables = schemaFromCode({ databaseName: 'immich' }).tables.filter((table) => names.has(table.name));
  expect(tables).toHaveLength(3);
  for (const table of tables) {
    const columns = await sql<{
      name: string;
      type: string;
      notnull: boolean;
    }>`SELECT attname AS name, format_type(atttypid, atttypmod) AS type, attnotnull AS notnull FROM pg_attribute WHERE attrelid = ${table.name}::regclass AND attnum > 0 AND NOT attisdropped`.execute(
      db,
    );
    expect(columns.rows.toSorted((a, b) => a.name.localeCompare(b.name))).toEqual(
      table.columns
        .map((column) => ({ name: column.name, type: column.type, notnull: !column.nullable }))
        .toSorted((a, b) => a.name.localeCompare(b.name)),
    );
    const constraints = await sql<{
      name: string;
    }>`SELECT conname AS name FROM pg_constraint WHERE conrelid = ${table.name}::regclass AND contype <> 'n'`.execute(
      db,
    );
    expect(constraints.rows.map((row) => row.name).toSorted()).toEqual(
      table.constraints.map((constraint) => constraint.name).toSorted(),
    );
    const indexes = await sql<{
      name: string;
    }>`SELECT c.relname AS name FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE i.indrelid = ${table.name}::regclass AND NOT EXISTS (SELECT 1 FROM pg_constraint p WHERE p.conindid = i.indexrelid)`.execute(
      db,
    );
    expect(indexes.rows.map((row) => row.name).toSorted()).toEqual(table.indexes.map((index) => index.name).toSorted());
  }
});
it('migration rollback removes only this slice', async () => {
  await down(db);
  expect(await catalog.getStage(stageId)).toBeDefined();
  await up(db);
  expect(await originals.resolve(assetId)).toEqual({ authority: 'local' });
});
