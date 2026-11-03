import { schemaFromCode } from '@immich/sql-tools';
import { Kysely, sql } from 'kysely';
import { PostgresJSDialect } from 'kysely-postgres-js';
import { randomUUID } from 'node:crypto';
import { mkdtemp, readdir, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { Readable } from 'node:stream';
import postgres from 'postgres';
import { DB, ImmichDatabase } from 'src/schema/index.js';
import { up as catalogUp } from 'src/schema/migrations/1793500000000-AddObjectCatalog.js';
import { up as originalUp } from 'src/schema/migrations/1793500002000-AddAssetOriginal.js';
import { up } from 'src/schema/migrations/1793500003000-AddUploadSourceReceipt.js';
import { up as publicationUp } from 'src/schema/migrations/1793500004000-AddUploadPublication.js';
import { OwnedSourceService } from 'src/services/owned-source.service.js';

const socket = process.env.CATALOG_TEST_SOCKET;
if (process.env.CATALOG_TEST_DISPOSABLE !== 'YES' || !socket || !path.isAbsolute(socket)) {
  throw new Error('Requires disposable private PostgreSQL socket');
}
const database = `immich_source_${randomUUID().replaceAll('-', '')}`;
const options = { host: socket, port: Number(process.env.CATALOG_TEST_PORT || 55_441), user: os.userInfo().username };
const admin = postgres({ ...options, database: 'postgres', max: 1 });
let db: Kysely<DB>;
let root: string;
let ownerId: string;
beforeAll(async () => {
  await admin.unsafe(`CREATE DATABASE "${database}"`);
  db = new Kysely<DB>({ dialect: new PostgresJSDialect({ postgres: postgres({ ...options, database }) }) });
  await sql`CREATE TABLE "user" (id uuid PRIMARY KEY)`.execute(db);
  await sql`CREATE TABLE asset (id uuid PRIMARY KEY, "ownerId" uuid NOT NULL REFERENCES "user"(id))`.execute(db);
  await catalogUp(db);
  await originalUp(db);
  await up(db);
  await publicationUp(db);
});
afterAll(async () => {
  await db?.destroy();
  await admin.unsafe(`DROP DATABASE "${database}"`);
  await admin.end();
});
beforeEach(async () => {
  root = await mkdtemp(path.join(os.tmpdir(), 'source-pg-'));
  ownerId = randomUUID();
  await sql`INSERT INTO "user" VALUES (${ownerId}::uuid)`.execute(db);
});
afterEach(async () => rm(root, { recursive: true, force: true }));

it('matches migration columns, named constraints and FK indexes to registered decorators', async () => {
  expect(new ImmichDatabase().tables.map((table) => table.name)).toContain('SourceReceiptTable');
  const names = new Set(['source_receipt', 'owned_stage_receipt']);
  const tables = schemaFromCode({ databaseName: 'immich' }).tables.filter((table) => names.has(table.name));
  expect(tables).toHaveLength(2);
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
    const foreignKeys = await sql<{
      ondelete: string;
      onupdate: string;
    }>`SELECT confdeltype AS ondelete, confupdtype AS onupdate FROM pg_constraint WHERE conrelid = ${table.name}::regclass AND contype = 'f'`.execute(
      db,
    );
    expect(foreignKeys.rows.length).toBeGreaterThan(0);
    expect(foreignKeys.rows.every((key) => key.ondelete === 'r' && key.onupdate === 'r')).toBe(true);
  }
});

it('durably owns the source before consuming its first byte with no asset row', async () => {
  const exists = await sql<{ exists: boolean }>`SELECT to_regclass('source_receipt') IS NOT NULL AS exists`.execute(db);
  expect(exists.rows[0].exists).toBe(true);
  const { OwnedSourceRepository } = await import('src/repositories/owned-source.repository.js');
  const repository = new OwnedSourceRepository(db);
  const service = new OwnedSourceService(repository, root);
  const input = Readable.from(
    (async function* () {
      const receipts = await sql<{
        ownerId: string;
        state: string;
      }>`SELECT "ownerId", state FROM source_receipt WHERE root = ${root}`.execute(db);
      expect(receipts.rows).toEqual([{ ownerId, state: 'writing' }]);
      yield Buffer.from('actual source bytes');
    })(),
  );
  const received = await service.receive(ownerId, 'original', '.jpg', input);
  expect(await repository.get(received.id, randomUUID())).toBeUndefined();
  expect(await repository.get(received.id, ownerId)).toMatchObject({ state: 'received', size: '19' });
  const sealed = await service.seal(ownerId, received.id, new Date('2020-01-01T00:00:00Z'));
  expect(sealed.state).toBe('sealed');
  await expect(db.deleteFrom('user').where('id', '=', ownerId).execute()).rejects.toThrow();
  expect(await readdir(root)).toEqual([received.name]);
});

it('reserves owner source and creating stage atomically before copy', async () => {
  const { OwnedSourceRepository } = await import('src/repositories/owned-source.repository.js');
  const { ObjectCatalogRepository } = await import('src/repositories/object-catalog.repository.js');
  const { ObjectCatalogService } = await import('src/services/object-catalog.service.js');
  const { mkdir, readFile } = await import('node:fs/promises');
  const repository = new OwnedSourceRepository(db);
  const sources = new OwnedSourceService(repository, root);
  const received = await sources.receive(ownerId, 'original', '.jpg', Readable.from(['source for stage']));
  await sources.seal(ownerId, received.id, new Date('2020-01-01T00:00:00Z'));
  const stageRoot = path.join(root, 'stage');
  await mkdir(stageRoot, { mode: 0o700 });
  const catalog = new ObjectCatalogRepository(db);
  const service = new ObjectCatalogService(catalog, stageRoot);
  expect(typeof service.stageOwned).toBe('function');
  const reserve = vi.spyOn(catalog, 'reserveOwnedStage');
  const stage = await service.stageOwned(received.id, ownerId);
  const reservation = await reserve.mock.results[0].value;
  expect(reservation.created).toBe(true);
  expect(reservation.stage.state).toBe('creating');
  expect(await readFile(path.join(stageRoot, stage.id), 'utf8')).toBe('source for stage');
  const association = await sql<{
    ownerId: string;
    sourceId: string;
    stageId: string;
  }>`SELECT "ownerId", "sourceId", "stageId" FROM owned_stage_receipt WHERE "stageId" = ${stage.id}::uuid`.execute(db);
  expect(association.rows).toEqual([{ ownerId, sourceId: received.id, stageId: stage.id }]);
  const replayed = await service.stageOwned(received.id, ownerId);
  expect(replayed.id).toBe(stage.id);
  expect(await catalog.listStages(stageRoot)).toHaveLength(1);
  await expect(service.stageOwned(received.id, randomUUID())).rejects.toThrow();
  await expect(catalog.prepare(stage.id, 's3', 'unowned-store')).rejects.toThrow(
    'Receipt-owned stage requires an owned intent',
  );
  await catalog.releaseStage(stage.id);
  expect(await catalog.claimCleanup(stage.id)).toBeUndefined();
});

it('rejects legacy claim adoption of a receipt-owned stage even for an existing other-owner asset', async () => {
  const { OwnedSourceRepository } = await import('src/repositories/owned-source.repository.js');
  const { ObjectCatalogRepository } = await import('src/repositories/object-catalog.repository.js');
  const { ObjectCatalogService } = await import('src/services/object-catalog.service.js');
  const { AssetOriginalRepository } = await import('src/repositories/asset-original.repository.js');
  const { mkdir } = await import('node:fs/promises');
  const sources = new OwnedSourceService(new OwnedSourceRepository(db), root);
  const source = await sources.receive(ownerId, 'original', '.jpg', Readable.from(['owner bytes']));
  await sources.seal(ownerId, source.id, new Date());
  const stageRoot = path.join(root, 'stage');
  await mkdir(stageRoot, { mode: 0o700 });
  const stage = await new ObjectCatalogService(new ObjectCatalogRepository(db), stageRoot).stageOwned(
    source.id,
    ownerId,
  );
  const otherOwnerId = randomUUID();
  const assetId = randomUUID();
  await sql`INSERT INTO "user" VALUES (${otherOwnerId}::uuid)`.execute(db);
  await sql`INSERT INTO asset (id, "ownerId") VALUES (${assetId}::uuid, ${otherOwnerId}::uuid)`.execute(db);
  await expect(
    new AssetOriginalRepository(db).registerClaim({ assetId, ownerId: otherOwnerId, stageId: stage.id }),
  ).rejects.toThrow();
});

it('rejects a forged sealed inode identity at the database boundary', async () => {
  const { OwnedSourceRepository } = await import('src/repositories/owned-source.repository.js');
  const repository = new OwnedSourceRepository(db);
  const source = await new OwnedSourceService(repository, root).receive(
    ownerId,
    'sidecar',
    '.xmp',
    Readable.from(['sidecar']),
  );
  await expect(
    db
      .updateTable('source_receipt')
      .set({ identity: { ...source.identity!, ino: 'not-an-inode' } })
      .where('id', '=', source.id)
      .execute(),
  ).rejects.toThrow();
});

it('retains attributable held bytes on stream failure without unlinking', async () => {
  const { OwnedSourceRepository } = await import('src/repositories/owned-source.repository.js');
  const repository = new OwnedSourceRepository(db);
  const input = Readable.from(
    (function* () {
      yield Buffer.from('partial bytes');
      throw new Error('synthetic stream failure');
    })(),
  );
  await expect(new OwnedSourceService(repository, root).receive(ownerId, 'original', '.jpg', input)).rejects.toThrow(
    'synthetic stream failure',
  );
  const rows = await db.selectFrom('source_receipt').selectAll().where('ownerId', '=', ownerId).execute();
  expect(rows).toHaveLength(1);
  expect(rows[0].state).toBe('held');
  expect(await readdir(root)).toEqual([rows[0].name]);
});

it('holds changed source identity without unlinking replacement bytes', async () => {
  const { OwnedSourceRepository } = await import('src/repositories/owned-source.repository.js');
  const { rename, writeFile, readFile } = await import('node:fs/promises');
  const repository = new OwnedSourceRepository(db);
  const service = new OwnedSourceService(repository, root);
  const source = await service.receive(ownerId, 'original', '.jpg', Readable.from(['received bytes']));
  await rename(path.join(root, source.name), path.join(root, 'original-held'));
  await writeFile(path.join(root, source.name), 'replacement bytes');
  await expect(service.seal(ownerId, source.id, new Date())).rejects.toThrow('Received source identity changed');
  const held = await repository.get(source.id, ownerId);
  expect(held?.state).toBe('held');
  expect(await readFile(path.join(root, source.name), 'utf8')).toBe('replacement bytes');
  expect(await readFile(path.join(root, 'original-held'), 'utf8')).toBe('received bytes');
});

it('keeps the creating stage associated and held across the reserve-before-copy crash boundary', async () => {
  const { OwnedSourceRepository } = await import('src/repositories/owned-source.repository.js');
  const { ObjectCatalogRepository } = await import('src/repositories/object-catalog.repository.js');
  const { ObjectCatalogService } = await import('src/services/object-catalog.service.js');
  const { mkdir } = await import('node:fs/promises');
  const repository = new OwnedSourceRepository(db);
  const sources = new OwnedSourceService(repository, root);
  const source = await sources.receive(ownerId, 'original', '.jpg', Readable.from(['owned bytes']));
  await sources.seal(ownerId, source.id, new Date());
  const stageRoot = path.join(root, 'stage');
  await mkdir(stageRoot, { mode: 0o700 });
  const catalog = new ObjectCatalogRepository(db);
  const reservation = await catalog.reserveOwnedStage(source.id, ownerId, stageRoot);
  expect(reservation.created).toBe(true);
  expect(await readdir(stageRoot)).toEqual([]);
  const replayed = await catalog.reserveOwnedStage(source.id, ownerId, stageRoot);
  expect(replayed.created).toBe(false);
  await expect(new ObjectCatalogService(catalog, stageRoot).stageOwned(source.id, ownerId)).rejects.toThrow(
    'Owned stage is held',
  );
  await expect(db.deleteFrom('staged_object').where('id', '=', reservation.stage.id).execute()).rejects.toThrow();
  await expect(db.deleteFrom('source_receipt').where('id', '=', source.id).execute()).rejects.toThrow();
  await expect(
    db
      .updateTable('owned_stage_receipt')
      .set({ ownerId: randomUUID() })
      .where('stageId', '=', reservation.stage.id)
      .execute(),
  ).rejects.toThrow();
  await expect(
    db.updateTable('source_receipt').set({ state: 'held' }).where('id', '=', source.id).execute(),
  ).rejects.toThrow();
});
