import { schemaFromCode } from '@immich/sql-tools';
import { Kysely, sql } from 'kysely';
import { PostgresJSDialect } from 'kysely-postgres-js';
import { randomUUID } from 'node:crypto';
import os from 'node:os';
import path from 'node:path';
import postgres from 'postgres';
import { UploadPublicationRepository } from 'src/repositories/upload-publication.repository.js';
import { DB, ImmichDatabase } from 'src/schema/index.js';
import { up as catalogUp } from 'src/schema/migrations/1793500000000-AddObjectCatalog.js';
import { up as multipartUp } from 'src/schema/migrations/1793500001000-AddS3MultipartJournal.js';
import { up as originalUp } from 'src/schema/migrations/1793500002000-AddAssetOriginal.js';
import { up as sourceUp } from 'src/schema/migrations/1793500003000-AddUploadSourceReceipt.js';
import {
  down as publicationDown,
  up as publicationUp,
} from 'src/schema/migrations/1793500004000-AddUploadPublication.js';
import { publicationFixture } from 'test/catalog/upload-publication-fixture.js';

const socket = process.env.CATALOG_TEST_SOCKET;
if (process.env.CATALOG_TEST_DISPOSABLE !== 'YES' || !socket || !path.isAbsolute(socket)) {
  throw new Error('Requires disposable private PostgreSQL socket');
}
const database = `immich_publication_${randomUUID().replaceAll('-', '')}`;
const options = { host: socket, port: Number(process.env.CATALOG_TEST_PORT || 55_441), user: os.userInfo().username };
const admin = postgres({ ...options, database: 'postgres', max: 1 });
let db: Kysely<DB>;
beforeAll(async () => {
  await admin.unsafe(`CREATE DATABASE "${database}"`);
  db = new Kysely<DB>({ dialect: new PostgresJSDialect({ postgres: postgres({ ...options, database }) }) });
  await sql`CREATE TABLE "user" (id uuid PRIMARY KEY, "deletedAt" timestamptz, status varchar NOT NULL DEFAULT 'active', "quotaSizeInBytes" bigint, "quotaUsageInBytes" bigint NOT NULL DEFAULT 0);
    CREATE TABLE asset (id uuid PRIMARY KEY, "ownerId" uuid NOT NULL REFERENCES "user"(id), "originalPath" varchar NOT NULL, checksum bytea NOT NULL, "checksumAlgorithm" varchar NOT NULL DEFAULT 'sha1', "libraryId" uuid, type varchar NOT NULL, "fileCreatedAt" timestamptz NOT NULL, "fileModifiedAt" timestamptz NOT NULL, "localDateTime" timestamptz NOT NULL, "originalFileName" varchar NOT NULL, "isFavorite" boolean NOT NULL DEFAULT false, visibility varchar NOT NULL DEFAULT 'timeline', duration integer, "livePhotoVideoId" uuid REFERENCES asset(id), "deletedAt" timestamptz);
    CREATE UNIQUE INDEX "UQ_assets_owner_checksum" ON asset ("ownerId", checksum) WHERE "libraryId" IS NULL;
    CREATE TABLE asset_exif ("assetId" uuid PRIMARY KEY REFERENCES asset(id), "fileSizeInByte" bigint NOT NULL);
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
  await catalogUp(db);
  await multipartUp(db);
  await originalUp(db);
  await sourceUp(db);
  await publicationUp(db);
  await publicationDown(db);
  await publicationUp(db);
});
afterAll(async () => {
  await db?.destroy();
  await admin.unsafe(`DROP DATABASE "${database}"`);
  await admin.end();
});
it('has durable publication and pending effect ledgers before any asset is allocated', async () => {
  const result = await sql<{
    exists: boolean;
  }>`SELECT to_regclass('upload_publication') IS NOT NULL AND to_regclass('upload_publication_outbox') IS NOT NULL AS exists`.execute(
    db,
  );
  expect(result.rows[0].exists).toBe(true);
});

it('publishes only after actual SDK full GET and atomically records one quota charge and pending effects', async () => {
  const f = await publicationFixture(db);
  f.input.options.isFavorite = true;
  f.input.options.metadata = [{ key: 'test', value: { x: 1 } }];
  f.controls.beforeGet = async () => {
    expect(await db.selectFrom('asset').select('id').where('ownerId', '=', f.ownerId).execute()).toEqual([]);
    expect(new URL(f.requests.at(-1)!.url, f.binding.endpoint).searchParams.get('versionId')).toBe('exact-version');
  };
  try {
    const concurrent = await Promise.allSettled([f.service.publish(f.input), f.service.publish(f.input)]);
    expect(concurrent.some((value) => value.status === 'fulfilled')).toBe(true);
    const result = await f.service.publish(f.input);
    expect(result.status).toBe('created');
    expect(await f.service.publish(f.input)).toEqual(result);
    expect(f.requests.map((request) => request.method)).toEqual(['PUT', 'GET']);
    const asset = await db.selectFrom('asset').selectAll().where('id', '=', result.id).executeTakeFirstOrThrow();
    expect(asset).toMatchObject({
      originalAuthority: 'object',
      originalPath: path.join(f.root, f.source.name),
      isFavorite: true,
    });
    const { readFile } = await import('node:fs/promises');
    expect(await readFile(asset.originalPath, 'utf8')).toBe('publication bytes');
    expect(await db.selectFrom('asset_original').selectAll().where('assetId', '=', asset.id).execute()).toHaveLength(1);
    expect(
      await db
        .selectFrom('asset_exif')
        .select('fileSizeInByte')
        .where('assetId', '=', asset.id)
        .executeTakeFirstOrThrow(),
    ).toMatchObject({ fileSizeInByte: '17' });
    expect(await db.selectFrom('asset_metadata').selectAll().where('assetId', '=', asset.id).execute()).toHaveLength(1);
    expect(await f.usage()).toBe('17');
    const receipt = await f.receipt();
    expect(receipt).toMatchObject({ state: 'published', reservedAssetId: asset.id, result });
    expect(receipt.exactAttempt).not.toBeNull();
    const effects = await db
      .selectFrom('upload_publication_outbox')
      .selectAll()
      .where('receiptId', '=', receipt.id)
      .execute();
    expect(effects.map((effect) => effect.kind).sort()).toEqual(['event', 'metadata', 'workflow']);
    expect(effects.map((effect) => effect.target).sort()).toEqual([
      'AssetCreate',
      'AssetCreate',
      'AssetExtractMetadata',
    ]);
    expect(
      effects.every(
        (effect) => effect.state === 'pending' && effect.payload.quotaAlreadyCharged && !('file' in effect.payload),
      ),
    ).toBe(true);
    await expect(
      db
        .updateTable('upload_publication')
        .set({ catalogVersionId: randomUUID() })
        .where('id', '=', receipt.id)
        .execute(),
    ).rejects.toThrow();
  } finally {
    await f.close();
  }
});

it('rejects DTO timestamps not applied to the sealed original before any provider call', async () => {
  const f = await publicationFixture(db);
  try {
    f.input.options.fileModifiedAt = '2021-01-01T00:00:00Z';
    await expect(f.service.publish(f.input)).rejects.toThrow();
    expect(f.requests).toEqual([]);
  } finally {
    await f.close();
  }
});

it('pins sealed sidecar evidence restrictively even while failed verification holds publication', async () => {
  const f = await publicationFixture(db);
  try {
    const sidecar = await f.makeSource('sidecar bytes', 'sidecar');
    f.input.sidecarId = sidecar.id;
    f.controls.corruptGet = true;
    await expect(f.service.publish(f.input)).rejects.toThrow('held');
    expect(await db.selectFrom('asset').select('id').where('ownerId', '=', f.ownerId).execute()).toEqual([]);
    await expect(
      db.updateTable('source_receipt').set({ state: 'held' }).where('id', '=', sidecar.id).execute(),
    ).rejects.toThrow();
  } finally {
    await f.close();
  }
});

it('joins held acknowledged verification without another PUT or another intent', async () => {
  const f = await publicationFixture(db);
  try {
    f.controls.corruptGet = true;
    await expect(f.service.publish(f.input)).rejects.toThrow('held');
    const held = await f.receipt();
    expect(held.state).toBe('held');
    expect(await f.catalog.listIntents(held.stageId!)).toHaveLength(1);
    expect(await f.catalog.getIntent(held.intentId!)).toMatchObject({ state: 'acknowledged' });
    expect(await f.usage()).toBe('0');
    f.controls.corruptGet = false;
    expect(await f.service.publish(f.input)).toMatchObject({ status: 'created' });
    expect(f.requests.map((request) => request.method)).toEqual(['PUT', 'GET', 'GET']);
    expect(await f.catalog.listIntents(held.stageId!)).toHaveLength(1);
  } finally {
    await f.close();
  }
});

it('uses a versioned full owner fingerprint and rejects explicit-key conflicts and owner forgeries', async () => {
  const f = await publicationFixture(db);
  try {
    f.input.idempotencyKey = 'stable-client-key';
    const published = await f.service.publish(f.input);
    for (const changes of [
      { filename: 'changed.jpg' },
      { isFavorite: true },
      { visibility: 'hidden' as const },
      { duration: 1 },
      { fileCreatedAt: '2021-01-01T00:00:00Z' },
      { livePhotoVideoId: randomUUID() },
      { metadata: [{ key: 'test', value: { a: 1 } }] },
    ]) {
      await expect(
        f.service.publish({ ...f.input, options: { ...f.input.options, ...changes } }),
      ).rejects.toMatchObject({ status: 409 });
    }
    const sidecar = await f.makeSource('sidecar', 'sidecar');
    await expect(f.service.publish({ ...f.input, sidecarId: sidecar.id })).rejects.toMatchObject({ status: 409 });
    const otherSource = await f.makeSource('different bytes');
    await expect(f.service.publish({ ...f.input, sourceId: otherSource.id })).rejects.toMatchObject({ status: 409 });
    const otherOwner = randomUUID();
    await sql`INSERT INTO "user" (id) VALUES (${otherOwner}::uuid)`.execute(db);
    await expect(f.service.publish({ ...f.input, ownerId: otherOwner })).rejects.toThrow();
    const persisted = await f.receipt();
    await expect(f.publications.get(persisted.id, otherOwner)).rejects.toThrow();
    await expect(f.catalog.prepareOwned(persisted.id, otherOwner, f.binding.storeId)).rejects.toThrow();
    await expect(f.service.publish({ ...f.input, auth: { kind: 'session', id: randomUUID() } })).rejects.toMatchObject({
      status: 409,
    });
    expect(
      await f.service.publish({
        ...f.input,
        options: { ...f.input.options, fileCreatedAt: '2019-12-31T18:00:00-06:00' },
      }),
    ).toEqual(published);
    expect(persisted.fingerprint).toMatch(/^v1:[a-f0-9]{64}$/);
    expect(f.requests.map((request) => request.method)).toEqual(['PUT', 'GET']);
    for (const key of ['', 'x'.repeat(201), 'space key']) {
      await expect(f.service.publish({ ...f.input, idempotencyKey: key })).rejects.toMatchObject({ status: 400 });
    }
  } finally {
    await f.close();
  }
});

it('holds an incomplete source-stage crash reservation and never recopies or allocates another stage', async () => {
  const f = await publicationFixture(db);
  try {
    const receipt = await f.publications.reserve(f.input, f.binding);
    expect(await db.selectFrom('asset').select('id').where('id', '=', receipt.reservedAssetId).execute()).toEqual([]);
    const stage = await f.catalog.reserveOwnedStage(f.source.id, f.ownerId, f.stageRoot, receipt.id);
    expect(await f.receipt()).toMatchObject({ stageId: stage.stage.id });
    await expect(f.service.publish(f.input)).rejects.toThrow('held');
    await expect(f.service.publish(f.input)).rejects.toThrow('held');
    expect(await f.catalog.listStages(f.stageRoot)).toHaveLength(1);
    expect(await f.catalog.listIntents(stage.stage.id)).toEqual([]);
    expect(f.requests).toEqual([]);
    const { readdir } = await import('node:fs/promises');
    expect(await readdir(f.stageRoot)).toEqual([]);
    await expect(f.catalog.reserveOwnedStage(f.source.id, randomUUID(), f.stageRoot, receipt.id)).rejects.toThrow();
  } finally {
    await f.close();
  }
});

it('commits exactly one owned prepared intent before SDK send and rejects generic or forged permits', async () => {
  const f = await publicationFixture(db);
  try {
    const receipt = await f.publications.reserve(f.input, f.binding);
    const stage = await f.stages.stageOwned(f.source.id, f.ownerId, receipt.id);
    const [first, second] = await Promise.all([
      f.catalog.prepareOwned(receipt.id, f.ownerId, f.binding.storeId),
      f.catalog.prepareOwned(receipt.id, f.ownerId, f.binding.storeId),
    ]);
    expect(first.id).toBe(second.id);
    expect(await f.receipt()).toMatchObject({ intentId: first.id });
    await expect(f.catalog.prepare(stage.id, 's3', f.binding.storeId)).rejects.toThrow('owned intent');
    await expect(f.stages.sendPrepared(first.id, f.binding)).rejects.toThrow('permit');
    await expect(f.stages.sendPrepared(first.id, f.binding, 60_000, randomUUID())).rejects.toThrow();
    await expect(f.catalog.prepareOwned(receipt.id, f.ownerId, 'forged-store')).rejects.toThrow();
    const result = await f.service.publish(f.input);
    expect(result.status).toBe('created');
    expect(await f.catalog.listIntents(stage.id)).toHaveLength(1);
    expect(f.requests.map((request) => request.method)).toEqual(['PUT', 'GET']);
  } finally {
    await f.close();
  }
});

it('never retries an unknown SDK PUT outcome or expires sending into retry permission', async () => {
  const f = await publicationFixture(db);
  try {
    f.controls.unknownPut = true;
    await expect(f.service.publish(f.input)).rejects.toThrow('held');
    const receipt = await f.receipt();
    expect(await f.catalog.getIntent(receipt.intentId!)).toMatchObject({ state: 'ambiguous' });
    await f.catalog.expireSending(receipt.stageId!);
    f.controls.unknownPut = false;
    await expect(f.service.publish(f.input)).rejects.toThrow('held');
    expect(f.requests.map((request) => request.method)).toEqual(['PUT']);
    expect(await f.catalog.listIntents(receipt.stageId!)).toHaveLength(1);
    expect(await f.catalog.listVersions(receipt.stageId!)).toEqual([]);
    expect(await f.usage()).toBe('0');
  } finally {
    await f.close();
  }
});

it('rechecks authoritative quota after GET and resumes the acknowledged version without PUT', async () => {
  const f = await publicationFixture(db);
  try {
    f.controls.beforeGet = async () => {
      await sql`UPDATE "user" SET "quotaSizeInBytes"=16 WHERE id=${f.ownerId}::uuid`.execute(db);
    };
    await expect(f.service.publish(f.input)).rejects.toThrow('held');
    expect(await f.usage()).toBe('0');
    expect(await db.selectFrom('asset').select('id').where('ownerId', '=', f.ownerId).execute()).toEqual([]);
    f.controls.beforeGet = undefined;
    await sql`UPDATE "user" SET "quotaSizeInBytes"=17 WHERE id=${f.ownerId}::uuid`.execute(db);
    expect(await f.service.publish(f.input)).toMatchObject({ status: 'created' });
    expect(await f.usage()).toBe('17');
    expect(f.requests.map((request) => request.method)).toEqual(['PUT', 'GET', 'GET']);
  } finally {
    await f.close();
  }
});

it('replays persisted success after postcommit coordinator failure without deleting asset or evidence', async () => {
  const f = await publicationFixture(db);
  try {
    const actual = UploadPublicationRepository.prototype.complete;
    vi.spyOn(UploadPublicationRepository.prototype, 'complete').mockImplementation(async function (
      this: UploadPublicationRepository,
      ...args
    ) {
      await actual.apply(this, args);
      throw new Error('postcommit delivery failed');
    });
    const response = await f.service.publish(f.input);
    expect(await f.service.publish(f.input)).toEqual(response);
    expect(await f.receipt()).toMatchObject({ result: response });
    expect(await f.usage()).toBe('17');
    expect(
      await db.selectFrom('asset_original').select('assetId').where('assetId', '=', response.id).execute(),
    ).toHaveLength(1);
    const { readFile, readdir } = await import('node:fs/promises');
    expect(await readFile(path.join(f.root, f.source.name), 'utf8')).toBe('publication bytes');
    const persisted = await f.receipt();
    expect(await readdir(f.stageRoot)).toEqual([persisted.stageId]);
    expect(f.requests.map((request) => request.method)).toEqual(['PUT', 'GET']);
  } finally {
    vi.restoreAllMocks();
    await f.close();
  }
});

it('persists the receipt exact attempt token atomically before the SDK dispatch', async () => {
  const f = await publicationFixture(db);
  let observed: string | null = null;
  let token: string | null = null;
  try {
    f.controls.beforePut = async () => {
      const receipt = await f.receipt();
      const intent = await f.catalog.getIntent(receipt.intentId!);
      observed = receipt.exactAttempt;
      token = intent.token;
      expect(intent.state).toBe('sending');
    };
    expect(await f.service.publish(f.input)).toMatchObject({ status: 'created' });
    expect(observed).toBe(token);
    expect(observed).not.toBeNull();
  } finally {
    await f.close();
  }
});

it('returns held instead of exhausting SQL connections waiting for another coordinator', async () => {
  const f = await publicationFixture(db);
  const { promise: started, resolve: entered } = Promise.withResolvers<void>();
  const { promise: gate, resolve: release } = Promise.withResolvers<void>();
  const id = randomUUID();
  const first = f.publications.withWorker(id, async () => {
    entered();
    await gate;
  });
  await started;
  const second = f.publications
    .withWorker(id, () => Promise.resolve('incorrectly ran'))
    .then(() => 'ran')
    .catch(() => 'held');
  const result = await Promise.race([
    second,
    new Promise<string>((resolve) => setTimeout(() => resolve('blocked'), 100)),
  ]);
  release();
  await first;
  await second;
  await f.close();
  expect(result).toBe('held');
});

it('matches publication decorators, columns, named constraints, FK indexes and restrictive actions', async () => {
  expect(new ImmichDatabase().tables.map((table) => table.name)).toContain('UploadPublicationTable');
  const tables = schemaFromCode({ databaseName: 'immich' }).tables.filter((table) =>
    ['upload_publication', 'upload_publication_outbox'].includes(table.name),
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

it('returns the durable terminal response even if releasing worker serialization fails after commit', async () => {
  const f = await publicationFixture(db);
  try {
    const actual = f.publications.withWorker.bind(f.publications);
    vi.spyOn(f.publications, 'withWorker').mockImplementation(async (...args) => {
      await actual(...args);
      throw new Error('worker release failed after commit');
    });
    const result = await f.service.publish(f.input);
    expect(await f.receipt()).toMatchObject({ result });
    expect(result.status).toBe('created');
    expect(await f.usage()).toBe('17');
    expect(f.requests.map((request) => request.method)).toEqual(['PUT', 'GET']);
  } finally {
    await f.close();
  }
});

it('holds unknown multipart Complete with the durable upload ID and manifest without a second provider attempt', async () => {
  const f = await publicationFixture(db, true);
  try {
    f.controls.unknownComplete = true;
    await expect(f.service.publish(f.input)).rejects.toThrow('held');
    const receipt = await f.receipt();
    const journal = await f.catalog.listMultipart(receipt.stageId!);
    expect(journal).toHaveLength(1);
    expect(journal[0]).toMatchObject({ uploadId: 'owned-upload', state: 'completing' });
    expect(journal[0].manifest).toHaveLength(1);
    expect(journal[0].evidence).toEqual(
      expect.arrayContaining([expect.objectContaining({ completeDispatched: true })]),
    );
    const before = [...f.requests];
    f.controls.unknownComplete = false;
    await expect(f.service.publish(f.input)).rejects.toThrow('held');
    expect(f.requests).toEqual(before);
    expect(before.map((request) => request.method)).toEqual(['POST', 'PUT', 'POST']);
    expect(await f.catalog.listIntents(receipt.stageId!)).toHaveLength(1);
    expect(await db.selectFrom('asset').select('id').where('ownerId', '=', f.ownerId).execute()).toEqual([]);
    expect(await f.usage()).toBe('0');
  } finally {
    await f.close();
  }
});

const localAsset = async (ownerId: string, checksum: string, type = 'VIDEO') => {
  const id = randomUUID();
  await sql`INSERT INTO asset (id,"ownerId","originalPath",checksum,type,"fileCreatedAt","fileModifiedAt","localDateTime","originalFileName") VALUES (${id}::uuid,${ownerId}::uuid,${`/retained/${id}`},decode(${checksum},'hex'),${type},now(),now(),now(),'existing')`.execute(
    db,
  );
  return id;
};

it('atomically rolls back EXIF metadata sidecar membership live-photo hide quota binding and outbox on final-write failure', async () => {
  const f = await publicationFixture(db);
  const sidecar = await f.makeSource('sidecar bytes', 'sidecar');
  const video = await localAsset(f.ownerId, 'a'.repeat(40));
  const album = randomUUID();
  const link = randomUUID();
  await sql`INSERT INTO album(id) VALUES (${album}::uuid)`.execute(db);
  await sql`INSERT INTO album_user ("albumId","userId",role) VALUES (${album}::uuid,${f.ownerId}::uuid,'owner')`.execute(
    db,
  );
  await sql`INSERT INTO shared_link (id,"userId",type,"albumId","allowUpload") VALUES (${link}::uuid,${f.ownerId}::uuid,'ALBUM',${album}::uuid,true)`.execute(
    db,
  );
  f.input.auth = { kind: 'sharedLink', id: link, type: 'ALBUM', albumId: album };
  f.input.sidecarId = sidecar.id;
  f.input.options.livePhotoVideoId = video;
  f.input.options.metadata = [{ key: 'test', value: { a: 1 } }];
  await sql`CREATE FUNCTION reject_publication_effect() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic final publication write failure'; END $$; CREATE TRIGGER reject_publication_effect BEFORE INSERT ON upload_publication_outbox FOR EACH ROW EXECUTE FUNCTION reject_publication_effect()`.execute(
    db,
  );
  try {
    await expect(f.service.publish(f.input)).rejects.toThrow('held');
    const receipt = await f.receipt();
    expect(receipt.result).toBeNull();
    expect(await f.usage()).toBe('0');
    expect(
      await db.selectFrom('asset').select('visibility').where('id', '=', video).executeTakeFirstOrThrow(),
    ).toMatchObject({ visibility: 'timeline' });
    expect(await db.selectFrom('asset').select('id').where('id', '=', receipt.reservedAssetId).execute()).toEqual([]);
    for (const table of [
      'asset_exif',
      'asset_metadata',
      'asset_file',
      'album_asset',
      'asset_original',
      'asset_original_claim',
    ] as const) {
      expect(
        await db.selectFrom(table).select('assetId').where('assetId', '=', receipt.reservedAssetId).execute(),
      ).toEqual([]);
    }
    expect(
      await db.selectFrom('upload_publication_outbox').select('id').where('receiptId', '=', receipt.id).execute(),
    ).toEqual([]);
    await sql`DROP TRIGGER reject_publication_effect ON upload_publication_outbox; DROP FUNCTION reject_publication_effect()`.execute(
      db,
    );
    const result = await f.service.publish(f.input);
    expect(result.status).toBe('created');
    expect(await f.usage()).toBe('17');
    expect(
      await db.selectFrom('asset').select('visibility').where('id', '=', video).executeTakeFirstOrThrow(),
    ).toMatchObject({ visibility: 'hidden' });
    for (const table of [
      'asset_exif',
      'asset_metadata',
      'asset_file',
      'album_asset',
      'asset_original',
      'asset_original_claim',
    ] as const) {
      expect(await db.selectFrom(table).select('assetId').where('assetId', '=', result.id).execute()).toHaveLength(1);
    }
    expect(
      await db.selectFrom('asset_file').select('path').where('assetId', '=', result.id).executeTakeFirstOrThrow(),
    ).toMatchObject({ path: path.join(f.root, sidecar.name) });
    expect(f.requests.map((request) => request.method)).toEqual(['PUT', 'GET', 'GET']);
  } finally {
    await sql`DROP TRIGGER IF EXISTS reject_publication_effect ON upload_publication_outbox; DROP FUNCTION IF EXISTS reject_publication_effect()`.execute(
      db,
    );
    await f.close();
  }
});

it('preserves an existing local duplicate winner without overriding authority or quota', async () => {
  const f = await publicationFixture(db);
  try {
    const winner = await localAsset(f.ownerId, f.source.sha1!, 'IMAGE');
    const before = await db.selectFrom('asset').selectAll().where('id', '=', winner).executeTakeFirstOrThrow();
    const result = await f.service.publish(f.input);
    expect(result).toEqual({ id: winner, status: 'duplicate' });
    expect(await f.service.publish(f.input)).toEqual(result);
    expect(await db.selectFrom('asset').selectAll().where('id', '=', winner).executeTakeFirstOrThrow()).toEqual(before);
    expect(await db.selectFrom('asset_original').select('assetId').where('ownerId', '=', f.ownerId).execute()).toEqual(
      [],
    );
    expect(await f.usage()).toBe('0');
    const receipt = await f.receipt();
    expect(receipt.state).toBe('duplicate');
    expect(receipt.publishedAssetId).toBeNull();
    expect(await f.catalog.claimCleanup(receipt.stageId!)).toBeUndefined();
    expect(
      await db.selectFrom('upload_publication_outbox').select('id').where('receiptId', '=', receipt.id).execute(),
    ).toEqual([]);
    expect(f.requests.map((request) => request.method)).toEqual(['PUT', 'GET']);
  } finally {
    await f.close();
  }
});

it('preserves the first object duplicate exact version and holds the losing retained stage', async () => {
  const f = await publicationFixture(db);
  try {
    const winner = await f.service.publish(f.input);
    const before = await db
      .selectFrom('asset_original')
      .selectAll()
      .where('assetId', '=', winner.id)
      .executeTakeFirstOrThrow();
    const source = await f.makeSource();
    const result = await f.service.publish({
      ...f.input,
      sourceId: source.id,
      options: { ...f.input.options, filename: 'new-name.jpg' },
    });
    expect(result).toEqual({ id: winner.id, status: 'duplicate' });
    expect(
      await db.selectFrom('asset_original').selectAll().where('assetId', '=', winner.id).executeTakeFirstOrThrow(),
    ).toEqual(before);
    expect(await db.selectFrom('asset').select('id').where('ownerId', '=', f.ownerId).execute()).toHaveLength(1);
    expect(await f.usage()).toBe('17');
    const loser = await db
      .selectFrom('upload_publication')
      .selectAll()
      .where('sourceId', '=', source.id)
      .executeTakeFirstOrThrow();
    expect(loser.state).toBe('duplicate');
    expect(loser.reservedAssetId).not.toBe(winner.id);
    expect(await f.catalog.claimCleanup(loser.stageId!)).toBeUndefined();
    expect(f.requests.map((request) => request.method)).toEqual(['PUT', 'GET', 'PUT', 'GET']);
  } finally {
    await f.close();
  }
});

it('serializes final quota contention across distinct concurrently verified originals', async () => {
  const f = await publicationFixture(db);
  try {
    const other = await f.makeSource('PUBLICATION BYTES');
    await sql`UPDATE "user" SET "quotaSizeInBytes"=17 WHERE id=${f.ownerId}::uuid`.execute(db);
    const responses = await Promise.allSettled([
      f.service.publish(f.input),
      f.service.publish({ ...f.input, sourceId: other.id }),
    ]);
    expect(responses.filter((response) => response.status === 'fulfilled')).toHaveLength(1);
    expect(responses.filter((response) => response.status === 'rejected')).toHaveLength(1);
    expect(await f.usage()).toBe('17');
    expect(await db.selectFrom('asset').select('id').where('ownerId', '=', f.ownerId).execute()).toHaveLength(1);
    const receipts = await db.selectFrom('upload_publication').selectAll().where('ownerId', '=', f.ownerId).execute();
    expect(receipts.map((receipt) => receipt.state).sort()).toEqual(['held', 'published']);
    const published = receipts.find((receipt) => receipt.state === 'published')!;
    expect(
      await db.selectFrom('upload_publication_outbox').select('id').where('receiptId', '=', published.id).execute(),
    ).toHaveLength(3);
  } finally {
    await f.close();
  }
});

it.each(['session', 'apiKey', 'owner', 'linkGrant', 'linkExpiry', 'linkType', 'linkOwner', 'albumGrant', 'livePhoto'])(
  'rechecks revoked %s dependency after actual GET without partial publication',
  async (dependency) => {
    const f = await publicationFixture(db);
    try {
      let revoke: () => Promise<unknown>;
      if (dependency === 'apiKey') {
        const id = randomUUID();
        await sql`INSERT INTO api_key(id,"userId",permissions) VALUES (${id}::uuid,${f.ownerId}::uuid,ARRAY['asset.upload'])`.execute(
          db,
        );
        f.input.auth = { kind: 'apiKey', id };
        revoke = () => sql`UPDATE api_key SET permissions=ARRAY[]::varchar[] WHERE id=${id}::uuid`.execute(db);
      } else if (dependency.startsWith('link') || dependency === 'albumGrant') {
        const id = randomUUID();
        const album = randomUUID();
        await sql`INSERT INTO album(id) VALUES (${album}::uuid)`.execute(db);
        await sql`INSERT INTO album_user("albumId","userId",role) VALUES (${album}::uuid,${f.ownerId}::uuid,'owner')`.execute(
          db,
        );
        await sql`INSERT INTO shared_link(id,"userId",type,"albumId","allowUpload") VALUES (${id}::uuid,${f.ownerId}::uuid,'ALBUM',${album}::uuid,true)`.execute(
          db,
        );
        f.input.auth = { kind: 'sharedLink', id, type: 'ALBUM', albumId: album };
        switch (dependency) {
          case 'linkGrant': {
            revoke = () => sql`UPDATE shared_link SET "allowUpload"=false WHERE id=${id}::uuid`.execute(db);

            break;
          }
          case 'linkExpiry': {
            revoke = () =>
              sql`UPDATE shared_link SET "expiresAt"=clock_timestamp()-interval '1 second' WHERE id=${id}::uuid`.execute(
                db,
              );

            break;
          }
          case 'linkType': {
            revoke = () => sql`UPDATE shared_link SET type='INDIVIDUAL' WHERE id=${id}::uuid`.execute(db);

            break;
          }
          case 'albumGrant': {
            revoke = () => sql`DELETE FROM album_user WHERE "albumId"=${album}::uuid`.execute(db);

            break;
          }
          default: {
            const other = randomUUID();
            await sql`INSERT INTO "user"(id) VALUES (${other}::uuid)`.execute(db);
            revoke = () => sql`UPDATE shared_link SET "userId"=${other}::uuid WHERE id=${id}::uuid`.execute(db);
          }
        }
      } else if (dependency === 'owner') {
        revoke = () => sql`UPDATE "user" SET status='deleted' WHERE id=${f.ownerId}::uuid`.execute(db);
      } else if (dependency === 'livePhoto') {
        const video = await localAsset(f.ownerId, 'b'.repeat(40));
        f.input.options.livePhotoVideoId = video;
        revoke = () => sql`UPDATE asset SET "deletedAt"=now() WHERE id=${video}::uuid`.execute(db);
      } else {
        revoke = () => sql`DELETE FROM session WHERE id=${f.sessionId}::uuid`.execute(db);
      }
      f.controls.beforeGet = async () => {
        await revoke();
      };
      await expect(f.service.publish(f.input)).rejects.toThrow('held');
      const receipt = await f.receipt();
      expect(receipt.result).toBeNull();
      const attempt = await f.catalog.getIntent(receipt.intentId!);
      expect(attempt, JSON.stringify(attempt)).toMatchObject({ state: 'acknowledged' });
      expect(await db.selectFrom('asset').select('id').where('id', '=', receipt.reservedAssetId).execute()).toEqual([]);
      expect(await f.usage()).toBe('0');
      expect(
        await db.selectFrom('upload_publication_outbox').select('id').where('receiptId', '=', receipt.id).execute(),
      ).toEqual([]);
    } finally {
      await f.close();
    }
  },
);

it('uses current DB time after waiting on an unchanged locked authentication row', async () => {
  const f = await publicationFixture(db);
  let release!: () => void;
  let blocker: Promise<void> | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    f.controls.beforeGet = async () => {
      await sql`UPDATE session SET "expiresAt"=clock_timestamp()+interval '200 milliseconds' WHERE id=${f.sessionId}::uuid`.execute(
        db,
      );
      const { promise: started, resolve: entered } = Promise.withResolvers<void>();
      const { promise: gate, resolve } = Promise.withResolvers<void>();
      release = resolve;
      blocker = db.transaction().execute(async (tx) => {
        await tx.selectFrom('session').select('id').where('id', '=', f.sessionId).forUpdate().executeTakeFirstOrThrow();
        entered();
        await gate;
      });
      await started;
      timer = setTimeout(release, 500);
    };
    await expect(f.service.publish(f.input)).rejects.toThrow('held');
    expect(await f.usage()).toBe('0');
    expect(await db.selectFrom('asset').select('id').where('ownerId', '=', f.ownerId).execute()).toEqual([]);
  } finally {
    clearTimeout(timer);
    release?.();
    await blocker;
    await f.close();
  }
});

it('rolls publication back if a shared link expires during slow SQL writes before commit', async () => {
  const f = await publicationFixture(db);
  const id = randomUUID();
  await sql`INSERT INTO shared_link(id,"userId",type,"allowUpload") VALUES (${id}::uuid,${f.ownerId}::uuid,'INDIVIDUAL',true)`.execute(
    db,
  );
  f.input.auth = { kind: 'sharedLink', id, type: 'INDIVIDUAL', albumId: null };
  await sql`CREATE FUNCTION slow_publication_effect() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.3); RETURN NEW; END $$; CREATE TRIGGER slow_publication_effect BEFORE INSERT ON upload_publication_outbox FOR EACH ROW EXECUTE FUNCTION slow_publication_effect()`.execute(
    db,
  );
  try {
    f.controls.beforeGet = async () => {
      await sql`UPDATE shared_link SET "expiresAt"=clock_timestamp()+interval '200 milliseconds' WHERE id=${id}::uuid`.execute(
        db,
      );
    };
    await expect(f.service.publish(f.input)).rejects.toThrow('held');
    expect(await f.usage()).toBe('0');
    const receipt = await f.receipt();
    expect(
      await db
        .selectFrom('shared_link_asset')
        .select('assetId')
        .where('assetId', '=', receipt.reservedAssetId)
        .execute(),
    ).toEqual([]);
    expect(await db.selectFrom('asset').select('id').where('id', '=', receipt.reservedAssetId).execute()).toEqual([]);
    expect(
      await db.selectFrom('upload_publication_outbox').select('id').where('receiptId', '=', receipt.id).execute(),
    ).toEqual([]);
  } finally {
    await sql`DROP TRIGGER slow_publication_effect ON upload_publication_outbox; DROP FUNCTION slow_publication_effect()`.execute(
      db,
    );
    await f.close();
  }
});

it('rolls a real checksum race back and resolves the winning asset in a fresh transaction', async () => {
  const f = await publicationFixture(db);
  const lockId = 179_350_004;
  let release!: () => void;
  let locker: Promise<void> | undefined;
  let publishing: ReturnType<typeof f.service.publish> | undefined;
  try {
    f.controls.beforeGet = async () => {
      const receipt = await f.receipt();
      await sql
        .raw(
          `CREATE FUNCTION wait_publication_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id = '${receipt.reservedAssetId}'::uuid THEN PERFORM pg_advisory_xact_lock(${lockId}); END IF; RETURN NEW; END $$; CREATE TRIGGER wait_publication_insert BEFORE INSERT ON asset FOR EACH ROW EXECUTE FUNCTION wait_publication_insert()`,
        )
        .execute(db);
      const { promise: started, resolve: entered } = Promise.withResolvers<void>();
      const { promise: gate, resolve } = Promise.withResolvers<void>();
      release = resolve;
      locker = db.connection().execute(async (connection) => {
        await sql`SELECT pg_advisory_lock(${lockId})`.execute(connection);
        entered();
        await gate;
        await sql`SELECT pg_advisory_unlock(${lockId})`.execute(connection);
      });
      await started;
    };
    publishing = f.service.publish(f.input);
    await vi.waitFor(
      async () => {
        const waiting = await sql<{
          count: string;
        }>`SELECT count(*)::text AS count FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory'`.execute(
          db,
        );
        expect(Number(waiting.rows[0].count)).toBeGreaterThan(0);
      },
      { timeout: 5000 },
    );
    const winner = await localAsset(f.ownerId, f.source.sha1!, 'IMAGE');
    release();
    await locker;
    const response = await publishing;
    expect(response).toEqual({ id: winner, status: 'duplicate' });
    expect(await f.usage()).toBe('0');
    const receipt = await f.receipt();
    expect(await db.selectFrom('asset').select('id').where('id', '=', receipt.reservedAssetId).execute()).toEqual([]);
    expect(await db.selectFrom('asset_original_claim').select('id').where('ownerId', '=', f.ownerId).execute()).toEqual(
      [],
    );
    expect(
      await db.selectFrom('upload_publication_outbox').select('id').where('receiptId', '=', receipt.id).execute(),
    ).toEqual([]);
    expect(receipt.state).toBe('duplicate');
  } finally {
    release?.();
    await locker;
    await publishing?.catch(() => {});
    await sql`DROP TRIGGER IF EXISTS wait_publication_insert ON asset; DROP FUNCTION IF EXISTS wait_publication_insert()`.execute(
      db,
    );
    await f.close();
  }
});

it('does not acknowledge an unpersisted alternate idempotency key for an already reserved source', async () => {
  const f = await publicationFixture(db);
  try {
    f.input.idempotencyKey = 'first-key';
    await f.service.publish(f.input);
    await expect(f.service.publish({ ...f.input, idempotencyKey: 'unbound-key' })).rejects.toMatchObject({
      status: 409,
    });
  } finally {
    await f.close();
  }
});

it('exposes the pinned worker connection for SQL progress without reserving a second pool slot', async () => {
  const f = await publicationFixture(db);
  try {
    const result = await f.publications.withWorker(randomUUID(), async (connection) => {
      expect(connection).toBeDefined();
      const query = await sql<{ value: number }>`SELECT 1 AS value`.execute(connection);
      return query.rows[0].value;
    });
    expect(result).toBe(1);
  } finally {
    await f.close();
  }
});

it('completes actual SDK publication with a one-connection SQL pool', async () => {
  const single = new Kysely<DB>({
    dialect: new PostgresJSDialect({ postgres: postgres({ ...options, database, max: 1 }) }),
  });
  const f = await publicationFixture(single);
  try {
    expect(await f.service.publish(f.input)).toMatchObject({ status: 'created' });
    expect(await f.usage()).toBe('17');
    expect(f.requests.map((request) => request.method)).toEqual(['PUT', 'GET']);
  } finally {
    await f.close();
    await single.destroy();
  }
});

it('rejects publication of an actual uploaded but unverified version and mismatched source ownership', async () => {
  const f = await publicationFixture(db);
  try {
    const receipt = await f.publications.reserve(f.input, f.binding);
    const other = await f.makeSource('other source');
    await expect(f.stages.stageOwned(other.id, f.ownerId, receipt.id)).rejects.toThrow();
    const stage = await f.stages.stageOwned(f.source.id, f.ownerId, receipt.id);
    const intent = await f.catalog.prepareOwned(receipt.id, f.ownerId, f.binding.storeId);
    await f.stages.sendPrepared(intent.id, f.binding, 60_000, receipt.id);
    await expect(f.publications.complete(receipt.id, f.ownerId)).rejects.toThrow('evidence mismatch');
    await expect(f.publications.complete(receipt.id, randomUUID())).rejects.toThrow();
    expect(await db.selectFrom('asset').select('id').where('id', '=', receipt.reservedAssetId).execute()).toEqual([]);
    expect(await f.usage()).toBe('0');
    expect(f.requests.map((request) => request.method)).toEqual(['PUT']);
    expect(await f.service.publish(f.input)).toMatchObject({ status: 'created' });
    expect(await f.catalog.listIntents(stage.id)).toHaveLength(1);
    expect(f.requests.map((request) => request.method)).toEqual(['PUT', 'GET']);
  } finally {
    await f.close();
  }
});

it('snapshots mutable caller DTO and trusted binding before awaiting SQL or provider effects', async () => {
  const f = await publicationFixture(db);
  try {
    const storeId = f.binding.storeId;
    const endpoint = f.binding.endpoint;
    const publishing = f.service.publish(f.input);
    f.input.options.filename = 'changed-by-caller.jpg';
    f.input.options.metadata.push({ key: 'late', value: {} });
    f.input.auth.id = randomUUID();
    f.binding.storeId = randomUUID();
    f.binding.endpoint = 'https://never-contact.invalid';
    const result = await publishing;
    expect(result.status).toBe('created');
    const receipt = await f.receipt();
    expect(receipt).toMatchObject({
      storeId,
      endpoint,
      options: { filename: 'photo.jpg', metadata: [] },
      auth: { id: f.sessionId },
    });
    const mapping = await db
      .selectFrom('asset_original')
      .selectAll()
      .where('assetId', '=', result.id)
      .executeTakeFirstOrThrow();
    expect(mapping).toMatchObject({ storeId, endpoint });
    expect(await db.selectFrom('asset_metadata').select('assetId').where('assetId', '=', result.id).execute()).toEqual(
      [],
    );
  } finally {
    await f.close();
  }
});
