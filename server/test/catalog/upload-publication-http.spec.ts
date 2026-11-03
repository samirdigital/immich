import { APP_GUARD, APP_PIPE } from '@nestjs/core';
import { Test } from '@nestjs/testing';
import { Kysely, sql } from 'kysely';
import { PostgresJSDialect } from 'kysely-postgres-js';
import { ZodValidationPipe } from 'nestjs-zod';
import { createHash, randomUUID } from 'node:crypto';
import { createWriteStream } from 'node:fs';
import { readFile, stat } from 'node:fs/promises';
import { request as httpRequest } from 'node:http';
import os from 'node:os';
import path from 'node:path';
import postgres from 'postgres';
import request from 'supertest';
import { AssetMediaController } from 'src/controllers/asset-media.controller.js';
import { UserController } from 'src/controllers/user.controller.js';
import { AuthDto } from 'src/dtos/auth.dto.js';
import { AlbumUserRole, Permission, SharedLinkType } from 'src/enum.js';
import { AssetUploadInterceptor } from 'src/middleware/asset-upload.interceptor.js';
import { AuthGuard } from 'src/middleware/auth.guard.js';
import { FileUploadInterceptor, getFiles } from 'src/middleware/file-upload.interceptor.js';
import { AssetOriginalRepository } from 'src/repositories/asset-original.repository.js';
import { AssetRepository } from 'src/repositories/asset.repository.js';
import { LoggingRepository } from 'src/repositories/logging.repository.js';
import { OwnedSourceRepository } from 'src/repositories/owned-source.repository.js';
import { StorageRepository } from 'src/repositories/storage.repository.js';
import { DB } from 'src/schema/index.js';
import { up as catalogUp } from 'src/schema/migrations/1793500000000-AddObjectCatalog.js';
import { up as multipartUp } from 'src/schema/migrations/1793500001000-AddS3MultipartJournal.js';
import { up as originalUp } from 'src/schema/migrations/1793500002000-AddAssetOriginal.js';
import { up as sourceUp } from 'src/schema/migrations/1793500003000-AddUploadSourceReceipt.js';
import {
  down as publicationDown,
  up as publicationUp,
} from 'src/schema/migrations/1793500004000-AddUploadPublication.js';
import { AssetMediaService } from 'src/services/asset-media.service.js';
import { AuthService } from 'src/services/auth.service.js';
import { OriginalReader } from 'src/services/original-reader.js';
import { UploadPublicationHttpAdapter } from 'src/services/upload-publication-http.adapter.js';
import { createManagedUpload } from 'src/services/upload-publication-http.factory.js';
import { UserService } from 'src/services/user.service.js';
import { ImmichFile, UploadFiles } from 'src/types.js';
import { publicationFixture } from 'test/catalog/upload-publication-fixture.js';
import { newTestService } from 'test/utils.js';

const socket = process.env.CATALOG_TEST_SOCKET;
if (process.env.CATALOG_TEST_DISPOSABLE !== 'YES' || !socket || !path.isAbsolute(socket)) {
  throw new Error('Requires disposable private PostgreSQL socket');
}
const database = `immich_http_${randomUUID().replaceAll('-', '')}`;
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

const setupHttp = async () => {
  const f = await publicationFixture(db);
  const { sut, mocks } = newTestService(AssetMediaService);
  const adapter = (await createManagedUpload(
    {
      IMMICH_OBJECT_UPLOAD_ENABLED: 'true',
      IMMICH_OBJECT_SOURCE_ROOT: f.root,
      IMMICH_OBJECT_CATALOG_ROOT: f.stageRoot,
    },
    db,
    { get: () => f.binding.store, uploadBinding: f.binding },
  ))!;
  const received = vi.spyOn(adapter, 'receive');
  const originals = new OriginalReader(new AssetOriginalRepository(db), { get: () => f.binding.store });
  Object.assign(sut, { managedUpload: adapter, originalReader: originals });
  mocks.asset.getForOriginal.mockImplementation((id, edited) => new AssetRepository(db).getForOriginal(id, edited));
  mocks.access.asset.checkOwnerAccess.mockImplementation((_owner, ids) => Promise.resolve(new Set(ids)));
  const auth = { user: { id: f.ownerId }, session: { id: f.sessionId } } as AuthDto;
  const authenticate = vi.fn().mockResolvedValue(auth);
  const profile = { createProfileImage: vi.fn().mockResolvedValue({ success: true }) };
  const module = await Test.createTestingModule({
    controllers: [AssetMediaController, UserController],
    providers: [
      { provide: UserService, useValue: profile },
      { provide: APP_GUARD, useClass: AuthGuard },
      { provide: APP_PIPE, useClass: ZodValidationPipe },
      { provide: AuthService, useValue: { authenticate } },
      { provide: AssetMediaService, useValue: sut },
      { provide: LoggingRepository, useValue: mocks.logger },
      { provide: StorageRepository, useValue: mocks.storage },
      { provide: UploadPublicationHttpAdapter, useValue: adapter },
      AssetUploadInterceptor,
      FileUploadInterceptor,
    ],
  }).compile();
  const app = module.createNestApplication();
  app.useLogger(false);
  await app.init();
  const post = (bytes = 'actual multipart bytes', key?: string) => {
    const upload = request(app.getHttpServer()).post('/assets');
    if (key) {
      upload.set('x-immich-upload-idempotency-key', key);
    }
    return upload
      .field('fileCreatedAt', '2020-01-01T00:00:00Z')
      .field('fileModifiedAt', '2020-01-01T00:00:00Z')
      .attach('assetData', Buffer.from(bytes), 'photo.jpg');
  };
  return {
    f,
    sut,
    mocks,
    adapter,
    auth,
    authenticate,
    app,
    module,
    received,
    post,
    profile,
    close: async () => {
      await app.close();
      await f.close();
    },
  };
};

it('publishes actual authenticated multipart only after independent SDK GET, with pending effects and no legacy mutations', async () => {
  const { f, sut, mocks, authenticate, app, received, close } = await setupHttp();
  f.controls.beforeGet = async () => {
    expect(await db.selectFrom('asset').select('id').where('ownerId', '=', f.ownerId).execute()).toEqual([]);
  };
  try {
    const response = await request(app.getHttpServer())
      .post('/assets')
      .field('fileCreatedAt', '2020-01-01T00:00:00Z')
      .field('fileModifiedAt', '2020-01-01T00:00:00Z')
      .field('isFavorite', 'true')
      .attach('assetData', Buffer.from('actual multipart bytes'), 'photo.jpg');
    expect(response.status, JSON.stringify(response.body)).toBe(201);
    expect(response.body).toMatchObject({ status: 'created', id: expect.any(String) });
    expect(f.requests.map((x) => x.method)).toEqual(['PUT', 'GET']);
    const asset = await db.selectFrom('asset').selectAll().where('id', '=', response.body.id).executeTakeFirstOrThrow();
    expect(asset).toMatchObject({ originalAuthority: 'object', isFavorite: true, originalFileName: 'photo.jpg' });
    expect(await f.usage()).toBe(String(Buffer.byteLength('actual multipart bytes')));
    const publication = await db
      .selectFrom('upload_publication')
      .select('id')
      .where('reservedAssetId', '=', asset.id)
      .executeTakeFirstOrThrow();
    const effects = await db
      .selectFrom('upload_publication_outbox')
      .selectAll()
      .where('receiptId', '=', publication.id)
      .execute();
    expect(effects).toHaveLength(3);
    expect(effects.every((x) => x.state === 'pending')).toBe(true);
    expect(mocks.asset.create).not.toHaveBeenCalled();
    expect(mocks.job.queue).not.toHaveBeenCalled();
    expect(mocks.event.emit).not.toHaveBeenCalled();
    expect(authenticate).toHaveBeenCalled();
    f.controls.beforeGet = undefined;
    const downloaded = await request(app.getHttpServer()).get(`/assets/${asset.id}/original`);
    expect(downloaded.status).toBe(200);
    expect(downloaded.body).toEqual(Buffer.from('actual multipart bytes'));
    expect(f.requests.map((entry) => entry.method)).toEqual(['PUT', 'GET', 'GET']);
    const raw = received.mock.calls[0][1] as ImmichFile;
    const mapped = getFiles({ assetData: [raw], sidecarData: [] } as UploadFiles).file;
    expect(mapped.sourceReceiptId).toBe(raw.uuid);
    expect(mapped.checksum.toString('hex')).toBe(createHash('sha1').update('actual multipart bytes').digest('hex'));
    await sut.onUploadError({ user: { user: { id: f.ownerId } } } as never, raw);
    expect(await readFile(asset.originalPath, 'utf8')).toBe('actual multipart bytes');
    expect(mocks.job.queue).not.toHaveBeenCalled();
  } finally {
    await close();
  }
});

it('uses the actual body rather than the checksum shortcut for idempotency conflicts and ignores forged body receipt IDs', async () => {
  const h = await setupHttp();
  try {
    const created = await h.post('one', 'same-key');
    expect(created.status).toBe(201);
    const replay = await h.post('one', 'same-key');
    expect(replay.status).toBe(201);
    expect(replay.body).toEqual(created.body);
    const conflict = await h
      .post('two', 'same-key')
      .set('x-immich-checksum', 'forged')
      .field('sourceReceiptId', h.f.source.id)
      .field('originalPath', '/etc/passwd');
    expect(conflict.status).toBe(409);
    const duplicate = await h.post('one', 'different-key');
    expect(duplicate.status).toBe(200);
    expect(duplicate.body).toEqual({ ...created.body, status: 'duplicate' });
    expect(await h.f.usage()).toBe('3');
    expect(await db.selectFrom('asset').select('id').where('ownerId', '=', h.f.ownerId).execute()).toHaveLength(1);
    expect(h.mocks.asset.getUploadAssetIdByChecksum).not.toHaveBeenCalled();
    expect(h.mocks.job.queue).not.toHaveBeenCalled();
  } finally {
    await h.close();
  }
});

it('maps validated options and sidecar with both receipts sealed to the DTO timestamp', async () => {
  const h = await setupHttp();
  try {
    const response = await h
      .post()
      .field('filename', 'renamed.jpg')
      .field('duration', '0')
      .field('visibility', 'archive')
      .field('metadata', JSON.stringify([{ key: 'test', value: { nested: [1, true] } }]))
      .attach('sidecarData', Buffer.from('<xmp/>'), 'photo.xmp');
    expect(response.status, JSON.stringify(response.body)).toBe(201);
    const receipt = await db
      .selectFrom('upload_publication')
      .selectAll()
      .where('reservedAssetId', '=', response.body.id)
      .executeTakeFirstOrThrow();
    expect(receipt.options).toMatchObject({
      filename: 'renamed.jpg',
      type: 'IMAGE',
      duration: 0,
      visibility: 'archive',
      isFavorite: false,
      metadata: [{ key: 'test', value: { nested: [1, true] } }],
    });
    expect(receipt.sidecarId).toBeTruthy();
    const sources = await db
      .selectFrom('source_receipt')
      .selectAll()
      .where('ownerId', '=', h.f.ownerId)
      .where('id', 'in', [receipt.sourceId, receipt.sidecarId!])
      .execute();
    expect(sources).toHaveLength(2);
    for (const source of sources) {
      expect(source.state).toBe('sealed');
      const stats = await stat(path.join(source.root, source.name));
      expect(stats.mtime.toISOString()).toBe('2020-01-01T00:00:00.000Z');
    }
  } finally {
    await h.close();
  }
});

it('rejects revoked current permission before receiving any bytes or calling the provider', async () => {
  const h = await setupHttp();
  try {
    await db.deleteFrom('session').where('id', '=', h.f.sessionId).execute();
    const response = await h.post();
    expect(response.status).toBe(403);
    expect(h.f.requests).toEqual([]);
    expect(
      await db.selectFrom('source_receipt').select('id').where('ownerId', '=', h.f.ownerId).execute(),
    ).toHaveLength(1);
    expect(h.mocks.job.queue).not.toHaveBeenCalled();
  } finally {
    await h.close();
  }
});

it.each([
  ['bad-date', 'photo.jpg', 'not-a-date'],
  ['unsupported', 'photo.exe', '2020-01-01T00:00:00Z'],
  ['empty', 'photo.jpg', '2020-01-01T00:00:00Z'],
])('retains journaled sources and avoids provider calls for %s multipart', async (kind, name, date) => {
  const h = await setupHttp();
  try {
    const response = await request(h.app.getHttpServer())
      .post('/assets')
      .field('fileCreatedAt', date)
      .field('fileModifiedAt', '2020-01-01T00:00:00Z')
      .attach('assetData', Buffer.from(kind === 'empty' ? '' : 'bytes'), name);
    expect(response.status).toBe(400);
    expect(h.f.requests).toEqual([]);
    expect(h.mocks.storage.unlink).not.toHaveBeenCalled();
    expect(h.mocks.job.queue).not.toHaveBeenCalled();
  } finally {
    await h.close();
  }
});

it('keeps published originals and SQL assets after a real postcommit HTTP disconnect and both legacy cleanup hooks', async () => {
  const h = await setupHttp();
  const publish = h.adapter.upload.bind(h.adapter);
  const { promise: committedSignal, resolve: committed } = Promise.withResolvers<void>();
  const { promise: gate, resolve: resume } = Promise.withResolvers<void>();
  const { promise: completedSignal, resolve: completed } = Promise.withResolvers<void>();
  let publishedId = '';
  vi.spyOn(h.adapter, 'upload').mockImplementation(async (...args) => {
    const result = await publish(...args);
    publishedId = result.id;
    committed();
    await gate;
    completed();
    return result;
  });
  h.mocks.job.queue.mockRejectedValue(new Error('queue must not be involved'));
  h.mocks.event.emit.mockRejectedValue(new Error('event must not be involved'));
  try {
    await h.app.listen(0, '127.0.0.1');
    const server = h.app.getHttpServer();
    const body = Buffer.from(
      '--boundary\r\nContent-Disposition: form-data; name="fileCreatedAt"\r\n\r\n2020-01-01T00:00:00Z\r\n--boundary\r\nContent-Disposition: form-data; name="fileModifiedAt"\r\n\r\n2020-01-01T00:00:00Z\r\n--boundary\r\nContent-Disposition: form-data; name="assetData"; filename="photo.jpg"\r\nContent-Type: image/jpeg\r\n\r\ndisconnect bytes\r\n--boundary--\r\n',
    );
    const client = httpRequest({
      host: '127.0.0.1',
      port: server.address().port,
      path: '/assets',
      method: 'POST',
      headers: { 'content-type': 'multipart/form-data; boundary=boundary', 'content-length': body.length },
    });
    client.on('error', () => {});
    client.end(body);
    await committedSignal;
    const closed = new Promise<void>((resolve) => client.once('close', resolve));
    client.destroy();
    resume();
    await closed;
    await completedSignal;
    const raw = h.received.mock.calls[0][1];
    const cloned = { ...raw };
    const hooks = h.module.get(FileUploadInterceptor) as unknown as {
      removeFile: (req: unknown, file: Express.Multer.File, cb: (error: Error | null) => void) => Promise<void>;
    };
    await new Promise<void>((resolve, reject) => {
      void hooks.removeFile({ user: h.auth }, cloned, (error) => (error ? reject(error) : resolve()));
    });
    await h.sut.onUploadError({ user: h.auth } as never, raw);
    const failedRead = vi
      .spyOn(OwnedSourceRepository.prototype, 'get')
      .mockRejectedValueOnce(new Error('SQL unavailable'));
    expect(await h.adapter.retain(cloned, h.f.ownerId)).toBe(true);
    failedRead.mockRestore();
    expect(await h.adapter.retain({ sourceReceiptId: h.f.source.id, path: raw.path }, h.f.ownerId)).toBe(false);
    const asset = await db.selectFrom('asset').selectAll().where('id', '=', publishedId).executeTakeFirstOrThrow();
    expect(await readFile(asset.originalPath, 'utf8')).toBe('disconnect bytes');
    expect(h.mocks.storage.unlink).not.toHaveBeenCalled();
    expect(h.mocks.asset.remove).not.toHaveBeenCalled();
    expect(h.mocks.job.queue).not.toHaveBeenCalled();
    expect(h.mocks.event.emit).not.toHaveBeenCalled();
    const response = await request(server).get(`/assets/${publishedId}/original`);
    expect(response.status).toBe(200);
    expect(response.body).toEqual(Buffer.from('disconnect bytes'));
  } finally {
    resume();
    await h.close();
  }
});

it('uses a fresh shared-link SQL context from the trusted authenticated ID and never fingerprints credentials', async () => {
  const h = await setupHttp();
  try {
    const id = randomUUID();
    await sql`INSERT INTO shared_link (id, "userId", type, "allowUpload") VALUES (${id}::uuid, ${h.f.ownerId}::uuid, 'INDIVIDUAL', true)`.execute(
      db,
    );
    h.authenticate.mockResolvedValue({
      user: h.auth.user,
      sharedLink: { id, userId: h.f.ownerId, albumId: null, allowUpload: true, password: 'never-record-this' },
    });
    const response = await h.post().field('auth', JSON.stringify({ kind: 'session', id: randomUUID() }));
    expect(response.status, JSON.stringify(response.body)).toBe(201);
    const receipt = await db
      .selectFrom('upload_publication')
      .selectAll()
      .where('reservedAssetId', '=', response.body.id)
      .executeTakeFirstOrThrow();
    expect(receipt.auth).toEqual({ kind: 'sharedLink', id, type: 'INDIVIDUAL', albumId: null });
    expect(JSON.stringify(receipt)).not.toContain('never-record-this');
    expect(await db.selectFrom('shared_link_asset').selectAll().where('sharedLinkId', '=', id).execute()).toHaveLength(
      1,
    );
    await db.updateTable('shared_link').set({ allowUpload: false }).where('id', '=', id).execute();
    const calls = h.f.requests.length;
    const denied = await h.post('another');
    expect(denied.status).toBe(403);
    expect(h.f.requests).toHaveLength(calls);
  } finally {
    await h.close();
  }
});

it('leaves ambiguous remote attempts held across HTTP replay without another PUT or legacy rollback', async () => {
  const h = await setupHttp();
  try {
    h.f.controls.unknownPut = true;
    const first = await h.post('ambiguous', 'held-key');
    const replay = await h.post('ambiguous', 'held-key');
    expect(first.status).toBe(409);
    expect(replay.status).toBe(409);
    expect(h.f.requests.map((x) => x.method)).toEqual(['PUT']);
    expect(await db.selectFrom('asset').select('id').where('ownerId', '=', h.f.ownerId).execute()).toEqual([]);
    expect(h.mocks.job.queue).not.toHaveBeenCalled();
    expect(h.mocks.asset.remove).not.toHaveBeenCalled();
  } finally {
    await h.close();
  }
});

it('journals a cancelled half-written real HTTP multipart source and never schedules raw deletion', async () => {
  const h = await setupHttp();
  try {
    await h.app.listen(0, '127.0.0.1');
    const body = Buffer.from(
      '--boundary\r\nContent-Disposition: form-data; name="assetData"; filename="photo.jpg"\r\nContent-Type: image/jpeg\r\n\r\nhalf-written',
    );
    const client = httpRequest({
      host: '127.0.0.1',
      port: h.app.getHttpServer().address().port,
      path: '/assets',
      method: 'POST',
      headers: { 'content-type': 'multipart/form-data; boundary=boundary', 'content-length': body.length + 1000 },
    });
    client.on('error', () => {});
    client.write(body);
    await vi.waitFor(async () => {
      const source = await db
        .selectFrom('source_receipt')
        .selectAll()
        .where('ownerId', '=', h.f.ownerId)
        .where('id', '!=', h.f.source.id)
        .executeTakeFirstOrThrow();
      const stats = await stat(path.join(source.root, source.name));
      expect(stats.size).toBeGreaterThan(0);
    });
    client.destroy();
    await vi.waitFor(
      async () => {
        const sources = await db
          .selectFrom('source_receipt')
          .selectAll()
          .where('ownerId', '=', h.f.ownerId)
          .where('id', '!=', h.f.source.id)
          .execute();
        expect(sources).toHaveLength(1);
        expect(sources[0].state).toBe('held');
      },
      { timeout: 2000 },
    );
    expect(h.f.requests).toEqual([]);
    expect(h.mocks.storage.unlink).not.toHaveBeenCalled();
    expect(h.mocks.job.queue).not.toHaveBeenCalled();
  } finally {
    await h.close();
  }
});

it('returns a local checksum duplicate unchanged without manufacturing object authority or charging quota', async () => {
  const h = await setupHttp();
  try {
    const id = randomUUID();
    await sql`INSERT INTO asset (id,"ownerId","originalPath",checksum,type,"fileCreatedAt","fileModifiedAt","localDateTime","originalFileName") VALUES (${id}::uuid,${h.f.ownerId}::uuid,'/retained/existing.jpg',decode(${createHash('sha1').update('local winner').digest('hex')},'hex'),'IMAGE',now(),now(),now(),'existing.jpg')`.execute(
      db,
    );
    const before = await db.selectFrom('asset').selectAll().where('id', '=', id).executeTakeFirstOrThrow();
    const response = await h.post('local winner');
    expect(response.status).toBe(200);
    expect(response.body).toEqual({ id, status: 'duplicate' });
    expect(await db.selectFrom('asset').selectAll().where('id', '=', id).executeTakeFirstOrThrow()).toEqual(before);
    expect(before.originalAuthority).toBe('local');
    expect(await h.f.usage()).toBe('0');
    expect(await db.selectFrom('asset_original').selectAll().where('assetId', '=', id).execute()).toEqual([]);
    expect(
      await db
        .selectFrom('upload_publication_outbox')
        .selectAll()
        .innerJoin('upload_publication', 'upload_publication.id', 'upload_publication_outbox.receiptId')
        .where('upload_publication.ownerId', '=', h.f.ownerId)
        .execute(),
    ).toEqual([]);
  } finally {
    await h.close();
  }
});

it('preserves legacy multipart storage for the real profile route while managed asset uploads are enabled', async () => {
  const h = await setupHttp();
  try {
    vi.spyOn(h.sut, 'getUploadFolder').mockReturnValue(h.f.root);
    h.mocks.storage.createWriteStream.mockImplementation(createWriteStream);
    const response = await request(h.app.getHttpServer())
      .post('/users/profile-image')
      .attach('file', Buffer.from('avatar'), 'avatar.jpg');
    expect(response.status).toBe(201);
    expect(h.received).not.toHaveBeenCalled();
    const file = h.profile.createProfileImage.mock.calls[0][1] as Express.Multer.File;
    expect(await readFile(file.path, 'utf8')).toBe('avatar');
    expect(h.f.requests).toEqual([]);
  } finally {
    await h.close();
  }
});

it('preserves actual local multipart dispatch and checksum shortcut when the managed flag is disabled', async () => {
  const h = await setupHttp();
  try {
    Object.assign(h.sut, { managedUpload: undefined });
    vi.spyOn(h.sut, 'getUploadFolder').mockReturnValue(h.f.root);
    h.mocks.storage.createWriteStream.mockImplementation(createWriteStream);
    const id = randomUUID();
    h.mocks.asset.create.mockResolvedValue({ id } as never);
    const response = await h.post('legacy bytes');
    expect(response.status).toBe(201);
    expect(response.body).toEqual({ id, status: 'created' });
    const local = h.mocks.asset.create.mock.calls[0][0];
    expect(await readFile(local.originalPath!, 'utf8')).toBe('legacy bytes');
    expect(h.received).not.toHaveBeenCalled();
    expect(h.f.requests).toEqual([]);
    expect(h.mocks.job.queue).toHaveBeenCalled();
    expect(h.mocks.event.emit).toHaveBeenCalled();
    h.mocks.asset.getUploadAssetIdByChecksum.mockResolvedValue(id);
    const duplicate = await request(h.app.getHttpServer()).post('/assets').set('x-immich-checksum', '00'.repeat(20));
    expect(duplicate.status).toBe(200);
    expect(duplicate.body).toEqual({ id, status: 'duplicate' });
  } finally {
    await h.close();
  }
});

it('maps a live-photo dependency from the validated DTO and leaves its SQL hiding to atomic publication', async () => {
  const h = await setupHttp();
  try {
    const id = randomUUID();
    await sql`INSERT INTO asset (id,"ownerId","originalPath",checksum,type,"fileCreatedAt","fileModifiedAt","localDateTime","originalFileName") VALUES (${id}::uuid,${h.f.ownerId}::uuid,'/retained/video.mov',decode(${'aa'.repeat(20)},'hex'),'VIDEO',now(),now(),now(),'video.mov')`.execute(
      db,
    );
    const response = await h.post().field('livePhotoVideoId', id);
    expect(response.status).toBe(201);
    const asset = await db.selectFrom('asset').selectAll().where('id', '=', response.body.id).executeTakeFirstOrThrow();
    expect(asset.livePhotoVideoId).toBe(id);
    const video = await db.selectFrom('asset').select('visibility').where('id', '=', id).executeTakeFirstOrThrow();
    expect(video.visibility).toBe('hidden');
    expect(h.mocks.event.emit).not.toHaveBeenCalled();
  } finally {
    await h.close();
  }
});

it('runs real Multer removeFile on a later unexpected file without unlinking the receipt-backed original', async () => {
  const h = await setupHttp();
  try {
    const response = await h.post().attach('unexpectedFile', Buffer.from('invalid'), 'invalid.jpg');
    expect(response.status).toBe(400);
    const source = await db
      .selectFrom('source_receipt')
      .selectAll()
      .where('ownerId', '=', h.f.ownerId)
      .where('id', '!=', h.f.source.id)
      .executeTakeFirstOrThrow();
    expect(await readFile(path.join(source.root, source.name), 'utf8')).toBe('actual multipart bytes');
    expect(h.mocks.storage.unlink).not.toHaveBeenCalled();
    expect(h.mocks.job.queue).not.toHaveBeenCalled();
    expect(h.f.requests).toEqual([]);
  } finally {
    await h.close();
  }
});

it('rejects an unsupported sidecar before publication and leaves any received original journaled', async () => {
  const h = await setupHttp();
  try {
    const response = await h.post().attach('sidecarData', Buffer.from('invalid'), 'invalid.exe');
    expect(response.status).toBe(400);
    expect(h.f.requests).toEqual([]);
    expect(h.mocks.storage.unlink).not.toHaveBeenCalled();
    expect(h.mocks.job.queue).not.toHaveBeenCalled();
  } finally {
    await h.close();
  }
});

it('rejects a current SQL API-key permission revocation before receive despite a stale trusted auth grant', async () => {
  const h = await setupHttp();
  try {
    const id = randomUUID();
    await sql`INSERT INTO api_key (id,"userId",permissions) VALUES (${id}::uuid,${h.f.ownerId}::uuid,ARRAY[]::varchar[])`.execute(
      db,
    );
    h.authenticate.mockResolvedValue({ user: h.auth.user, apiKey: { id, permissions: [Permission.AssetUpload] } });
    const response = await h.post();
    expect(response.status).toBe(403);
    expect(h.f.requests).toEqual([]);
    expect(
      await db.selectFrom('source_receipt').select('id').where('ownerId', '=', h.f.ownerId).execute(),
    ).toHaveLength(1);
  } finally {
    await h.close();
  }
});

it('maps video type duration dates and credential-free API-key identity from actual multipart', async () => {
  const h = await setupHttp();
  try {
    const id = randomUUID();
    await sql`INSERT INTO api_key (id,"userId",permissions) VALUES (${id}::uuid,${h.f.ownerId}::uuid,ARRAY[${Permission.AssetUpload}]::varchar[])`.execute(
      db,
    );
    h.authenticate.mockResolvedValue({ user: h.auth.user, apiKey: { id, permissions: [Permission.AssetUpload] } });
    const response = await request(h.app.getHttpServer())
      .post('/assets')
      .field('fileCreatedAt', '2020-01-01T02:00:00+02:00')
      .field('fileModifiedAt', '2020-01-02T02:00:00+02:00')
      .field('duration', '1234')
      .attach('assetData', Buffer.from('video bytes'), 'video.mov');
    expect(response.status).toBe(201);
    const receipt = await db
      .selectFrom('upload_publication')
      .selectAll()
      .where('reservedAssetId', '=', response.body.id)
      .executeTakeFirstOrThrow();
    expect(receipt.auth).toEqual({ kind: 'apiKey', id });
    expect(receipt.options).toEqual({
      filename: 'video.mov',
      type: 'VIDEO',
      fileCreatedAt: '2020-01-01T00:00:00.000Z',
      fileModifiedAt: '2020-01-02T00:00:00.000Z',
      duration: 1234,
      isFavorite: false,
      visibility: 'timeline',
      livePhotoVideoId: null,
      metadata: [],
    });
  } finally {
    await h.close();
  }
});

it('requires current shared-album owner membership before receiving bytes', async () => {
  const h = await setupHttp();
  try {
    const id = randomUUID();
    const album = randomUUID();
    await sql`INSERT INTO album (id) VALUES (${album}::uuid)`.execute(db);
    await sql`INSERT INTO album_user ("albumId","userId",role) VALUES (${album}::uuid,${h.f.ownerId}::uuid,${AlbumUserRole.Viewer})`.execute(
      db,
    );
    await sql`INSERT INTO shared_link (id,"userId",type,"albumId","allowUpload") VALUES (${id}::uuid,${h.f.ownerId}::uuid,${SharedLinkType.Album},${album}::uuid,true)`.execute(
      db,
    );
    h.authenticate.mockResolvedValue({
      user: h.auth.user,
      sharedLink: { id, userId: h.f.ownerId, albumId: album, allowUpload: true },
    });
    const response = await h.post();
    expect(response.status).toBe(403);
    expect(h.f.requests).toEqual([]);
    expect(
      await db.selectFrom('source_receipt').select('id').where('ownerId', '=', h.f.ownerId).execute(),
    ).toHaveLength(1);
  } finally {
    await h.close();
  }
});
