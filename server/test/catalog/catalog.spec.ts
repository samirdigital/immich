import { S3Client } from '@aws-sdk/client-s3';
import { schemaFromCode } from '@immich/sql-tools';
import { Kysely, sql } from 'kysely';
import { PostgresJSDialect } from 'kysely-postgres-js';
import { execFile, fork } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import {
  chmod,
  link,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rename,
  rm,
  stat,
  symlink,
  unlink,
  writeFile,
} from 'node:fs/promises';
import { createServer } from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { Readable } from 'node:stream';
import { buffer } from 'node:stream/consumers';
import { setTimeout as delay } from 'node:timers/promises';
import { promisify } from 'node:util';
import postgres from 'postgres';
import { LocalObjectStoreRepository } from 'src/repositories/local-object-store.repository.js';
import { ObjectCatalogRepository } from 'src/repositories/object-catalog.repository.js';
import { ObjectStore } from 'src/repositories/object-store.repository.js';
import { S3ObjectStoreRepository } from 'src/repositories/s3-object-store.repository.js';
import { DB, ImmichDatabase } from 'src/schema/index.js';
import { down, up } from 'src/schema/migrations/1793500000000-AddObjectCatalog.js';
import { down as downMultipart, up as upMultipart } from 'src/schema/migrations/1793500001000-AddS3MultipartJournal.js';
import { ObjectCatalogService } from 'src/services/object-catalog.service.js';

const socket = process.env.CATALOG_TEST_SOCKET;
if (process.env.CATALOG_TEST_DISPOSABLE !== 'YES' || !socket || !path.isAbsolute(socket)) {
  throw new Error(
    'Requires CATALOG_TEST_DISPOSABLE=YES and CATALOG_TEST_SOCKET absolute private Unix socket directory',
  );
}
const database = `immich_catalog_test_${randomUUID().replaceAll('-', '')}`;
const options = { host: socket, port: Number(process.env.CATALOG_TEST_PORT || 55_441), user: os.userInfo().username };
const admin = postgres({ ...options, database: 'postgres', max: 1 });
let db: Kysely<DB>;
let repository: ObjectCatalogRepository;
let service: ObjectCatalogService;
let directory: string;
let root: string;
let source: string;
beforeAll(async () => {
  await admin.unsafe(`CREATE DATABASE "${database}"`);
  db = new Kysely<DB>({ dialect: new PostgresJSDialect({ postgres: postgres({ ...options, database }) }) });
  await up(db);
  await upMultipart(db);
});
afterAll(async () => {
  await db?.destroy();
  await admin.unsafe(`DROP DATABASE "${database}"`);
  await admin.end();
});
beforeEach(async () => {
  directory = await mkdtemp(path.join(os.tmpdir(), 'catalog-'));
  root = path.join(directory, 'owned');
  await mkdir(root, { mode: 0o700 });
  source = path.join(directory, 'source');
  await writeFile(source, 'synthetic catalog bytes');
  repository = new ObjectCatalogRepository(db);
  service = new ObjectCatalogService(repository, root);
});
afterEach(async () => {
  await chmod(root, 0o700);
  await rm(directory, { recursive: true, force: true });
});

it('durably stages an exclusive owned readonly copy with lossless identity and SHA256', async () => {
  const stage = await service.stage(source);
  expect(stage.state).toBe('ready');
  expect(stage.hash).toMatch(/^[a-f0-9]{64}$/);
  expect(stage.size).toBe('23');
  expect(await readFile(path.join(root, stage.id), 'utf8')).toBe('synthetic catalog bytes');
  const owned = await stat(path.join(root, stage.id), { bigint: true });
  const sourceStat1 = await stat(source, { bigint: true });
  expect(owned.ino).not.toBe(sourceStat1.ino);
  expect(owned.mode & 0o777n).toBe(0o400n);
  expect(stage.identity?.ino).toBe(owned.ino.toString());
  const storedStage2 = await repository.getStage(stage.id);
  expect(storedStage2?.state).toBe('ready');
});

it.each(['symlink', 'hardlink', 'directory'])(
  'quarantines rejected %s without touching the caller source',
  async (kind) => {
    const rejected = path.join(directory, 'rejected');
    switch (kind) {
      case 'symlink': {
        await symlink(source, rejected);

        break;
      }
      case 'hardlink': {
        await link(source, rejected);

        break;
      }
      case 'directory': {
        await mkdir(rejected);

        break;
      }
      // No default
    }
    await expect(service.stage(rejected)).rejects.toThrow();
    const rows = await db.selectFrom('staged_object').selectAll().where('root', '=', root).execute();
    expect(rows).toHaveLength(1);
    expect(rows[0].state).toBe('quarantined');
    expect(await readFile(source, 'utf8')).toBe('synthetic catalog bytes');
  },
);

it('restart holds incomplete stages and reports unknown files without adopting or sweeping', async () => {
  const id = randomUUID();
  await repository.createStage(id, root);
  await writeFile(path.join(root, id), 'partial');
  await writeFile(path.join(root, 'unknown'), 'unknown');
  const restarted = new ObjectCatalogService(new ObjectCatalogRepository(db), root);
  const report = await restarted.recover();
  expect(report.unknown).toEqual(['unknown']);
  const storedStage3 = await repository.getStage(id);
  expect(storedStage3?.state).toBe('quarantined');
  expect(await readdir(root)).toEqual(expect.arrayContaining([id, 'unknown']));
});

it('database failure before creating stage leaves source intact and creates no owned file', async () => {
  const connection = postgres({ ...options, database: `${database}_missing` });
  const failed = new Kysely<DB>({ dialect: new PostgresJSDialect({ postgres: connection }) });
  try {
    await expect(new ObjectCatalogService(new ObjectCatalogRepository(failed), root).stage(source)).rejects.toThrow();
  } finally {
    await connection.end();
  }
  expect(await readdir(root)).toEqual([]);
  expect(await readFile(source, 'utf8')).toBe('synthetic catalog bytes');
});

it('persists sending before provider effects and atomically acknowledges a local identity', async () => {
  const stage = await service.stage(source);
  const remote = path.join(directory, 'remote');
  await mkdir(remote);
  const store = new LocalObjectStoreRepository(remote);
  const putFile = store.putFile.bind(store);
  store.putFile = async (...args) => {
    const intents = await repository.listIntents(stage.id);
    expect(intents).toHaveLength(1);
    expect(intents[0].state).toBe('sending');
    expect(intents[0].token).toBeTruthy();
    return putFile(...args);
  };
  const version = await service.upload(stage.id, { provider: 'local', storeId: 'synthetic-local-1', store });
  expect(version.versionId).toBeNull();
  expect(version.verification).toBe('pending');
  const storedIntents4 = await repository.listIntents(stage.id);
  expect(storedIntents4[0].state).toBe('acknowledged');
  expect(await readFile(path.join(root, stage.id), 'utf8')).toBe('synthetic catalog bytes');
});

it('two workers cannot prepare or send the same stage concurrently', async () => {
  const stage = await service.stage(source);
  const workers = [repository, new ObjectCatalogRepository(db)];
  const prepared = await Promise.allSettled(workers.map((worker) => worker.prepare(stage.id, 's3', 'bucket-id')));
  expect(prepared.filter((result) => result.status === 'fulfilled')).toHaveLength(1);
  const [intent] = await repository.listIntents(stage.id);
  const claims = await Promise.all(workers.map((worker) => worker.startSending(intent.id, 60_000)));
  expect(claims.filter(Boolean)).toHaveLength(1);
});

it('restart after remote success before acknowledgement holds expired sending forever and fences stale completion', async () => {
  const stage = await service.stage(source);
  const intent = await repository.prepare(stage.id, 'local', 'local-1');
  const claim = await repository.startSending(intent.id, 60_000);
  const remote = path.join(directory, 'remote');
  await mkdir(remote);
  const store = new LocalObjectStoreRepository(remote);
  const result = await store.putFile({ key: intent.key }, path.join(root, stage.id), {
    size: Number(stage.size),
    expectedHash: { algorithm: 'sha256', hex: stage.hash! },
  });
  await db
    .updateTable('object_upload_intent')
    .set({ leaseUntil: sql`clock_timestamp() - interval '1 second'` })
    .where('id', '=', intent.id)
    .execute();
  const restarted = new ObjectCatalogService(new ObjectCatalogRepository(db), root);
  await restarted.recover();
  const storedIntents5 = await repository.listIntents(stage.id);
  expect(storedIntents5[0].state).toBe('ambiguous');
  await expect(repository.acknowledge(intent.id, claim!.token!, result)).rejects.toThrow();
  await expect(repository.prepare(stage.id, 'local', 'local-1')).rejects.toThrow('Unresolved');
  expect(await repository.startSending(intent.id, 60_000)).toBeUndefined();
  expect(await readFile(path.join(root, stage.id), 'utf8')).toBe('synthetic catalog bytes');
  const read = await store.get(result.object);
  expect(read).toBeDefined();
  read!.stream.destroy();
});

it('invalid S3 identity rolls back acknowledgement and keeps all evidence', async () => {
  const stage = await service.stage(source);
  const intent = await repository.prepare(stage.id, 's3', 'bucket-1');
  const claim = await repository.startSending(intent.id, 60_000);
  await expect(repository.acknowledge(intent.id, claim!.token!, { object: { key: intent.key } })).rejects.toThrow(
    'exact identity',
  );
  const storedIntents6 = await repository.listIntents(stage.id);
  expect(storedIntents6[0].state).toBe('sending');
  expect(await repository.listVersions(stage.id)).toEqual([]);
  await expect(
    repository.acknowledge(intent.id, randomUUID(), { object: { key: intent.key, versionId: 'v1' } }),
  ).rejects.toThrow();
  const version = await repository.acknowledge(intent.id, claim!.token!, {
    object: { key: intent.key, versionId: 'v1' },
  });
  expect(version.versionId).toBe('v1');
});

it('a provider error remains an ambiguous pinned attempt, not a retry authorization', async () => {
  const stage = await service.stage(source);
  const remote = path.join(directory, 'remote');
  await mkdir(remote);
  const store = new LocalObjectStoreRepository(remote);
  store.putFile = () => Promise.reject(new Error('uncertain network outcome'));
  await expect(service.upload(stage.id, { provider: 's3', storeId: 'bucket-1', store })).rejects.toThrow('uncertain');
  const [intent] = await repository.listIntents(stage.id);
  expect(intent.state).toBe('ambiguous');
  expect(intent.evidence).toContain('uncertain');
  await expect(repository.prepare(stage.id, 's3', 'bucket-1')).rejects.toThrow();
});

it('holds acknowledged bytes until exact full GET verification, release and no processing pin', async () => {
  const stage = await service.stage(source);
  const remote = path.join(directory, 'remote');
  await mkdir(remote);
  const binding = { provider: 'local' as const, storeId: 'local-1', store: new LocalObjectStoreRepository(remote) };
  const version = await service.upload(stage.id, binding);
  expect(await service.cleanup(stage.id)).toBe(false);
  await repository.releaseStage(stage.id);
  expect(await service.cleanup(stage.id)).toBe(false);
  expect(await service.verify(stage.id, version.id, binding)).toBe(true);
  const pin = await repository.pin(stage.id);
  expect(await service.cleanup(stage.id)).toBe(false);
  await repository.unpin(stage.id, pin);
  expect(await service.cleanup(stage.id)).toBe(true);
  const storedStage7 = await repository.getStage(stage.id);
  expect(storedStage7?.state).toBe('deleted');
  expect(await readdir(root)).toEqual([]);
  expect(await readFile(source, 'utf8')).toBe('synthetic catalog bytes');
  const read = await binding.store.get({ key: version.key });
  expect(read).toBeDefined();
  read!.stream.destroy();
});

it.each(['hash', 'size', 'short', 'long', 'error', 'metadata-error', 'missing', 'identity'])(
  'failed full GET verification (%s) retains stage and durable evidence',
  async (failure) => {
    const stage = await service.stage(source);
    const remote = path.join(directory, 'remote');
    await mkdir(remote);
    const store = new LocalObjectStoreRepository(remote);
    const binding = { provider: 'local' as const, storeId: 'local-1', store };
    const version = await service.upload(stage.id, binding);
    const get = store.get.bind(store);
    (store as ObjectStore).get = async (object) => {
      const read = await get(object);
      if (failure === 'missing') {
        read!.stream.destroy();
        return;
      }
      if (failure === 'error' || failure === 'metadata-error') {
        if (failure === 'metadata-error') {
          read!.size++;
        }
        read!.stream.destroy(new Error('GET stream error'));
        return read;
      }
      if (failure === 'identity') {
        read!.object.key = 'wrong';
        return read;
      }
      if (failure === 'size') {
        read!.size++;
        return read;
      }
      read!.stream.destroy();
      return {
        ...read!,
        stream: Readable.from([failure === 'short' ? 'short' : failure === 'long' ? 'x'.repeat(24) : 'x'.repeat(23)]),
      };
    };
    expect(await service.verify(stage.id, version.id, binding)).toBe(false);
    await repository.releaseStage(stage.id);
    expect(await service.cleanup(stage.id)).toBe(false);
    const [observed] = await repository.listVersions(stage.id);
    expect(observed.verification).toBe('failed');
    expect(observed.observations).toHaveLength(1);
    expect(await readFile(path.join(root, stage.id), 'utf8')).toBe('synthetic catalog bytes');
  },
);

it('cleanup racing upload cannot pass the prepared or sending durable pin', async () => {
  const stage = await service.stage(source);
  const intent = await repository.prepare(stage.id, 's3', 'bucket-1');
  await repository.releaseStage(stage.id);
  const results = await Promise.all([repository.startSending(intent.id, 60_000), service.cleanup(stage.id)]);
  expect(results[0]?.state).toBe('sending');
  expect(results[1]).toBe(false);
  expect(await readFile(path.join(root, stage.id), 'utf8')).toBe('synthetic catalog bytes');
});

async function verifiedStage() {
  const stage = await service.stage(source);
  const remote = path.join(directory, 'remote');
  await mkdir(remote);
  const binding = { provider: 'local' as const, storeId: 'local-1', store: new LocalObjectStoreRepository(remote) };
  const version = await service.upload(stage.id, binding);
  await service.verify(stage.id, version.id, binding);
  await repository.releaseStage(stage.id);
  return { stage, binding, version };
}

it.each(['before', 'after'])('restart completes deleting idempotently %s unlink', async (boundary) => {
  const { stage } = await verifiedStage();
  expect(await repository.claimCleanup(stage.id)).toBeDefined();
  if (boundary === 'after') {
    await unlink(path.join(root, stage.id));
  }
  await new ObjectCatalogService(new ObjectCatalogRepository(db), root).recover();
  const storedStage8 = await repository.getStage(stage.id);
  expect(storedStage8?.state).toBe('deleted');
  await service.recover();
  expect(await readdir(root)).toEqual([]);
});

it.each(['replace', 'symlink', 'hardlink', 'corrupt'])(
  'quarantines %s stage before upload without calling provider',
  async (kind) => {
    const stage = await service.stage(source);
    const owned = path.join(root, stage.id);
    if (kind === 'replace' || kind === 'symlink') {
      await unlink(owned);
    }
    switch (kind) {
      case 'replace': {
        await writeFile(owned, 'synthetic catalog bytes', { mode: 0o400 });

        break;
      }
      case 'symlink': {
        await symlink(source, owned);

        break;
      }
      case 'hardlink': {
        await link(owned, path.join(directory, 'extra'));

        break;
      }
      case 'corrupt': {
        await chmod(owned, 0o600);
        await writeFile(owned, 'corrupted');

        break;
      }
      // No default
    }
    const remote = path.join(directory, 'remote');
    await mkdir(remote);
    const store = new LocalObjectStoreRepository(remote);
    const put = vi.spyOn(store, 'putFile');
    await expect(service.upload(stage.id, { provider: 'local', storeId: 'local-1', store })).rejects.toThrow();
    expect(put).not.toHaveBeenCalled();
    const storedStage9 = await repository.getStage(stage.id);
    expect(storedStage9?.state).toBe('quarantined');
    expect(await readFile(source, 'utf8')).toBe('synthetic catalog bytes');
  },
);

it('cleanup validates inode and preserves a replacement instead of unlinking it', async () => {
  const { stage } = await verifiedStage();
  const owned = path.join(root, stage.id);
  await unlink(owned);
  await writeFile(owned, 'unrelated replacement');
  await expect(service.cleanup(stage.id)).rejects.toThrow();
  expect(await readFile(owned, 'utf8')).toBe('unrelated replacement');
  const storedStage10 = await repository.getStage(stage.id);
  expect(storedStage10?.state).toBe('quarantined');
});

it('DB failure inserting version rolls back acknowledged state after actual remote success', async () => {
  const stage = await service.stage(source);
  const remote = path.join(directory, 'remote');
  await mkdir(remote);
  const store = new LocalObjectStoreRepository(remote);
  await sql`ALTER TABLE object_version ADD CONSTRAINT catalog_test_insert_failure CHECK (false) NOT VALID`.execute(db);
  try {
    await expect(service.upload(stage.id, { provider: 'local', storeId: 'local-1', store })).rejects.toThrow();
    const [intent] = await repository.listIntents(stage.id);
    expect(intent.state).toBe('ambiguous');
    expect(await repository.listVersions(stage.id)).toEqual([]);
    const read = await store.get({ key: intent.key });
    expect(read).toBeDefined();
    read!.stream.destroy();
    expect(await readFile(path.join(root, stage.id), 'utf8')).toBe('synthetic catalog bytes');
  } finally {
    await sql`ALTER TABLE object_version DROP CONSTRAINT catalog_test_insert_failure`.execute(db);
  }
});

it('failed observation persistence preserves durable verification pin even after an earlier verification', async () => {
  const { stage, binding, version } = await verifiedStage();
  await sql`ALTER TABLE object_version ADD CONSTRAINT catalog_test_observation_failure CHECK (false) NOT VALID`.execute(
    db,
  );
  try {
    await expect(service.verify(stage.id, version.id, binding)).rejects.toThrow();
    const storedStage11 = await repository.getStage(stage.id);
    expect(storedStage11?.pins).toHaveLength(1);
    expect(await service.cleanup(stage.id)).toBe(false);
  } finally {
    await sql`ALTER TABLE object_version DROP CONSTRAINT catalog_test_observation_failure`.execute(db);
  }
});

it('cleanup DB failure after unlink keeps deleting recoverable instead of quarantining a known deletion', async () => {
  const { stage } = await verifiedStage();
  await sql`ALTER TABLE staged_object ADD CONSTRAINT catalog_test_finish_failure CHECK (state <> 'deleted') NOT VALID`.execute(
    db,
  );
  try {
    await expect(service.cleanup(stage.id)).rejects.toThrow();
    expect(await readdir(root)).toEqual([]);
    const storedStage12 = await repository.getStage(stage.id);
    expect(storedStage12?.state).toBe('deleting');
  } finally {
    await sql`ALTER TABLE staged_object DROP CONSTRAINT catalog_test_finish_failure`.execute(db);
  }
  await service.recover();
  const storedStage13 = await repository.getStage(stage.id);
  expect(storedStage13?.state).toBe('deleted');
});

it('multiple cleanup workers have exactly one deletion claim', async () => {
  const { stage } = await verifiedStage();
  const results = await Promise.all([
    service.cleanup(stage.id),
    new ObjectCatalogService(new ObjectCatalogRepository(db), root).cleanup(stage.id),
  ]);
  expect(results.sort()).toEqual([false, true]);
});

it('database rejects malformed sealed inode evidence and pins', async () => {
  const stage = await service.stage(source);
  await expect(
    db
      .updateTable('staged_object')
      .set({ identity: {} as never })
      .where('id', '=', stage.id)
      .execute(),
  ).rejects.toThrow();
  await expect(
    db
      .updateTable('staged_object')
      .set({ pins: {} as never })
      .where('id', '=', stage.id)
      .execute(),
  ).rejects.toThrow();
});

it('database prevents a version from referring to a different store than its intent', async () => {
  const stage = await service.stage(source);
  const intent = await repository.prepare(stage.id, 's3', 'bucket-1');
  await expect(
    db
      .insertInto('object_version')
      .values({
        id: randomUUID(),
        intentId: intent.id,
        provider: 's3',
        storeId: 'wrong-bucket',
        key: intent.key,
        versionId: 'v1',
      })
      .execute(),
  ).rejects.toThrow();
});

it('reports ambiguous attempts on recovery and retains known late result as evidence without acknowledging', async () => {
  const stage = await service.stage(source);
  const remote = path.join(directory, 'remote');
  await mkdir(remote);
  const store = new LocalObjectStoreRepository(remote);
  const put = store.putFile.bind(store);
  store.putFile = async (...args) => {
    const result = await put(...args);
    const [intent] = await repository.listIntents(stage.id);
    await db
      .updateTable('object_upload_intent')
      .set({ leaseUntil: sql`clock_timestamp() - interval '1 second'` })
      .where('id', '=', intent.id)
      .execute();
    await service.recover();
    return result;
  };
  await expect(service.upload(stage.id, { provider: 'local', storeId: 'local-1', store })).rejects.toThrow();
  const [intent] = await repository.listIntents(stage.id);
  expect(intent.state).toBe('ambiguous');
  expect(intent.evidence).toContain(intent.key);
  expect(await repository.listVersions(stage.id)).toEqual([]);
  const report = await service.recover();
  expect(report.held.map((entry) => entry.stageId)).toContain(stage.id);
});

it('rejects caller sources within the staging root and symlinked source ancestors', async () => {
  const nested = path.join(root, 'caller');
  await writeFile(nested, 'caller-owned');
  await expect(service.stage(nested)).rejects.toThrow();
  expect(await readFile(nested, 'utf8')).toBe('caller-owned');
  const alias = path.join(directory, 'alias');
  await symlink(directory, alias);
  await expect(service.stage(path.join(alias, 'source'))).rejects.toThrow();
});

it('exact S3 version is carried through the full streaming GET and wrong store binding never fetches', async () => {
  const stage = await service.stage(source);
  const store: ObjectStore = {
    putFile: (object) => Promise.resolve({ object: { ...object, versionId: 'exact-v1' } }),
    get: vi.fn((object) =>
      Promise.resolve({ object, stream: Readable.from(['synthetic ', 'catalog bytes']), size: 23 }),
    ),
    delete: vi.fn(),
  };
  const binding = { provider: 's3' as const, storeId: 'stable-bucket-1', store };
  const version = await service.upload(stage.id, binding);
  await expect(service.verify(stage.id, version.id, { ...binding, storeId: 'other-bucket' })).rejects.toThrow(
    'identity',
  );
  expect(store.get).not.toHaveBeenCalled();
  expect(await service.verify(stage.id, version.id, binding)).toBe(true);
  expect(store.get).toHaveBeenCalledWith({ key: version.key, versionId: 'exact-v1' });
  expect(store.delete).not.toHaveBeenCalled();
});

it('reports acknowledged but not independently verified objects as held', async () => {
  const stage = await service.stage(source);
  const remote = path.join(directory, 'remote');
  await mkdir(remote);
  await service.upload(stage.id, {
    provider: 'local',
    storeId: 'local-1',
    store: new LocalObjectStoreRepository(remote),
  });
  const recoveryReport14 = await service.recover();
  expect(recoveryReport14.held.map((entry) => entry.stageId)).toContain(stage.id);
});

it('wrong verification store binding leaves no processing pin because no GET began', async () => {
  const { stage, binding, version } = await verifiedStage();
  await expect(service.verify(stage.id, version.id, { ...binding, storeId: 'wrong' })).rejects.toThrow();
  const storedStage15 = await repository.getStage(stage.id);
  expect(storedStage15?.pins).toEqual([]);
});

it('detects replacement of the private owned file before sealing and retains both files', async () => {
  const sync = service['syncDirectory'].bind(service);
  vi.spyOn(service as unknown as { syncDirectory: () => Promise<void> }, 'syncDirectory').mockImplementation(
    async () => {
      await sync();
      const [owned] = await readdir(root);
      await rename(path.join(root, owned), path.join(root, 'displaced'));
      await writeFile(path.join(root, owned), 'replacement');
    },
  );
  await expect(service.stage(source)).rejects.toThrow();
  const stages = await repository.listStages(root);
  expect(stages[0].state).toBe('quarantined');
  expect(await readFile(path.join(root, 'displaced'), 'utf8')).toBe('synthetic catalog bytes');
  expect(await readFile(path.join(root, stages[0].id), 'utf8')).toBe('replacement');
});

it('resumes only a prepared intent after restart and never creates a second key', async () => {
  const stage = await service.stage(source);
  const intent = await repository.prepare(stage.id, 'local', 'local-1');
  const remote = path.join(directory, 'remote');
  await mkdir(remote);
  const binding = { provider: 'local' as const, storeId: 'local-1', store: new LocalObjectStoreRepository(remote) };
  const restarted = new ObjectCatalogService(new ObjectCatalogRepository(db), root);
  const version = await restarted.sendPrepared(intent.id, binding);
  expect(version.key).toBe(intent.key);
  expect(await repository.listIntents(stage.id)).toHaveLength(1);
  await expect(restarted.sendPrepared(intent.id, binding)).rejects.toThrow();
});

it('records DB-time verification observations and retains history across failed rechecks', async () => {
  const { stage, binding, version } = await verifiedStage();
  binding.store.get = vi.fn<typeof binding.store.get>().mockResolvedValue(undefined);
  expect(await service.verify(stage.id, version.id, binding)).toBe(false);
  const storedVersions16 = await repository.listVersions(stage.id);
  const observations = storedVersions16[0].observations;
  expect(observations.map((observation) => observation.outcome)).toEqual(['verified', 'failed']);
  expect(observations[0].observedAt).toMatch(/^\d{4}-\d{2}-\d{2}T/);
  expect(await service.cleanup(stage.id)).toBe(false);
});

it('concurrent restart recovery after unlink is idempotent and reports reappeared deleted names as unknown', async () => {
  const { stage } = await verifiedStage();
  await repository.claimCleanup(stage.id);
  await unlink(path.join(root, stage.id));
  await Promise.all([service.recover(), new ObjectCatalogService(new ObjectCatalogRepository(db), root).recover()]);
  await writeFile(path.join(root, stage.id), 'unexpected');
  const recoveryReport17 = await service.recover();
  expect(recoveryReport17.unknown).toContain(stage.id);
  expect(await readFile(path.join(root, stage.id), 'utf8')).toBe('unexpected');
});

function coordinatedWorker(action: string, stageId: string) {
  const child = fork('test/catalog/crash-worker.mjs', [action, database, root, source, stageId], {
    execArgv: [],
    env: {
      ...process.env,
      CATALOG_TEST_DISPOSABLE: 'YES',
      CATALOG_TEST_SOCKET: socket!,
      CATALOG_TEST_PORT: String(options.port),
    },
    stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
  });
  const messages = new Map<string, { type: string; ok?: boolean; error?: string }>();
  child.on('message', (message: { type: string; ok?: boolean; error?: string }) => messages.set(message.type, message));
  const exited = once(child, 'exit');
  return {
    child,
    messages,
    async wait(type: string) {
      while (!messages.has(type)) {
        if (child.exitCode !== null || child.signalCode !== null) {
          throw new Error(`Worker exited before ${type}`);
        }
        await new Promise<void>((resolve) => setImmediate(resolve));
      }
      return messages.get(type)!;
    },
    async stop() {
      if (child.exitCode === null && child.signalCode === null) {
        child.kill('SIGKILL');
      }
      await exited;
    },
  };
}

it.each(['cleanup', 'recover'])(
  'serializes %s against recovery before unlink with a real opened descriptor',
  async (first) => {
    const { stage } = await verifiedStage();
    if (first === 'recover') {
      await repository.claimCleanup(stage.id);
    }
    const b = coordinatedWorker(`paused-${first}`, stage.id);
    let a: ReturnType<typeof coordinatedWorker> | undefined;
    try {
      await b.wait('opened');
      a = coordinatedWorker('recover-before-ack', stage.id);
      // On the broken implementation A unlinks while B has the descriptor open.
      // On the fixed implementation PostgreSQL proves A is waiting for B's session lock.
      while (!a.messages.has('unlinked')) {
        const waiting = await sql<{ waiting: boolean }>`SELECT EXISTS (
          SELECT 1 FROM pg_stat_activity WHERE application_name = ${`catalog-recover-before-ack-${stage.id}`}
          AND wait_event = 'advisory'
        ) AS waiting`.execute(db);
        if (waiting.rows[0].waiting) {
          break;
        }
        await new Promise<void>((resolve) => setImmediate(resolve));
      }
      b.child.send('resume');
      const resultB = await b.wait('result');
      if (a.messages.has('unlinked')) {
        a.child.send('resume');
      }
      const resultA = await a.wait('result');
      expect(resultB.error).toBeUndefined();
      expect(resultA.error).toBeUndefined();
      expect(resultB).toMatchObject({ ok: true });
      expect(resultA).toMatchObject({ ok: true });
      const finished = await repository.getStage(stage.id);
      expect(finished?.state).toBe('deleted');
      expect(await readdir(root)).toEqual([]);
    } finally {
      await b.stop();
      await a?.stop();
    }
  },
);

it('SIGKILL releases cleanup coordination for an already waiting recovery process', async () => {
  const { stage } = await verifiedStage();
  const b = coordinatedWorker('paused-cleanup', stage.id);
  let a: ReturnType<typeof coordinatedWorker> | undefined;
  try {
    await b.wait('opened');
    const session = await sql<{ state: string; xact_start: Date | null; locked: boolean }>`
      SELECT state, xact_start, EXISTS (
        SELECT 1 FROM pg_locks WHERE pid = pg_stat_activity.pid AND locktype = 'advisory' AND granted
      ) AS locked FROM pg_stat_activity WHERE application_name = ${`catalog-paused-cleanup-${stage.id}`}
    `.execute(db);
    expect(session.rows).toEqual([{ state: 'idle', xact_start: null, locked: true }]);
    a = coordinatedWorker('recover-before-ack', stage.id);
    while (true) {
      const waiting = await sql<{ waiting: boolean }>`SELECT EXISTS (
        SELECT 1 FROM pg_stat_activity WHERE application_name = ${`catalog-recover-before-ack-${stage.id}`}
        AND wait_event = 'advisory'
      ) AS waiting`.execute(db);
      if (waiting.rows[0].waiting) {
        break;
      }
      await new Promise<void>((resolve) => setImmediate(resolve));
    }
    expect(await readFile(path.join(root, stage.id), 'utf8')).toBe('synthetic catalog bytes');
    await b.stop(); // SIGKILL, no finally/unlock/application cleanup can run.
    await a.wait('unlinked');
    a.child.send('resume');
    expect(await a.wait('result')).toMatchObject({ ok: true });
    const finished = await repository.getStage(stage.id);
    expect(finished?.state).toBe('deleted');
    expect(await readdir(root)).toEqual([]);
  } finally {
    await b.stop();
    await a?.stop();
  }
});

it('cleanup and recovery use one connection without pool deadlock and release locks on failure', async () => {
  const { stage } = await verifiedStage();
  const single = new Kysely<DB>({
    dialect: new PostgresJSDialect({ postgres: postgres({ ...options, database, max: 1 }) }),
  });
  const worker = new ObjectCatalogService(new ObjectCatalogRepository(single), root);
  try {
    await sql`ALTER TABLE staged_object ADD CONSTRAINT catalog_test_lock_finish_failure CHECK (state <> 'deleted') NOT VALID`.execute(
      db,
    );
    try {
      await expect(worker.cleanup(stage.id)).rejects.toThrow();
      const deleting = await repository.getStage(stage.id);
      expect(deleting?.state).toBe('deleting');
      // The session remains live/pooled after the SQL error, but must not retain a lock.
      const locks = await sql<{ count: string }>`SELECT count(*)::text AS count FROM pg_locks
        WHERE locktype = 'advisory' AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`.execute(
        db,
      );
      expect(locks.rows[0].count).toBe('0');
    } finally {
      await sql`ALTER TABLE staged_object DROP CONSTRAINT catalog_test_lock_finish_failure`.execute(db);
    }
    await Promise.all([worker.recover(), worker.cleanup(stage.id)]);
    const finished = await repository.getStage(stage.id);
    expect(finished?.state).toBe('deleted');
    expect(await worker.cleanup(stage.id)).toBe(false);
  } finally {
    await single.destroy();
  }
});

it('recovery quarantines a wrong named inode and releases coordination without removing evidence', async () => {
  const { stage } = await verifiedStage();
  await repository.claimCleanup(stage.id);
  await rename(path.join(root, stage.id), path.join(directory, 'original'));
  await writeFile(path.join(root, stage.id), 'wrong named inode');
  await expect(service.recover()).rejects.toThrow('identity mismatch');
  const quarantined = await repository.getStage(stage.id);
  expect(quarantined?.state).toBe('quarantined');
  expect(await readFile(path.join(root, stage.id), 'utf8')).toBe('wrong named inode');
  expect(await readFile(path.join(directory, 'original'), 'utf8')).toBe('synthetic catalog bytes');
  const locks = await sql<{ count: string }>`SELECT count(*)::text AS count FROM pg_locks
    WHERE locktype = 'advisory' AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`.execute(
    db,
  );
  expect(locks.rows[0].count).toBe('0');
  await service.recover();
  expect(await readFile(path.join(root, stage.id), 'utf8')).toBe('wrong named inode');
});

it('decorated catalog schema registration matches migration defaults and indexes', async () => {
  const code = schemaFromCode({ databaseName: 'immich' });
  const names = new Set(['staged_object', 'object_upload_intent', 'object_version']);
  const tables = code.tables.filter((table) => names.has(table.name));
  expect(tables).toHaveLength(3);
  expect(new ImmichDatabase().tables.map((table) => table.name)).toEqual(
    expect.arrayContaining(['StagedObjectTable', 'ObjectUploadIntentTable', 'ObjectVersionTable']),
  );
  for (const table of tables) {
    for (const column of table.columns) {
      if (!['pins', 'observations'].includes(column.name)) {
        continue;
      }
      expect(column.default).toBe("'[]'::jsonb");
    }
    const actual = await sql<{
      indexname: string;
    }>`SELECT indexname FROM pg_indexes WHERE schemaname = 'public' AND tablename = ${table.name}`.execute(db);
    expect(actual.rows.map((row) => row.indexname)).toEqual(
      expect.arrayContaining(table.indexes.map((index) => index.name)),
    );
  }
});

it('preserves the fully copied stage and caller source when sealing SQL fails', async () => {
  await sql`ALTER TABLE staged_object ADD CONSTRAINT catalog_test_seal_failure CHECK (state <> 'ready') NOT VALID`.execute(
    db,
  );
  try {
    await expect(service.stage(source)).rejects.toThrow();
    const [stage] = await repository.listStages(root);
    expect(stage.state).toBe('quarantined');
    expect(await readFile(path.join(root, stage.id), 'utf8')).toBe('synthetic catalog bytes');
    expect(await readFile(source, 'utf8')).toBe('synthetic catalog bytes');
  } finally {
    await sql`ALTER TABLE staged_object DROP CONSTRAINT catalog_test_seal_failure`.execute(db);
  }
});

it('detects a caller mutation during real streaming copy and quarantines without removing either name', async () => {
  await writeFile(source, Buffer.alloc(8 * 1024 * 1024, 65));
  let settled = false;
  const staging = service.stage(source);
  void staging
    .then(() => {
      settled = true;
    })
    .catch(() => {
      settled = true;
    });
  while (!settled && (await readdir(root).then((entries) => entries.length === 0))) {
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  expect(settled).toBe(false);
  await writeFile(source, 'caller changed source');
  await expect(staging).rejects.toThrow('changed');
  const [stage] = await repository.listStages(root);
  expect(stage.state).toBe('quarantined');
  expect(await readFile(source, 'utf8')).toBe('caller changed source');
  expect(await readdir(root)).toContain(stage.id);
});

it('records lossless ownership and hardlink stat fields alongside inode identity', async () => {
  const stage = await service.stage(source);
  const owned = await stat(path.join(root, stage.id), { bigint: true });
  expect(stage.identity).toMatchObject({
    uid: owned.uid.toString(),
    gid: owned.gid.toString(),
    nlink: owned.nlink.toString(),
  });
});

async function crashWorker(action: string, stageId = '', endpoint = '') {
  await expect(
    promisify(execFile)(
      process.execPath,
      ['test/catalog/crash-worker.mjs', action, database, root, source, stageId, endpoint],
      {
        env: {
          ...process.env,
          CATALOG_TEST_DISPOSABLE: 'YES',
          CATALOG_TEST_SOCKET: socket!,
          CATALOG_TEST_PORT: String(options.port),
        },
      },
    ),
  ).rejects.toMatchObject({ code: 73 });
}

it.each(['creating', 'file-sync', 'sealed-copy'])(
  'subprocess exits at staging %s boundary: restart never adopts incomplete files',
  async (boundary) => {
    await crashWorker(boundary);
    const rows = await repository.listStages(root);
    expect(rows).toHaveLength(1);
    expect(rows[0].state).toBe('creating');
    await service.recover();
    const storedStage18 = await repository.getStage(rows[0].id);
    expect(storedStage18?.state).toBe('quarantined');
    expect(await readFile(source, 'utf8')).toBe('synthetic catalog bytes');
    if (boundary !== 'creating') {
      expect(await readFile(path.join(root, rows[0].id), 'utf8')).toBe('synthetic catalog bytes');
    }
  },
);

it.each(['sending', 'remote-success'])(
  'subprocess exits at upload %s boundary: restart retains ambiguous pins',
  async (boundary) => {
    const stage = await service.stage(source);
    await mkdir(path.join(directory, 'remote'));
    await crashWorker(boundary, stage.id);
    const [intent] = await repository.listIntents(stage.id);
    expect(intent.state).toBe('sending');
    await db
      .updateTable('object_upload_intent')
      .set({ leaseUntil: sql`clock_timestamp() - interval '1 second'` })
      .where('id', '=', intent.id)
      .execute();
    const recoveryReport19 = await service.recover();
    expect(recoveryReport19.held.map((entry) => entry.stageId)).toContain(stage.id);
    const storedIntents20 = await repository.listIntents(stage.id);
    expect(storedIntents20[0].state).toBe('ambiguous');
    await repository.releaseStage(stage.id);
    expect(await service.cleanup(stage.id)).toBe(false);
    expect(await readFile(path.join(root, stage.id), 'utf8')).toBe('synthetic catalog bytes');
    if (boundary !== 'remote-success') {
      return;
    }
    {
      const read = await new LocalObjectStoreRepository(path.join(directory, 'remote')).get({ key: intent.key });
      expect(read).toBeDefined();
      read!.stream.destroy();
    }
  },
);

it.each(['deleting', 'unlinked'])(
  'subprocess exits at cleanup %s boundary: restart completes only the claimed deletion',
  async (boundary) => {
    const { stage } = await verifiedStage();
    await crashWorker(boundary, stage.id);
    const storedStage21 = await repository.getStage(stage.id);
    expect(storedStage21?.state).toBe('deleting');
    await service.recover();
    const storedStage22 = await repository.getStage(stage.id);
    expect(storedStage22?.state).toBe('deleted');
    expect(await readdir(root)).toEqual([]);
  },
);

it('new migration alone can revert and reapply without extensions or the upstream chain', async () => {
  await downMultipart(db);
  await down(db);
  const result = await sql<{
    name: string;
  }>`SELECT tablename AS name FROM pg_tables WHERE schemaname = 'public'`.execute(db);
  expect(result.rows).toEqual([]);
  await up(db);
  await upMultipart(db);
});

async function s3Loopback(stageId: string, partDelayMs = 0) {
  const parts: Buffer[] = [];
  const requests: string[] = [];
  const failures: unknown[] = [];
  let completed = Buffer.alloc(0);
  const server = createServer((request, response) => {
    void (async () => {
      const url = new URL(request.url!, 'http://localhost');
      requests.push(`${request.method} ${url.search}`);
      const [journal] = await repository.listMultipart(stageId);
      if (url.searchParams.has('uploads')) {
        expect(journal.state).toBe('creating');
        response.end(
          '<InitiateMultipartUploadResult><UploadId>loopback-upload</UploadId></InitiateMultipartUploadResult>',
        );
      } else
        switch (request.method) {
          case 'PUT': {
            expect(journal.state).toBe('uploading');
            expect(journal.uploadId).toBe('loopback-upload');
            parts.push(await buffer(request));
            await delay(partDelayMs);
            response.setHeader('etag', `"part-${parts.length}"`);
            response.end();

            break;
          }
          case 'POST': {
            expect(journal.state).toBe('completing');
            expect(journal.manifest).toHaveLength(parts.length);
            await buffer(request);
            completed = Buffer.concat(parts);
            response.setHeader('x-amz-version-id', 'loopback-exact-version');
            response.end(
              '<CompleteMultipartUploadResult><ETag>"opaque-complete"</ETag></CompleteMultipartUploadResult>',
            );

            break;
          }
          case 'DELETE': {
            response.statusCode = 204;
            response.end();

            break;
          }
          case 'GET': {
            expect(url.searchParams.get('versionId')).toBe('loopback-exact-version');
            response.setHeader('x-amz-version-id', 'loopback-exact-version');
            response.setHeader('content-length', completed.length);
            response.end(completed);

            break;
          }
          default: {
            throw new Error('Unexpected loopback operation');
          }
        }
    })().catch((error: unknown) => {
      failures.push(error);
      response.statusCode = 500;
      response.end();
    });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address() as { port: number };
  const client = new S3Client({
    region: 'us-east-1',
    endpoint: `http://127.0.0.1:${address.port}`,
    forcePathStyle: true,
    maxAttempts: 1,
    credentials: { accessKeyId: 'synthetic', secretAccessKey: 'synthetic' },
    requestChecksumCalculation: 'WHEN_REQUIRED',
    responseChecksumValidation: 'WHEN_REQUIRED',
  });
  const partSize = 5 * 1024 ** 2;
  const store = new S3ObjectStoreRepository(client, {
    bucket: 'synthetic-bucket',
    multipart: { threshold: partSize, partSize, maxObjectSize: partSize * 10, cleanupTimeoutMs: 1000 },
  });
  return {
    store,
    requests,
    failures,
    endpoint: `http://127.0.0.1:${address.port}`,
    close: async () => {
      client.destroy();
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

it.each([
  ['mpu-beforeCreate', 'creating', 0],
  ['mpu-create-response', 'creating', 1],
  ['mpu-uploadCreated', 'uploading', 1],
  ['mpu-beforeComplete', 'completing', 3],
  ['mpu-remote-completed', 'received', 4],
] as const)(
  'process crash at %s retains durable %s evidence and never retries or aborts on recovery',
  async (boundary, state, requestCount) => {
    await writeFile(source, Buffer.alloc(5 * 1024 ** 2 + 17, 29));
    const stage = await service.stage(source);
    const fixture = await s3Loopback(stage.id);
    try {
      await crashWorker(boundary, stage.id, fixture.endpoint);
      const [intent] = await repository.listIntents(stage.id);
      const [journal] = await repository.listMultipart(stage.id);
      expect(journal.state).toBe(state);
      expect(journal.uploadId).toBe(state === 'creating' ? null : 'loopback-upload');
      if (state === 'received') {
        expect(journal.response?.versionId).toBe('loopback-exact-version');
      }
      expect(await repository.listVersions(stage.id)).toEqual([]);
      await db
        .updateTable('object_upload_intent')
        .set({ leaseUntil: sql`clock_timestamp() - interval '1 second'` })
        .where('id', '=', intent.id)
        .execute();
      const report = await new ObjectCatalogService(new ObjectCatalogRepository(db), root).recover();
      expect(report.held.find((entry) => entry.stageId === stage.id)?.multipart).toEqual([journal]);
      await repository.releaseStage(stage.id);
      expect(await service.cleanup(stage.id)).toBe(false);
      expect(fixture.requests).toHaveLength(requestCount);
      expect(fixture.failures).toEqual([]);
      const multipartObservation14 = await readFile(path.join(root, stage.id));
      expect(multipartObservation14.length).toBe(Number(stage.size));
    } finally {
      await fixture.close();
    }
  },
);

it('cancels the provider cooperatively on a stuck renewal, awaits it, and retains the stage hold', async () => {
  await writeFile(source, Buffer.alloc(5 * 1024 ** 2 + 17, 29));
  const stage = await service.stage(source);
  const fixture = await s3Loopback(stage.id, 450);
  let settled = false;
  const renew = vi.spyOn(repository, 'renewSending').mockImplementation(async () => {
    await delay(650);
    settled = true;
    return false;
  });
  try {
    await expect(
      service.upload(stage.id, { provider: 's3', storeId: 'physical-loopback-1', store: fixture.store }, 300),
    ).rejects.toThrow();
    expect(settled).toBe(true);
    expect(fixture.requests.filter((request) => request.startsWith('PUT'))).toHaveLength(1);
    const multipartObservation13 = await repository.listIntents(stage.id);
    expect(multipartObservation13[0].state).toBe('ambiguous');
    const multipartObservation12 = await repository.listMultipart(stage.id);
    expect(multipartObservation12[0].evidence.some((entry) => entry.phase === 'abortResult')).toBe(true);
    await repository.releaseStage(stage.id);
    expect(await service.cleanup(stage.id)).toBe(false);
    const calls = renew.mock.calls.length;
    await delay(350);
    expect(renew).toHaveBeenCalledTimes(calls);
  } finally {
    renew.mockRestore();
    await fixture.close();
  }
});

it('renews the fenced DB lease throughout a long SDK write and settles renewal before returning', async () => {
  await writeFile(source, Buffer.alloc(5 * 1024 ** 2 + 17, 29));
  const stage = await service.stage(source);
  const fixture = await s3Loopback(stage.id, 450);
  const renew = vi.spyOn(repository, 'renewSending');
  try {
    const version = await service.upload(
      stage.id,
      { provider: 's3', storeId: 'physical-loopback-1', store: fixture.store },
      300,
    );
    expect(version.versionId).toBe('loopback-exact-version');
    expect(renew.mock.calls.length).toBeGreaterThan(2);
    const calls = renew.mock.calls.length;
    await delay(350);
    expect(renew).toHaveBeenCalledTimes(calls);
    expect(fixture.failures).toEqual([]);
  } finally {
    renew.mockRestore();
    await fixture.close();
  }
});

it('integrates actual SDK multipart checkpoints with durable catalog and independent full GET before cleanup', async () => {
  await writeFile(source, Buffer.alloc(5 * 1024 ** 2 + 17, 29));
  const stage = await service.stage(source);
  const fixture = await s3Loopback(stage.id);
  try {
    const binding = { provider: 's3' as const, storeId: 'physical-loopback-1', store: fixture.store };
    const version = await service.upload(stage.id, binding);
    expect(version.versionId).toBe('loopback-exact-version');
    const multipartObservation11 = await repository.listMultipart(stage.id);
    expect(multipartObservation11[0].response).toEqual({
      versionId: version.versionId,
      etag: version.etag,
    });
    await repository.releaseStage(stage.id);
    expect(await service.cleanup(stage.id)).toBe(false);
    expect(await service.verify(stage.id, version.id, binding)).toBe(true);
    expect(await service.cleanup(stage.id)).toBe(true);
    expect(fixture.failures).toEqual([]);
    expect(fixture.requests).toHaveLength(5);
  } finally {
    await fixture.close();
  }
});

it.each(['creating', 'uploading', 'received', 'acknowledge'] as const)(
  'real DB failure at %s preserves durable holds and available response evidence',
  async (boundary) => {
    await writeFile(source, Buffer.alloc(5 * 1024 ** 2 + 17, 29));
    const stage = await service.stage(source);
    const fixture = await s3Loopback(stage.id);
    if (boundary === 'acknowledge') {
      await sql`ALTER TABLE object_version ADD CONSTRAINT multipart_test_fail CHECK (false) NOT VALID`.execute(db);
    } else {
      await sql`ALTER TABLE s3_multipart_journal ADD CONSTRAINT multipart_test_fail CHECK (state <> ${sql.lit(boundary)}) NOT VALID`.execute(
        db,
      );
    }
    try {
      await expect(
        service.upload(stage.id, { provider: 's3', storeId: 'physical-loopback-1', store: fixture.store }),
      ).rejects.toThrow();
      const [intent] = await repository.listIntents(stage.id);
      expect(intent.state).toBe('ambiguous');
      expect(await repository.listVersions(stage.id)).toEqual([]);
      const [journal] = await repository.listMultipart(stage.id);
      if (boundary === 'creating') {
        expect(journal).toBeUndefined();
        expect(fixture.requests).toHaveLength(0);
      } else if (boundary === 'uploading') {
        expect(journal.state).toBe('creating');
        expect(
          journal.evidence.some((entry) => entry.phase === 'abortResult' && entry.uploadId === 'loopback-upload'),
        ).toBe(true);
        expect(fixture.requests.some((entry) => entry.startsWith('PUT'))).toBe(false);
      } else {
        expect(journal.state).toBe(boundary === 'received' ? 'completing' : 'received');
        expect(journal.response?.versionId).toBe('loopback-exact-version');
        expect(fixture.requests.some((entry) => entry.startsWith('DELETE'))).toBe(false);
      }
      await repository.releaseStage(stage.id);
      expect(await service.cleanup(stage.id)).toBe(false);
      const multipartObservation10 = await readFile(path.join(root, stage.id));
      expect(multipartObservation10.length).toBe(Number(stage.size));
      expect(fixture.failures).toEqual([]);
    } finally {
      if (boundary === 'acknowledge') {
        await sql`ALTER TABLE object_version DROP CONSTRAINT multipart_test_fail`.execute(db);
      } else {
        await sql`ALTER TABLE s3_multipart_journal DROP CONSTRAINT multipart_test_fail`.execute(db);
      }
      await fixture.close();
    }
  },
);

it.each(['bucket', 'key', 'token', 'storeId', 'intentId'] as const)(
  'fences mismatched multipart %s binding before journal effects',
  async (field) => {
    const { stage, binding, base } = await multipartClaim();
    const wrong = { ...binding, [field]: ['token', 'intentId'].includes(field) ? randomUUID() : 'wrong' };
    await expect(
      repository.recordMultipart(wrong, { ...base, phase: 'uploadCreated', uploadId: 'u' }),
    ).rejects.toThrow();
    const multipartObservation9 = await repository.listMultipart(stage.id);
    expect(multipartObservation9[0].state).toBe('creating');
  },
);

it('rejects event replays, misbound upload IDs and invalid ordered manifests', async () => {
  const { stage, binding, base } = await multipartClaim();
  await repository.recordMultipart(binding, { ...base, phase: 'uploadCreated', uploadId: 'u' });
  await expect(
    repository.recordMultipart(binding, { ...base, phase: 'uploadCreated', uploadId: 'u' }),
  ).rejects.toThrow();
  await expect(
    repository.recordMultipart(binding, {
      ...base,
      phase: 'beforeComplete',
      uploadId: 'other',
      parts: [{ PartNumber: 1, ETag: 'p' }],
    }),
  ).rejects.toThrow();
  for (const parts of [[], [{ PartNumber: 2, ETag: 'p' }], [{ PartNumber: 1, ETag: '' }]]) {
    await expect(
      repository.recordMultipart(binding, { ...base, phase: 'beforeComplete', uploadId: 'u', parts }),
    ).rejects.toThrow();
  }
  await expect(
    repository.recordMultipart(binding, { ...base, phase: 'receivedResult', uploadId: 'u', versionId: 'v', etag: 'e' }),
  ).rejects.toThrow();
  const multipartObservation8 = await repository.listMultipart(stage.id);
  expect(multipartObservation8[0].state).toBe('uploading');
});

it.each([
  ['local', 'replace'],
  ['local', 'mutate'],
  ['s3', 'replace'],
  ['s3', 'mutate'],
] as const)(
  'rejects %s %s in the validation to provider-open gap on the provider owned descriptor',
  async (provider, kind) => {
    if (provider === 's3') {
      await writeFile(source, Buffer.alloc(5 * 1024 ** 2 + 17, 29));
    }
    const stage = await service.stage(source);
    const remote = path.join(directory, 'remote');
    await mkdir(remote);
    const fixture = provider === 's3' ? await s3Loopback(stage.id) : undefined;
    const store = fixture?.store ?? new LocalObjectStoreRepository(remote);
    const put = store.putFile.bind(store);
    const bytes = await readFile(path.join(root, stage.id));
    store.putFile = async (...args: Parameters<ObjectStore['putFile']>) => {
      if (kind === 'replace') {
        await rename(args[1], path.join(directory, 'old-inode'));
        await writeFile(args[1], bytes, { mode: 0o400 });
      } else {
        await chmod(args[1], 0o600);
        await writeFile(args[1], bytes);
      }
      return put(...args);
    };
    try {
      await expect(service.upload(stage.id, { provider, storeId: 'physical-1', store })).rejects.toThrow('identity');
      expect(await readdir(remote)).toEqual([]);
      expect(fixture?.requests ?? []).toEqual([]);
      const [intent] = await repository.listIntents(stage.id);
      expect(intent.state).toBe('ambiguous');
    } finally {
      await fixture?.close();
    }
  },
);

async function multipartClaim() {
  const stage = await service.stage(source);
  const intent = await repository.prepare(stage.id, 's3', 'physical-store-1');
  const claim = await repository.startSending(intent.id, 60_000);
  const binding = {
    intentId: intent.id,
    storeId: intent.storeId,
    bucket: 'synthetic-bucket',
    key: intent.key,
    token: claim!.token!,
  };
  const base = { bucket: binding.bucket, key: binding.key, attemptToken: binding.token };
  await repository.recordMultipart(binding, {
    ...base,
    phase: 'beforeCreate',
    size: Number(stage.size),
    expectedHash: { algorithm: 'sha256', hex: stage.hash! },
    partSize: 5 * 1024 ** 2,
    partCount: 1,
  });
  return { stage, intent, binding, base };
}

it('rolls back acknowledgement if the lease expires during version insertion', async () => {
  const stage = await service.stage(source);
  const intent = await repository.prepare(stage.id, 'local', 'physical-1');
  const claim = await repository.startSending(intent.id, 150);
  await sql`CREATE FUNCTION catalog_test_slow_version() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.3); RETURN NEW; END $$`.execute(
    db,
  );
  await sql`CREATE TRIGGER catalog_test_slow_version BEFORE INSERT ON object_version FOR EACH ROW EXECUTE FUNCTION catalog_test_slow_version()`.execute(
    db,
  );
  try {
    await expect(repository.acknowledge(intent.id, claim!.token!, { object: { key: intent.key } })).rejects.toThrow();
    expect(await repository.listVersions(stage.id)).toEqual([]);
    const held = await repository.getIntent(intent.id);
    expect(held.state).toBe('sending');
  } finally {
    await sql`DROP TRIGGER catalog_test_slow_version ON object_version`.execute(db);
    await sql`DROP FUNCTION catalog_test_slow_version()`.execute(db);
  }
});

it('fences a forward journal transition after waiting past lease expiry on a row lock', async () => {
  const { intent, binding, base, stage } = await multipartClaim();
  await db
    .updateTable('object_upload_intent')
    .set({ leaseUntil: sql`clock_timestamp() + interval '100 milliseconds'` })
    .where('id', '=', intent.id)
    .execute();
  let pending: Promise<unknown>;
  await db.transaction().execute(async (tx) => {
    await tx.selectFrom('object_upload_intent').selectAll().where('id', '=', intent.id).forUpdate().execute();
    pending = repository
      .recordMultipart(binding, { ...base, phase: 'uploadCreated', uploadId: 'late' })
      .then(() => 'advanced')
      .catch(() => 'fenced');
    await delay(300);
  });
  expect(await pending!).toBe('fenced');
  const multipartObservation6 = await repository.listMultipart(stage.id);
  expect(multipartObservation6[0].state).toBe('creating');
});

it('never renews a claim that expired while waiting for a row lock', async () => {
  const { intent, binding } = await multipartClaim();
  await db
    .updateTable('object_upload_intent')
    .set({ leaseUntil: sql`clock_timestamp() + interval '100 milliseconds'` })
    .where('id', '=', intent.id)
    .execute();
  let pending: Promise<boolean>;
  await db.transaction().execute(async (tx) => {
    await tx.selectFrom('object_upload_intent').selectAll().where('id', '=', intent.id).forUpdate().execute();
    pending = repository.renewSending(intent.id, binding.token, 60_000);
    await delay(300);
  });
  expect(await pending!).toBe(false);
});

it('renews only the live exact claim using database time and never revives an expired token', async () => {
  const { intent, binding } = await multipartClaim();
  const first = await repository.getIntent(intent.id);
  expect(await repository.renewSending(intent.id, randomUUID(), 60_000)).toBe(false);
  expect(await repository.renewSending(intent.id, binding.token, 60_000)).toBe(true);
  const multipartObservation5 = await repository.getIntent(intent.id);
  expect(new Date(multipartObservation5.leaseUntil!).getTime()).toBeGreaterThanOrEqual(
    new Date(first.leaseUntil!).getTime(),
  );
  await db
    .updateTable('object_upload_intent')
    .set({ leaseUntil: sql`clock_timestamp() - interval '1 second'` })
    .where('id', '=', intent.id)
    .execute();
  expect(await repository.renewSending(intent.id, binding.token, 60_000)).toBe(false);
});

it('database constrains journal state shape and restricts the exact claim identity', async () => {
  const { intent, stage } = await multipartClaim();
  await expect(
    db
      .updateTable('s3_multipart_journal')
      .set({ state: 'received', uploadId: 'u' })
      .where('intentId', '=', intent.id)
      .execute(),
  ).rejects.toThrow();
  await expect(
    db.updateTable('s3_multipart_journal').set({ token: randomUUID() }).where('intentId', '=', intent.id).execute(),
  ).rejects.toThrow();
  await expect(db.deleteFrom('object_upload_intent').where('id', '=', intent.id).execute()).rejects.toThrow();
  const code = schemaFromCode({ databaseName: 'immich' }).tables.find(
    (table) => table.name === 's3_multipart_journal',
  )!;
  expect(new ImmichDatabase().tables.map((table) => table.name)).toContain('S3MultipartJournalTable');
  const indexes = await sql<{
    indexname: string;
  }>`SELECT indexname FROM pg_indexes WHERE tablename = 's3_multipart_journal'`.execute(db);
  expect(indexes.rows.map((row) => row.indexname)).toEqual(
    expect.arrayContaining(code.indexes.map((index) => index.name)),
  );
  const evidenceColumns = code.columns.filter((column) => ['manifest', 'evidence'].includes(column.name));
  for (const column of evidenceColumns) {
    expect(column.default).toBe("'[]'::jsonb");
  }
  const multipartObservation4 = await repository.listMultipart(stage.id);
  expect(multipartObservation4[0].state).toBe('creating');
});

it('bounds accumulated secondary evidence even with large opaque known upload identifiers', async () => {
  const { stage, binding, base } = await multipartClaim();
  const uploadId = 'u'.repeat(4096);
  for (let index = 0; index < 40; index++) {
    await repository.recordMultipart(binding, {
      ...base,
      phase: 'abortResult',
      uploadId,
      outcome: { ok: false, reason: new Error('SECRET'.repeat(10_000)) },
    });
  }
  const [journal] = await repository.listMultipart(stage.id);
  expect(journal.evidence.length).toBeLessThanOrEqual(32);
  expect(Buffer.byteLength(JSON.stringify(journal.evidence))).toBeLessThanOrEqual(30_000);
  expect(journal.evidence[0].uploadId).toBe(uploadId);
  expect(JSON.stringify(journal.evidence)).not.toContain('SECRET');
});

it('rejects failure evidence for impossible event state without corrupting durable outcome history', async () => {
  const { stage, binding, base } = await multipartClaim();
  await repository.recordMultipart(binding, { ...base, phase: 'uploadCreated', uploadId: 'known-id' });
  await expect(
    repository.recordMultipart(binding, {
      ...base,
      phase: 'failure',
      at: 'complete',
      uploadId: 'known-id',
      completeDispatched: true,
      reason: 'bad',
      secondary: [],
    }),
  ).rejects.toThrow();
  const multipartObservation3 = await repository.listMultipart(stage.id);
  expect(multipartObservation3[0].evidence).toEqual([]);
});

it('retains bounded sanitized provider failure and abort observations after lease loss without advancing or asserting absence', async () => {
  const { stage, intent, binding, base } = await multipartClaim();
  await repository.recordMultipart(binding, { ...base, phase: 'uploadCreated', uploadId: 'known-id' });
  await db
    .updateTable('object_upload_intent')
    .set({ leaseUntil: sql`clock_timestamp() - interval '1 second'`, evidence: 'prior outcome' })
    .where('id', '=', intent.id)
    .execute();
  await service.recover();
  await repository.recordMultipart(binding, {
    ...base,
    phase: 'abortResult',
    uploadId: 'known-id',
    outcome: { ok: false, reason: new Error('SECRET'.repeat(10_000)) },
  });
  await repository.recordMultipart(binding, {
    ...base,
    phase: 'failure',
    at: 'parts',
    uploadId: 'known-id',
    completeDispatched: false,
    reason: new Error('SECRET'),
    secondary: [new Error('SECRET')],
  });
  const [journal] = await repository.listMultipart(stage.id);
  expect(journal.state).toBe('uploading');
  expect(journal.evidence).toHaveLength(2);
  expect(JSON.stringify(journal.evidence)).not.toContain('SECRET');
  expect(JSON.stringify(journal.evidence)).not.toContain('absent');
  const multipartObservation2 = await repository.getIntent(intent.id);
  expect(multipartObservation2.evidence).toContain('prior outcome');
  await expect(
    repository.recordMultipart(binding, {
      ...base,
      phase: 'beforeComplete',
      uploadId: 'known-id',
      parts: [{ PartNumber: 1, ETag: 'p' }],
    }),
  ).rejects.toThrow();
});

it('journals ordered completion and exact response before atomic acknowledgement', async () => {
  const { stage, binding, base } = await multipartClaim();
  await repository.recordMultipart(binding, { ...base, phase: 'uploadCreated', uploadId: 'opaque-upload' });
  await expect(
    repository.acknowledge(binding.intentId, binding.token, {
      object: { key: binding.key, versionId: 'v1' },
      etag: 'etag',
    }),
  ).rejects.toThrow();
  await repository.recordMultipart(binding, {
    ...base,
    phase: 'beforeComplete',
    uploadId: 'opaque-upload',
    parts: [{ PartNumber: 1, ETag: 'opaque-part' }],
  });
  await repository.recordMultipart(binding, {
    ...base,
    phase: 'receivedResult',
    uploadId: 'opaque-upload',
    versionId: 'v1',
    etag: 'etag',
  });
  await expect(
    repository.acknowledge(binding.intentId, binding.token, {
      object: { key: binding.key, versionId: 'other' },
      etag: 'etag',
    }),
  ).rejects.toThrow();
  const version = await repository.acknowledge(binding.intentId, binding.token, {
    object: { key: binding.key, versionId: 'v1' },
    etag: 'etag',
  });
  expect(version.versionId).toBe('v1');
  const [journal] = await repository.listMultipart(stage.id);
  expect(journal.state).toBe('received');
  expect(journal.manifest).toEqual([{ PartNumber: 1, ETag: 'opaque-part' }]);
  expect(journal.response).toEqual({ versionId: 'v1', etag: 'etag' });
  await repository.releaseStage(stage.id);
  expect(await service.cleanup(stage.id)).toBe(false);
});

it('durably journals creating bound to a live exact S3 claim before effects', async () => {
  const stage = await service.stage(source);
  const intent = await repository.prepare(stage.id, 's3', 'physical-store-1');
  const claim = await repository.startSending(intent.id, 60_000);
  const binding = {
    intentId: intent.id,
    storeId: intent.storeId,
    bucket: 'synthetic-bucket',
    key: intent.key,
    token: claim!.token!,
  };
  await repository.recordMultipart(binding, {
    bucket: binding.bucket,
    key: intent.key,
    attemptToken: binding.token,
    phase: 'beforeCreate',
    size: Number(stage.size),
    expectedHash: { algorithm: 'sha256', hex: stage.hash! },
    partSize: 5 * 1024 ** 2,
    partCount: 1,
  });
  const [journal] = await repository.listMultipart(stage.id);
  expect(journal.state).toBe('creating');
  expect(journal.uploadId).toBeNull();
  expect(journal.token).toBe(claim!.token);
  const multipartObservation1 = await service.recover();
  expect(multipartObservation1.held.find((entry) => entry.stageId === stage.id)?.multipart).toEqual([journal]);
});
