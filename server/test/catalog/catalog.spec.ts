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
import os from 'node:os';
import path from 'node:path';
import { Readable } from 'node:stream';
import { promisify } from 'node:util';
import postgres from 'postgres';
import { LocalObjectStoreRepository } from 'src/repositories/local-object-store.repository.js';
import { ObjectCatalogRepository } from 'src/repositories/object-catalog.repository.js';
import { ObjectStore } from 'src/repositories/object-store.repository.js';
import { DB, ImmichDatabase } from 'src/schema/index.js';
import { down, up } from 'src/schema/migrations/1793500000000-AddObjectCatalog.js';
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

async function crashWorker(action: string, stageId = '') {
  await expect(
    promisify(execFile)(process.execPath, ['test/catalog/crash-worker.mjs', action, database, root, source, stageId], {
      env: {
        ...process.env,
        CATALOG_TEST_DISPOSABLE: 'YES',
        CATALOG_TEST_SOCKET: socket!,
        CATALOG_TEST_PORT: String(options.port),
      },
    }),
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
  await down(db);
  const result = await sql<{
    name: string;
  }>`SELECT tablename AS name FROM pg_tables WHERE schemaname = 'public'`.execute(db);
  expect(result.rows).toEqual([]);
  await up(db);
});
