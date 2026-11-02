import { Kysely } from 'kysely';
import fs from 'node:fs/promises';
import { syncBuiltinESMExports } from 'node:module';
import { once } from 'node:events';
import { PostgresJSDialect } from 'kysely-postgres-js';
import os from 'node:os';
import path from 'node:path';
import postgres from 'postgres';
import { LocalObjectStoreRepository } from '../../dist/repositories/local-object-store.repository.js';
import { ObjectCatalogRepository } from '../../dist/repositories/object-catalog.repository.js';
import { ObjectCatalogService } from '../../dist/services/object-catalog.service.js';

const [action, database, root, source, stageId] = process.argv.slice(2);
if (
  process.env.CATALOG_TEST_DISPOSABLE !== 'YES' ||
  !/^immich_catalog_test_[a-f0-9]{32}$/.test(database) ||
  !path.isAbsolute(process.env.CATALOG_TEST_SOCKET || '') ||
  !path.isAbsolute(root)
) {
  throw new Error('Disposable catalog subprocess guard rejected configuration');
}
const db = new Kysely({
  dialect: new PostgresJSDialect({
    postgres: postgres({
      host: process.env.CATALOG_TEST_SOCKET,
      port: Number(process.env.CATALOG_TEST_PORT),
      user: os.userInfo().username,
      database,
      connection: { application_name: `catalog-${action}-${stageId}` },
    }),
  }),
});
const catalog = new ObjectCatalogRepository(db);
const service = new ObjectCatalogService(catalog, root);
const exit = () => process.exit(73);
if (['paused-cleanup', 'paused-recover', 'recover-before-ack'].includes(action)) {
  const pause = async (type) => {
    const resume = once(process, 'message');
    process.send({ type });
    await resume;
  };
  if (action === 'recover-before-ack') {
    const sync = service.syncDirectory.bind(service);
    service.syncDirectory = async () => {
      await pause('unlinked');
      await sync();
    };
  } else {
    const open = fs.open;
    fs.open = async (...args) => {
      const file = await open(...args);
      if (args[0] === path.join(root, stageId)) {
        // Actual descriptor exists, but validateStage has not yet fstat'd it.
        await pause('opened');
      }
      return file;
    };
    syncBuiltinESMExports();
  }
  try {
    const result = action === 'paused-cleanup' ? await service.cleanup(stageId) : await service.recover();
    process.send({ type: 'result', ok: true, result });
  } catch (error) {
    process.send({ type: 'result', ok: false, error: String(error) });
  }
  await db.destroy();
  process.disconnect();
  process.exit(0);
}
if (action === 'creating') {
  const create = catalog.createStage.bind(catalog);
  catalog.createStage = async (...args) => {
    await create(...args);
    exit();
  };
} else if (action === 'file-sync') {
  service.syncDirectory = exit;
} else if (action === 'sealed-copy') {
  catalog.sealStage = exit;
} else if (action === 'deleting') {
  const claim = ObjectCatalogRepository.prototype.claimCleanup;
  ObjectCatalogRepository.prototype.claimCleanup = async function (...args) {
    await claim.apply(this, args);
    exit();
  };
} else if (action === 'unlinked') {
  ObjectCatalogRepository.prototype.finishCleanup = exit;
}
if (['creating', 'file-sync', 'sealed-copy'].includes(action)) {
  await service.stage(source);
} else if (['sending', 'remote-success'].includes(action)) {
  const store = new LocalObjectStoreRepository(path.join(path.dirname(root), 'remote'));
  if (action === 'sending') {
    store.putFile = exit;
  } else {
    catalog.acknowledge = exit;
  }
  await service.upload(stageId, { provider: 'local', storeId: 'local-1', store });
} else if (['deleting', 'unlinked'].includes(action)) {
  await service.cleanup(stageId);
} else {
  throw new Error('Unknown crash boundary');
}
await db.destroy();
throw new Error('Crash boundary was not reached');
