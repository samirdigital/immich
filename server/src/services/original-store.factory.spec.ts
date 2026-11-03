import { S3Client } from '@aws-sdk/client-s3';
import { Test } from '@nestjs/testing';
import { chmod, mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { AssetOriginalRepository } from 'src/repositories/asset-original.repository.js';
import { OriginalReader, OriginalStoreRegistry } from 'src/services/original-reader.js';
import { ORIGINAL_STORES, createOriginalStores, originalProviders } from 'src/services/original-store.factory.js';

const registerStore = vi.fn<AssetOriginalRepository['registerStore']>();
const repository = { registerStore } as unknown as AssetOriginalRepository;
const destroy = vi.fn();
const factory = vi.fn(() => ({ destroy }) as unknown as S3Client);
let root: string;
beforeEach(async () => {
  root = await mkdtemp(path.join(os.tmpdir(), 'original-store-'));
  factory.mockClear();
  registerStore.mockReset();
  destroy.mockClear();
});
afterEach(async () => {
  await rm(root, { recursive: true, force: true });
});
const enabled = () => ({
  IMMICH_OBJECT_READ_ENABLED: 'true',
  IMMICH_OBJECT_NEW_UPLOAD_ENABLED: 'false',
  IMMICH_OBJECT_STORE_ID: 'immutable-fixture',
  IMMICH_OBJECT_ENDPOINT: 'https://s3.example.invalid',
  IMMICH_OBJECT_REGION: 'fixture-region',
  IMMICH_OBJECT_BUCKET: 'fixture-bucket',
  IMMICH_OBJECT_PROCESSING_ROOT: root,
  IMMICH_OBJECT_ACCESS_KEY_ID: 'synthetic-key',
  IMMICH_OBJECT_SECRET_ACCESS_KEY: 'synthetic-secret',
});
it('defaults to disabled without credentials, filesystem or S3 client construction', async () => {
  const stores = await createOriginalStores({}, repository, factory);
  const registry: OriginalStoreRegistry = stores;
  expect(registry.get('any')).toBeUndefined();
  stores.onModuleDestroy();
  expect(factory).not.toHaveBeenCalled();
  expect(registerStore).not.toHaveBeenCalled();
});
it('Nest resolves the real provider graph and shuts down the owned factory client', async () => {
  const module = await Test.createTestingModule({ providers: originalProviders })
    .overrideProvider(AssetOriginalRepository)
    .useValue(repository)
    .overrideProvider(ORIGINAL_STORES)
    .useFactory({ factory: () => createOriginalStores(enabled(), repository, factory) })
    .compile();
  expect(module.get(OriginalReader)).toBeInstanceOf(OriginalReader);
  expect(module.get<OriginalStoreRegistry>(ORIGINAL_STORES).get('immutable-fixture')).toBeDefined();
  await module.close();
  expect(destroy).toHaveBeenCalledTimes(1);
});
it('enabled fails closed with sanitized configuration errors and no ambient chain', async () => {
  await expect(createOriginalStores({ IMMICH_OBJECT_READ_ENABLED: 'true' }, repository, factory)).rejects.toThrow(
    'Invalid original object store configuration',
  );
  expect(factory).not.toHaveBeenCalled();
});
it('binds explicit validated identity, works with new uploads disabled, shuts down owned client once', async () => {
  const env = enabled();
  const stores = await createOriginalStores(env, repository, factory);
  env.IMMICH_OBJECT_STORE_ID = 'changed';
  expect(stores.get('immutable-fixture')).toBeDefined();
  expect(stores.get('changed')).toBeUndefined();
  expect(registerStore).toHaveBeenCalledWith({
    storeId: 'immutable-fixture',
    endpoint: 'https://s3.example.invalid',
    bucket: 'fixture-bucket',
  });
  expect(factory).toHaveBeenCalledWith(
    expect.objectContaining({
      endpoint: 'https://s3.example.invalid',
      credentials: { accessKeyId: 'synthetic-key', secretAccessKey: 'synthetic-secret' },
    }),
  );
  stores.onModuleDestroy();
  stores.onModuleDestroy();
  expect(destroy).toHaveBeenCalledTimes(1);
  expect(stores.get('immutable-fixture')).toBeUndefined();
});
it.each(['root', 'endpoint', 'bucket', 'credentials', 'identity'])(
  'rejects invalid %s before client construction',
  async (kind) => {
    const env = enabled();
    switch (kind) {
      case 'root': {
        await chmod(root, 0o755);

        break;
      }
      case 'endpoint': {
        env.IMMICH_OBJECT_ENDPOINT = 'https://user:secret@example.invalid/private?bad=true';

        break;
      }
      case 'bucket': {
        env.IMMICH_OBJECT_BUCKET = '../bad';

        break;
      }
      case 'credentials': {
        env.IMMICH_OBJECT_SECRET_ACCESS_KEY = '';

        break;
      }
      case 'identity': {
        registerStore.mockRejectedValue(new Error('identity was already registered elsewhere'));

        break;
      }
      // No default
    }
    await expect(createOriginalStores(env, repository, factory)).rejects.toThrow(
      'Invalid original object store configuration',
    );
    expect(factory).not.toHaveBeenCalled();
  },
);
