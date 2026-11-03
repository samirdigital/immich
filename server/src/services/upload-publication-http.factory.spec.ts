import { Test } from '@nestjs/testing';
import { Kysely } from 'kysely';
import { KYSELY_MODULE_CONNECTION_TOKEN } from 'nestjs-kysely';
import { chmod, mkdir, mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { ObjectStore } from 'src/repositories/object-store.repository.js';
import { DB } from 'src/schema/index.js';
import { AssetMediaService } from 'src/services/asset-media.service.js';
import { BASE_SERVICE_DEPENDENCIES } from 'src/services/base.service.js';
import { ORIGINAL_STORES, originalProviders } from 'src/services/original-store.factory.js';
import { UploadPublicationHttpAdapter } from 'src/services/upload-publication-http.adapter.js';
import { createManagedUpload } from 'src/services/upload-publication-http.factory.js';

it('leaves managed uploads disabled without inspecting roots, registry or SQL', async () => {
  expect(
    await createManagedUpload({}, {} as Kysely<DB>, {
      get: () => {
        throw new Error('must not inspect');
      },
    }),
  ).toBeUndefined();
});

it('rejects enabled uploads without a reviewed explicit read binding or private roots without exposing configuration', async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'managed-config-'));
  try {
    await mkdir(path.join(root, 'catalog'), { mode: 0o700 });
    const env = {
      IMMICH_OBJECT_UPLOAD_ENABLED: 'true',
      IMMICH_OBJECT_SOURCE_ROOT: root,
      IMMICH_OBJECT_CATALOG_ROOT: path.join(root, 'catalog'),
      IMMICH_OBJECT_SECRET_ACCESS_KEY: 'must-never-appear',
    };
    await expect(createManagedUpload(env, {} as Kysely<DB>, { get: () => {} })).rejects.toThrow(
      'Invalid managed upload configuration',
    );
    await expect(
      createManagedUpload({ ...env, IMMICH_OBJECT_UPLOAD_ENABLED: 'oops' }, {} as Kysely<DB>, { get: () => {} }),
    ).rejects.toThrow('Invalid managed upload configuration');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

it('validates both pre-provisioned roots with an explicit reviewed binding and performs no provider calls', async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'managed-config-'));
  const catalog = path.join(root, 'catalog');
  await mkdir(catalog, { mode: 0o700 });
  const store = {} as ObjectStore;
  const registry = {
    get: () => store,
    uploadBinding: {
      provider: 's3' as const,
      storeId: 'fixed',
      endpoint: 'https://example.invalid',
      bucket: 'fixture',
      store,
    },
  };
  const env = {
    IMMICH_OBJECT_UPLOAD_ENABLED: 'true',
    IMMICH_OBJECT_SOURCE_ROOT: root,
    IMMICH_OBJECT_CATALOG_ROOT: catalog,
  };
  try {
    expect(await createManagedUpload(env, {} as Kysely<DB>, registry)).toBeDefined();
    await chmod(catalog, 0o755);
    await expect(createManagedUpload(env, {} as Kysely<DB>, registry)).rejects.toThrow(
      'Invalid managed upload configuration',
    );
    await chmod(catalog, 0o700);
    await chmod(root, 0o755);
    await expect(createManagedUpload(env, {} as Kysely<DB>, registry)).rejects.toThrow(
      'Invalid managed upload configuration',
    );
    await expect(
      createManagedUpload({ ...env, IMMICH_OBJECT_SOURCE_ROOT: '/not-provisioned' }, {} as Kysely<DB>, registry),
    ).rejects.toThrow('Invalid managed upload configuration');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

it('resolves the enabled real factory and AssetMediaService optional property injection without changing BaseService construction', async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'managed-di-'));
  const catalog = path.join(root, 'catalog');
  await mkdir(catalog, { mode: 0o700 });
  const store = {} as ObjectStore;
  vi.stubEnv('IMMICH_OBJECT_UPLOAD_ENABLED', 'true');
  vi.stubEnv('IMMICH_OBJECT_SOURCE_ROOT', root);
  vi.stubEnv('IMMICH_OBJECT_CATALOG_ROOT', catalog);
  try {
    const module = await Test.createTestingModule({
      providers: [
        ...BASE_SERVICE_DEPENDENCIES.map((provide) => ({ provide, useValue: { setContext: vi.fn() } })),
        ...originalProviders,
        AssetMediaService,
        { provide: KYSELY_MODULE_CONNECTION_TOKEN(), useValue: {} },
      ],
    })
      .overrideProvider(ORIGINAL_STORES)
      .useValue({
        get: () => store,
        uploadBinding: {
          provider: 's3',
          storeId: 'fixed',
          endpoint: 'https://example.invalid',
          bucket: 'fixture',
          store,
        },
      })
      .compile();
    try {
      expect(module.get(UploadPublicationHttpAdapter)).toBeInstanceOf(UploadPublicationHttpAdapter);
      expect(module.get(AssetMediaService).isManagedUpload()).toBe(true);
      expect(module.get(AssetMediaService).isManagedUpload('file')).toBe(false);
    } finally {
      await module.close();
    }
  } finally {
    vi.unstubAllEnvs();
    await rm(root, { recursive: true, force: true });
  }
});
