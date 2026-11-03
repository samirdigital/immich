import { S3Client, S3ClientConfig } from '@aws-sdk/client-s3';
import { lstat, realpath } from 'node:fs/promises';
import path from 'node:path';
import { z } from 'zod';
import { AssetOriginalRepository } from 'src/repositories/asset-original.repository.js';
import { S3ObjectStoreRepository } from 'src/repositories/s3-object-store.repository.js';
import { OriginalReader, OriginalStoreRegistry } from 'src/services/original-reader.js';

export const ORIGINAL_STORES = Symbol('OriginalStores');

const configSchema = z.object({
  IMMICH_OBJECT_STORE_ID: z.string().regex(/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/),
  IMMICH_OBJECT_ENDPOINT: z.url(),
  IMMICH_OBJECT_REGION: z.string().regex(/^[a-z0-9-]+$/),
  IMMICH_OBJECT_BUCKET: z.string().regex(/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/),
  IMMICH_OBJECT_PROCESSING_ROOT: z.string().min(1),
  IMMICH_OBJECT_ACCESS_KEY_ID: z.string().min(1),
  IMMICH_OBJECT_SECRET_ACCESS_KEY: z.string().min(1),
});

// Server-only environment parsing; never part of public system config or request DTOs.
export const createOriginalStores = async (
  env: NodeJS.ProcessEnv,
  repository: AssetOriginalRepository,
  clientFactory: (options: S3ClientConfig) => S3Client = (options) => new S3Client(options),
): Promise<OriginalStoreRegistry & { privateProcessingRoot?: string; onModuleDestroy: () => void }> => {
  if (env.IMMICH_OBJECT_READ_ENABLED === undefined || env.IMMICH_OBJECT_READ_ENABLED === 'false') {
    return { get: (): undefined => undefined, onModuleDestroy: () => {} };
  }
  try {
    if (env.IMMICH_OBJECT_READ_ENABLED !== 'true') {
      throw new Error('Invalid flag');
    }
    const config = configSchema.parse(env);
    const endpoint = new URL(config.IMMICH_OBJECT_ENDPOINT);
    if (
      endpoint.protocol !== 'https:' ||
      endpoint.username ||
      endpoint.password ||
      endpoint.pathname !== '/' ||
      endpoint.search ||
      endpoint.hash
    ) {
      throw new Error('Invalid endpoint');
    }
    const root = config.IMMICH_OBJECT_PROCESSING_ROOT;
    if (!path.isAbsolute(root) || (await realpath(root)) !== root) {
      throw new Error('Non-canonical root');
    }
    const stats = await lstat(root);
    if (!stats.isDirectory() || (stats.mode & 0o777) !== 0o700 || stats.uid !== process.getuid!()) {
      throw new Error('Non-private root');
    }
    const storeId = config.IMMICH_OBJECT_STORE_ID;
    await repository.registerStore({ storeId, endpoint: endpoint.origin, bucket: config.IMMICH_OBJECT_BUCKET });
    const client = clientFactory({
      endpoint: endpoint.origin,
      region: config.IMMICH_OBJECT_REGION,
      credentials: {
        accessKeyId: config.IMMICH_OBJECT_ACCESS_KEY_ID,
        secretAccessKey: config.IMMICH_OBJECT_SECRET_ACCESS_KEY,
      },
      forcePathStyle: true,
      maxAttempts: 1,
      requestChecksumCalculation: 'WHEN_REQUIRED',
      responseChecksumValidation: 'WHEN_REQUIRED',
      requestHandler: { connectionTimeout: 10_000, requestTimeout: 30_000 },
    });
    const store = new S3ObjectStoreRepository(client, { bucket: config.IMMICH_OBJECT_BUCKET });
    let closed = false;
    return Object.freeze({
      privateProcessingRoot: root,
      readTimeoutMs: 30_000,
      get: (id: string) => (!closed && id === storeId ? store : undefined),
      onModuleDestroy: () => {
        if (closed) {
          return;
        }

        closed = true;
        client.destroy();
      },
    });
  } catch {
    throw new Error('Invalid original object store configuration');
  }
};

export const originalProviders = [
  AssetOriginalRepository,
  {
    provide: ORIGINAL_STORES,
    inject: [AssetOriginalRepository],
    useFactory: (repository: AssetOriginalRepository) => createOriginalStores(process.env, repository),
  },
  {
    provide: OriginalReader,
    inject: [AssetOriginalRepository, ORIGINAL_STORES],
    useFactory: (repository: AssetOriginalRepository, registry: OriginalStoreRegistry) =>
      new OriginalReader(repository, registry),
  },
];
