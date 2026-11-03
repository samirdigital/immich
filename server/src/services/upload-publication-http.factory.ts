import { Kysely } from 'kysely';
import { lstat, realpath } from 'node:fs/promises';
import path from 'node:path';
import { ObjectCatalogRepository } from 'src/repositories/object-catalog.repository.js';
import { OwnedSourceRepository } from 'src/repositories/owned-source.repository.js';
import { UploadPublicationRepository } from 'src/repositories/upload-publication.repository.js';
import { DB } from 'src/schema/index.js';
import { ObjectCatalogService } from 'src/services/object-catalog.service.js';
import { OriginalStoreRegistry } from 'src/services/original-reader.js';
import { OwnedSourceService } from 'src/services/owned-source.service.js';
import { UploadPublicationHttpAdapter } from 'src/services/upload-publication-http.adapter.js';
import { PublicationStoreBinding, UploadPublicationService } from 'src/services/upload-publication.service.js';

export interface UploadStoreRegistry extends OriginalStoreRegistry {
  readonly uploadBinding?: Readonly<PublicationStoreBinding>;
}

export const createManagedUpload = async (env: NodeJS.ProcessEnv, db: Kysely<DB>, registry: UploadStoreRegistry) => {
  if (env.IMMICH_OBJECT_UPLOAD_ENABLED === undefined || env.IMMICH_OBJECT_UPLOAD_ENABLED === 'false') {
    return;
  }
  try {
    if (env.IMMICH_OBJECT_UPLOAD_ENABLED !== 'true' || !db) {
      throw new Error('Invalid flag');
    }
    const binding = registry.uploadBinding;
    if (!binding || registry.get(binding.storeId) !== binding.store) {
      throw new Error('Managed read binding required');
    }
    const roots = [env.IMMICH_OBJECT_SOURCE_ROOT, env.IMMICH_OBJECT_CATALOG_ROOT];
    for (const root of roots) {
      if (!root || !path.isAbsolute(root) || (await realpath(root)) !== root) {
        throw new Error('Canonical root required');
      }
      const stats = await lstat(root);
      if (!stats.isDirectory() || (stats.mode & 0o777) !== 0o700 || stats.uid !== process.getuid!()) {
        throw new Error('Private root required');
      }
    }
    if (roots[0] === roots[1]) {
      throw new Error('Separate roots required');
    }
    const journal = new OwnedSourceRepository(db);
    const catalog = new ObjectCatalogRepository(db);
    return new UploadPublicationHttpAdapter(
      db,
      new OwnedSourceService(journal, roots[0]!),
      journal,
      new UploadPublicationService(
        new UploadPublicationRepository(db),
        catalog,
        new ObjectCatalogService(catalog, roots[1]!),
        binding,
      ),
    );
  } catch {
    throw new Error('Invalid managed upload configuration');
  }
};
