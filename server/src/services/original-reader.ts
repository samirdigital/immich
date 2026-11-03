import { ServiceUnavailableException } from '@nestjs/common';
import { AssetOriginalRepository } from 'src/repositories/asset-original.repository.js';
import { ObjectByteRange, ObjectStore, validateObjectIdentity } from 'src/repositories/object-store.repository.js';
import { ImmichFileResponse } from 'src/utils/file.js';

export interface OriginalStoreRegistry {
  readonly readTimeoutMs?: number;
  get(storeId: string): ObjectStore | undefined;
}
export type OriginalResponse =
  | { kind: 'local'; file: ImmichFileResponse }
  | {
      kind: 'object';
      readTimeoutMs: number;
      object: { key: string; versionId: string };
      file: ImmichFileResponse;
      size: number;
      modified: Date;
      etag: string;
      open: (signal: AbortSignal, range?: ObjectByteRange) => ReturnType<ObjectStore['get']>;
    };

export class OriginalReader {
  constructor(
    private repository: AssetOriginalRepository,
    private stores: OriginalStoreRegistry,
  ) {}

  async resolve(assetId: string, file: ImmichFileResponse): Promise<OriginalResponse> {
    try {
      const result = await this.repository.resolve(assetId);
      if (result.authority === 'local') {
        return { kind: 'local', file };
      }
      const mapping = result.mapping;
      if (
        !mapping ||
        mapping.provider !== 's3' ||
        mapping.verification !== 'verified' ||
        !mapping.versionId ||
        mapping.versionId === 'null' ||
        !mapping.hash ||
        !/^[a-f0-9]{64}$/.test(mapping.hash) ||
        mapping.size === null
      ) {
        throw new Error('Invalid original binding');
      }
      const size = Number(mapping.size);
      const modified = new Date(mapping.publishedAt);
      if (!Number.isSafeInteger(size) || size < 0 || !Number.isFinite(modified.getTime())) {
        throw new Error('Invalid original metadata');
      }
      const store = this.stores.get(mapping.storeId);
      if (!store) {
        throw new Error('Original store unavailable');
      }
      const object = Object.freeze({ key: mapping.key, versionId: mapping.versionId });
      validateObjectIdentity(object);
      const readTimeoutMs = this.stores.readTimeoutMs ?? 30_000;
      if (!Number.isSafeInteger(readTimeoutMs) || readTimeoutMs < 1 || readTimeoutMs > 300_000) {
        throw new Error('Invalid read deadline');
      }
      return {
        kind: 'object',
        readTimeoutMs,
        object,
        file,
        size,
        modified,
        etag: `"sha256-${mapping.hash}"`,
        open: (signal, range) => store.get(object, { signal, ...(range && { range }), expectedSize: size }),
      };
    } catch {
      throw new ServiceUnavailableException('Original unavailable');
    }
  }
}
