import { BadRequestException, ConflictException } from '@nestjs/common';
import { createHash } from 'node:crypto';
import path from 'node:path';
import { PublicationAuth, PublicationOptions } from 'src/schema/tables/upload-publication.table.js';
import { CatalogStoreBinding } from 'src/services/object-catalog.service.js';

export interface PublicationStoreBinding extends CatalogStoreBinding {
  provider: 's3';
  endpoint: string;
  bucket: string;
}
export interface PublicationRequest {
  ownerId: string;
  sourceId: string;
  sidecarId?: string;
  idempotencyKey?: string;
  auth: PublicationAuth;
  options: PublicationOptions;
}
export class PublicationHeld extends ConflictException {
  constructor(readonly receiptId: string) {
    super('Publication is held for reconciliation');
  }
}
export const canonicalPublicationJson = (value: unknown): string => {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') {
    return JSON.stringify(value);
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalPublicationJson(item)).join(',')}]`;
  }
  if (typeof value === 'object' && value !== null && Object.getPrototypeOf(value) === Object.prototype) {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalPublicationJson(record[key])}`)
      .join(',')}}`;
  }
  throw new BadRequestException('Invalid publication JSON');
};
export const publicationFingerprint = (input: unknown) =>
  `v1:${createHash('sha256').update(canonicalPublicationJson(input)).digest('hex')}`;

export const snapshotPublicationRequest = (request: PublicationRequest): PublicationRequest => {
  const snapshot = JSON.parse(canonicalPublicationJson(request)) as PublicationRequest;
  const options = snapshot.options;
  if (
    !options ||
    !options.filename ||
    Buffer.byteLength(options.filename) > 255 ||
    path.basename(options.filename) !== options.filename ||
    [...options.filename].some((character) => character.codePointAt(0)! < 32 || character === '\\') ||
    !['IMAGE', 'VIDEO', 'AUDIO'].includes(options.type) ||
    typeof options.isFavorite !== 'boolean' ||
    !['timeline', 'hidden', 'archive', 'locked'].includes(options.visibility) ||
    (options.duration !== null &&
      (!Number.isSafeInteger(options.duration) || options.duration < 0 || options.duration > 2_147_483_647)) ||
    !Array.isArray(options.metadata)
  ) {
    throw new BadRequestException('Invalid publication options');
  }
  for (const key of ['fileCreatedAt', 'fileModifiedAt'] as const) {
    const date = new Date(options[key]);
    if (!Number.isFinite(date.getTime())) {
      throw new BadRequestException('Invalid publication date');
    }
    options[key] = date.toISOString();
  }
  if (
    options.metadata.length > 64 ||
    options.metadata.some(
      (entry) =>
        !entry.key ||
        Buffer.byteLength(entry.key) > 255 ||
        !entry.value ||
        Array.isArray(entry.value) ||
        typeof entry.value !== 'object',
    ) ||
    new Set(options.metadata.map((entry) => entry.key)).size !== options.metadata.length ||
    Buffer.byteLength(canonicalPublicationJson(options.metadata)) > 65_536
  ) {
    throw new BadRequestException('Invalid publication metadata');
  }
  options.metadata.sort((a, b) => a.key.localeCompare(b.key));
  if (
    snapshot.idempotencyKey !== undefined &&
    (typeof snapshot.idempotencyKey !== 'string' || !/^[\u{21}-\u{7E}]{1,200}$/u.test(snapshot.idempotencyKey))
  ) {
    throw new BadRequestException('Invalid idempotency key');
  }
  const auth = snapshot.auth;
  if (!auth || !['session', 'apiKey', 'sharedLink'].includes(auth.kind) || !auth.id) {
    throw new BadRequestException('Invalid publication auth provenance');
  }
  snapshot.auth =
    auth.kind === 'sharedLink'
      ? { kind: auth.kind, id: auth.id, type: auth.type, albumId: auth.albumId }
      : { kind: auth.kind, id: auth.id };
  snapshot.options = {
    filename: options.filename,
    type: options.type,
    fileCreatedAt: options.fileCreatedAt,
    fileModifiedAt: options.fileModifiedAt,
    isFavorite: options.isFavorite,
    visibility: options.visibility,
    duration: options.duration,
    livePhotoVideoId: options.livePhotoVideoId,
    metadata: options.metadata,
  };
  return snapshot;
};
