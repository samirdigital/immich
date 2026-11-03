import { ConflictException, ForbiddenException } from '@nestjs/common';
import { Kysely, Selectable, Transaction, sql } from 'kysely';
import { randomUUID } from 'node:crypto';
import path from 'node:path';
import {
  AlbumUserRole,
  AssetFileType,
  AssetType,
  AssetVisibility,
  ChecksumAlgorithm,
  Permission,
  SharedLinkType,
} from 'src/enum.js';
import { DB } from 'src/schema/index.js';
import { SourceReceiptTable } from 'src/schema/tables/source-receipt.table.js';
import {
  PublicationAuth,
  PublicationResult,
  UploadPublicationTable,
} from 'src/schema/tables/upload-publication.table.js';
import { isAssetChecksumConstraint } from 'src/utils/database.js';
import {
  PublicationHeld,
  PublicationRequest,
  PublicationStoreBinding,
  publicationFingerprint,
} from 'src/utils/upload-publication.js';

export class UploadPublicationRepository {
  constructor(private readonly db: Kysely<DB>) {}

  get(id: string, ownerId: string) {
    return this.db
      .selectFrom('upload_publication')
      .selectAll()
      .where('id', '=', id)
      .where('ownerId', '=', ownerId)
      .executeTakeFirstOrThrow();
  }

  async reserve(input: PublicationRequest, binding: PublicationStoreBinding) {
    return this.db.transaction().execute(async (db) => {
      await db
        .selectFrom('user')
        .select('id')
        .where('id', '=', input.ownerId)
        .forNoKeyUpdate()
        .executeTakeFirstOrThrow();
      const source = await this.source(db, input.sourceId, input.ownerId, 'original');
      const sidecar = input.sidecarId ? await this.source(db, input.sidecarId, input.ownerId, 'sidecar') : undefined;
      const modified = BigInt(new Date(input.options.fileModifiedAt).getTime());
      if (
        [source, ...(sidecar ? [sidecar] : [])].some(
          (value) => BigInt(value.identity!.mtimeNs) / 1_000_000n !== modified,
        )
      ) {
        throw new ForbiddenException('DTO timestamp does not match the sealed source');
      }
      const fingerprint = publicationFingerprint({
        ownerId: input.ownerId,
        original: { sha256: source.sha256, size: source.size },
        sidecar: sidecar ? { sha256: sidecar.sha256, size: sidecar.size } : null,
        options: input.options,
        auth: input.auth,
        store: { storeId: binding.storeId, endpoint: binding.endpoint, bucket: binding.bucket },
      });
      const key = input.idempotencyKey ?? fingerprint;
      const existing = await db
        .selectFrom('upload_publication')
        .selectAll()
        .where('ownerId', '=', input.ownerId)
        .where((eb) => eb.or([eb('idempotencyKey', '=', key), eb('sourceId', '=', source.id)]))
        .execute();
      if (existing.length > 0) {
        if (existing.length !== 1 || existing[0].idempotencyKey !== key || existing[0].fingerprint !== fingerprint) {
          throw new ConflictException('Publication idempotency conflict');
        }
        return existing[0];
      }
      await db
        .insertInto('original_store_identity')
        .values({ storeId: binding.storeId, endpoint: binding.endpoint, bucket: binding.bucket })
        .onConflict((oc) => oc.column('storeId').doNothing())
        .execute();
      await db
        .selectFrom('original_store_identity')
        .select('storeId')
        .where('storeId', '=', binding.storeId)
        .where('endpoint', '=', binding.endpoint)
        .where('bucket', '=', binding.bucket)
        .executeTakeFirstOrThrow();
      await this.authorize(db, input.ownerId, input.auth);
      return db
        .insertInto('upload_publication')
        .values({
          id: randomUUID(),
          ownerId: input.ownerId,
          idempotencyKey: key,
          fingerprint,
          reservedAssetId: randomUUID(),
          sourceId: source.id,
          sourceHash: source.sha256!,
          sourceSize: source.size!,
          sourceIdentity: source.identity!,
          sidecarId: sidecar?.id ?? null,
          sidecarHash: sidecar?.sha256 ?? null,
          sidecarSize: sidecar?.size ?? null,
          sidecarIdentity: sidecar?.identity ?? null,
          storeId: binding.storeId,
          endpoint: binding.endpoint,
          bucket: binding.bucket,
          options: input.options,
          auth: input.auth,
        })
        .returningAll()
        .executeTakeFirstOrThrow();
    });
  }

  async withWorker<T>(id: string, action: (db: Kysely<DB>) => Promise<T>): Promise<T> {
    return this.db.connection().execute(async (db) => {
      const key = sql`hashtextextended(${`upload-publication:${id}`}::text, 0)`;
      const lock = await sql<{ acquired: boolean }>`SELECT pg_try_advisory_lock(${key}) AS acquired`.execute(db);
      if (!lock.rows[0].acquired) {
        throw new PublicationHeld(id);
      }
      try {
        return await action(db);
      } finally {
        await sql`SELECT pg_advisory_unlock(${key})`.execute(db);
      }
    });
  }

  hold(id: string, ownerId: string) {
    return this.db
      .updateTable('upload_publication')
      .set({ state: 'held' })
      .where('id', '=', id)
      .where('ownerId', '=', ownerId)
      .where('result', 'is', null)
      .execute();
  }

  private async source(db: Transaction<DB>, id: string, ownerId: string, role: 'original' | 'sidecar') {
    const source = await db
      .selectFrom('source_receipt')
      .selectAll()
      .where('id', '=', id)
      .where('ownerId', '=', ownerId)
      .forShare()
      .executeTakeFirstOrThrow();
    if (
      source.state !== 'sealed' ||
      source.role !== role ||
      !source.sha256 ||
      !source.sha1 ||
      source.size === null ||
      !source.identity
    ) {
      throw new ForbiddenException('A sealed owned source is required');
    }
    return source;
  }

  private async authorize(db: Transaction<DB>, ownerId: string, auth: PublicationAuth) {
    const owner = await db
      .selectFrom('user')
      .select(['id', 'deletedAt', 'status', 'quotaSizeInBytes', 'quotaUsageInBytes'])
      .where('id', '=', ownerId)
      .forNoKeyUpdate()
      .executeTakeFirstOrThrow();
    if (owner.deletedAt || owner.status !== 'active') {
      throw new ForbiddenException('Owner is unavailable');
    }
    if (auth.kind === 'session') {
      await db
        .selectFrom('session')
        .select('id')
        .where('id', '=', auth.id)
        .where('userId', '=', ownerId)
        .forUpdate()
        .executeTakeFirstOrThrow();
      await db
        .selectFrom('session')
        .select('id')
        .where('id', '=', auth.id)
        .where('userId', '=', ownerId)
        .where((eb) => eb.or([eb('expiresAt', 'is', null), eb('expiresAt', '>', sql<Date>`clock_timestamp()`)]))
        .forUpdate()
        .executeTakeFirstOrThrow();
      return null;
    }
    if (auth.kind === 'apiKey') {
      const key = await db
        .selectFrom('api_key')
        .select('permissions')
        .where('id', '=', auth.id)
        .where('userId', '=', ownerId)
        .forUpdate()
        .executeTakeFirstOrThrow();
      if (!key.permissions.includes(Permission.All) && !key.permissions.includes(Permission.AssetUpload)) {
        throw new ForbiddenException('Upload grant revoked');
      }
      return null;
    }
    await db
      .selectFrom('shared_link')
      .select('id')
      .where('id', '=', auth.id)
      .where('userId', '=', ownerId)
      .forUpdate()
      .executeTakeFirstOrThrow();
    const link = await db
      .selectFrom('shared_link')
      .select(['id', 'userId', 'type', 'albumId', 'allowUpload', 'expiresAt'])
      .where('id', '=', auth.id)
      .where('userId', '=', ownerId)
      .where('allowUpload', '=', true)
      .where((eb) => eb.or([eb('expiresAt', 'is', null), eb('expiresAt', '>', sql<Date>`clock_timestamp()`)]))
      .forUpdate()
      .executeTakeFirstOrThrow();
    if (
      link.type !== auth.type ||
      link.albumId !== auth.albumId ||
      ![SharedLinkType.Album, SharedLinkType.Individual].includes(link.type)
    ) {
      throw new ForbiddenException('Shared upload context changed');
    }
    if (link.type === SharedLinkType.Album) {
      if (!link.albumId) {
        throw new ForbiddenException('Missing shared album');
      }
      await db
        .selectFrom('album')
        .select('id')
        .where('id', '=', link.albumId)
        .where('deletedAt', 'is', null)
        .forUpdate()
        .executeTakeFirstOrThrow();
      await db
        .selectFrom('album_user')
        .select('albumId')
        .where('albumId', '=', link.albumId)
        .where('userId', '=', ownerId)
        .where('role', '=', AlbumUserRole.Owner)
        .forUpdate()
        .executeTakeFirstOrThrow();
    } else if (link.albumId !== null) {
      throw new ForbiddenException('Invalid individual link');
    }
    return link;
  }

  async complete(id: string, ownerId: string): Promise<PublicationResult> {
    try {
      return await this.completeTransaction(id, ownerId, false);
    } catch (error) {
      if (!isAssetChecksumConstraint(error)) {
        throw error;
      }
      return this.completeTransaction(id, ownerId, true);
    }
  }

  private async completeTransaction(id: string, ownerId: string, duplicateOnly: boolean): Promise<PublicationResult> {
    return this.db.transaction().execute(async (db) => {
      const receipt = await db
        .selectFrom('upload_publication')
        .selectAll()
        .where('id', '=', id)
        .where('ownerId', '=', ownerId)
        .forUpdate()
        .executeTakeFirstOrThrow();
      if (receipt.result) {
        return receipt.result;
      }
      const owner = await db
        .selectFrom('user')
        .select(['id', 'deletedAt', 'status', 'quotaSizeInBytes', 'quotaUsageInBytes'])
        .where('id', '=', ownerId)
        .forNoKeyUpdate()
        .executeTakeFirstOrThrow();
      const source = await this.source(db, receipt.sourceId, ownerId, 'original');
      const sidecar = receipt.sidecarId ? await this.source(db, receipt.sidecarId, ownerId, 'sidecar') : undefined;
      const link = await this.authorize(db, ownerId, receipt.auth);
      const stage = await db
        .selectFrom('staged_object')
        .selectAll()
        .where('id', '=', receipt.stageId!)
        .forUpdate()
        .executeTakeFirstOrThrow();
      const owned = await db
        .selectFrom('owned_stage_receipt')
        .selectAll()
        .where('stageId', '=', stage.id)
        .where('sourceId', '=', source.id)
        .where('ownerId', '=', ownerId)
        .executeTakeFirstOrThrow();
      const intent = await db
        .selectFrom('object_upload_intent')
        .selectAll()
        .where('id', '=', receipt.intentId!)
        .where('stageId', '=', stage.id)
        .forUpdate()
        .executeTakeFirstOrThrow();
      const version = await db
        .selectFrom('object_version')
        .selectAll()
        .where('intentId', '=', intent.id)
        .forUpdate()
        .executeTakeFirstOrThrow();
      if (
        stage.state !== 'ready' ||
        stage.released ||
        stage.hash !== source.sha256 ||
        stage.size !== source.size ||
        owned.hash !== source.sha256 ||
        owned.size !== source.size ||
        intent.state !== 'acknowledged' ||
        !intent.token ||
        receipt.exactAttempt !== intent.token ||
        intent.provider !== 's3' ||
        intent.storeId !== receipt.storeId ||
        version.provider !== 's3' ||
        version.storeId !== receipt.storeId ||
        version.key !== intent.key ||
        !version.versionId ||
        version.versionId === 'null' ||
        version.verification !== 'verified'
      ) {
        throw new ForbiddenException('Publication evidence mismatch');
      }
      await db
        .selectFrom('original_store_identity')
        .selectAll()
        .where('storeId', '=', receipt.storeId)
        .where('endpoint', '=', receipt.endpoint)
        .where('bucket', '=', receipt.bucket)
        .forShare()
        .executeTakeFirstOrThrow();
      const duplicate = await db
        .selectFrom('asset')
        .select('id')
        .where('ownerId', '=', ownerId)
        .where('libraryId', 'is', null)
        .where('checksum', '=', Buffer.from(source.sha1!, 'hex'))
        .executeTakeFirst();
      if (duplicate) {
        const result: PublicationResult = { id: duplicate.id, status: 'duplicate' };
        await db
          .updateTable('upload_publication')
          .set({
            state: 'duplicate',
            result,
            catalogVersionId: version.id,
            exactAttempt: intent.token,
            objectKey: version.key,
            versionId: version.versionId,
          })
          .where('id', '=', id)
          .execute();
        await this.authorize(db, ownerId, receipt.auth);
        return result;
      }
      if (duplicateOnly) {
        throw new ConflictException('Checksum winner needs reconciliation');
      }
      if (
        owner.quotaSizeInBytes !== null &&
        BigInt(owner.quotaUsageInBytes) + BigInt(source.size!) > BigInt(owner.quotaSizeInBytes)
      ) {
        throw new ForbiddenException('Quota exceeded');
      }
      const options = receipt.options;
      if (options.livePhotoVideoId) {
        await db
          .selectFrom('asset')
          .select('id')
          .where('id', '=', options.livePhotoVideoId)
          .where('ownerId', '=', ownerId)
          .where('type', '=', AssetType.Video)
          .where('deletedAt', 'is', null)
          .forUpdate()
          .executeTakeFirstOrThrow();
      }
      await db
        .insertInto('asset')
        .values({
          id: receipt.reservedAssetId,
          ownerId,
          libraryId: null,
          checksum: Buffer.from(source.sha1!, 'hex'),
          checksumAlgorithm: ChecksumAlgorithm.sha1File,
          originalPath: path.join(source.root, source.name),
          originalFileName: options.filename,
          type: options.type as AssetType,
          fileCreatedAt: options.fileCreatedAt,
          fileModifiedAt: options.fileModifiedAt,
          localDateTime: options.fileCreatedAt,
          isFavorite: options.isFavorite,
          visibility: options.visibility as AssetVisibility,
          duration: options.duration,
          livePhotoVideoId: options.livePhotoVideoId,
        })
        .execute();
      await db
        .insertInto('asset_exif')
        .values({ assetId: receipt.reservedAssetId, fileSizeInByte: Number(source.size) })
        .execute();
      for (const metadata of options.metadata) {
        await db
          .insertInto('asset_metadata')
          .values({ assetId: receipt.reservedAssetId, key: metadata.key, value: metadata.value })
          .execute();
      }
      if (sidecar) {
        await db
          .insertInto('asset_file')
          .values({
            assetId: receipt.reservedAssetId,
            type: AssetFileType.Sidecar,
            path: path.join(sidecar.root, sidecar.name),
          })
          .execute();
      }
      if (link?.type === SharedLinkType.Album) {
        await db
          .insertInto('album_asset')
          .values({ albumId: link.albumId!, assetId: receipt.reservedAssetId })
          .execute();
      } else if (link) {
        await db
          .insertInto('shared_link_asset')
          .values({ sharedLinkId: link.id, assetId: receipt.reservedAssetId })
          .execute();
      }
      if (options.livePhotoVideoId) {
        await db
          .updateTable('asset')
          .set({ visibility: AssetVisibility.Hidden })
          .where('id', '=', options.livePhotoVideoId)
          .execute();
      }
      await this.bind(db, receipt, source, intent.id, version.id, version.key, version.versionId!);
      await db
        .updateTable('user')
        .set({ quotaUsageInBytes: sql`"quotaUsageInBytes" + ${source.size}::bigint` })
        .where('id', '=', ownerId)
        .execute();
      const result: PublicationResult = { id: receipt.reservedAssetId, status: 'created' };
      await db
        .updateTable('upload_publication')
        .set({
          state: 'published',
          publishedAssetId: receipt.reservedAssetId,
          claimId: receipt.id,
          exactAttempt: intent.token,
          catalogVersionId: version.id,
          objectKey: version.key,
          versionId: version.versionId,
          result,
        })
        .where('id', '=', receipt.id)
        .execute();
      for (const kind of ['metadata', 'event', 'workflow'] as const) {
        await db
          .insertInto('upload_publication_outbox')
          .values({
            id: randomUUID(),
            receiptId: receipt.id,
            kind,
            target: kind === 'metadata' ? 'AssetExtractMetadata' : 'AssetCreate',
            state: 'pending',
            payload: { assetId: receipt.reservedAssetId, ownerId, quotaAlreadyCharged: true },
          })
          .execute();
      }
      await this.authorize(db, ownerId, receipt.auth);
      return result;
    });
  }

  private async bind(
    db: Transaction<DB>,
    receipt: Selectable<UploadPublicationTable>,
    source: Selectable<SourceReceiptTable>,
    intentId: string,
    catalogVersionId: string,
    key: string,
    versionId: string,
  ) {
    await db
      .insertInto('asset_original_claim')
      .values({ id: receipt.id, assetId: receipt.reservedAssetId, ownerId: receipt.ownerId, stageId: receipt.stageId! })
      .execute();
    await db
      .insertInto('asset_original')
      .values({
        assetId: receipt.reservedAssetId,
        ownerId: receipt.ownerId,
        stageId: receipt.stageId!,
        claimId: receipt.id,
        intentId,
        catalogVersionId,
        provider: 's3',
        storeId: receipt.storeId,
        endpoint: receipt.endpoint,
        bucket: receipt.bucket,
        key,
        versionId,
        hash: source.sha256!,
        size: source.size!,
        verification: 'verified',
      })
      .execute();
    await db
      .updateTable('asset')
      .set({ originalAuthority: 'object' })
      .where('id', '=', receipt.reservedAssetId)
      .execute();
  }
}
