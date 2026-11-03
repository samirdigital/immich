import { BadRequestException, ForbiddenException } from '@nestjs/common';
import { Kysely, sql } from 'kysely';
import path from 'node:path';
import { AssetMediaResponseDto, AssetMediaStatus } from 'src/dtos/asset-media-response.dto.js';
import { AssetMediaCreateDto, UploadFieldName } from 'src/dtos/asset-media.dto.js';
import { AuthDto } from 'src/dtos/auth.dto.js';
import { AlbumUserRole, AssetVisibility, Permission, SharedLinkType, UserStatus } from 'src/enum.js';
import { OwnedSourceRepository } from 'src/repositories/owned-source.repository.js';
import { DB } from 'src/schema/index.js';
import { PublicationAuth } from 'src/schema/tables/upload-publication.table.js';
import { OwnedSource, OwnedSourceService } from 'src/services/owned-source.service.js';
import { UploadPublicationService } from 'src/services/upload-publication.service.js';
import { ImmichFile, UploadFile } from 'src/types.js';
import { requireUploadAccess } from 'src/utils/access.js';
import { mimeTypes } from 'src/utils/mime-types.js';
import { snapshotPublicationRequest } from 'src/utils/upload-publication.js';

const receipts = new WeakMap<object, Promise<OwnedSource>>();
const receiptProof = Symbol('serverUploadReceipt');
const receiptFor = (file: object) =>
  receipts.get(file) ?? receipts.get((file as { [receiptProof]?: object })[receiptProof] ?? file);
export const transferUploadReceipt = (source: object, target: object) => {
  const receipt = receiptFor(source);
  if (receipt) {
    receipts.set(target, receipt);
  }
  return target;
};

export class UploadPublicationHttpAdapter {
  constructor(
    private db: Kysely<DB>,
    private sources: OwnedSourceService,
    private journal: OwnedSourceRepository,
    private publications: UploadPublicationService,
  ) {}

  private async provenance(auth: AuthDto): Promise<PublicationAuth> {
    requireUploadAccess(auth);
    if (auth.sharedLink) {
      const link = await this.db
        .selectFrom('shared_link')
        .select('type')
        .where('id', '=', auth.sharedLink.id)
        .where('userId', '=', auth.user.id)
        .executeTakeFirstOrThrow();
      return { kind: 'sharedLink', id: auth.sharedLink.id, type: link.type, albumId: auth.sharedLink.albumId };
    }
    if (auth.apiKey) {
      return { kind: 'apiKey', id: auth.apiKey.id };
    }
    if (auth.session) {
      return { kind: 'session', id: auth.session.id };
    }
    throw new ForbiddenException('Upload authentication provenance required');
  }

  private async authorize(auth: AuthDto) {
    const context = await this.provenance(auth);
    const ownerId = auth.user.id;
    const owner = await this.db
      .selectFrom('user')
      .select('id')
      .where('id', '=', ownerId)
      .where('deletedAt', 'is', null)
      .where('status', '=', UserStatus.Active)
      .executeTakeFirst();
    if (!owner) {
      throw new ForbiddenException('Owner unavailable');
    }
    if (context.kind === 'session') {
      const session = await this.db
        .selectFrom('session')
        .select('id')
        .where('id', '=', context.id)
        .where('userId', '=', ownerId)
        .where((eb) => eb.or([eb('expiresAt', 'is', null), eb('expiresAt', '>', sql<Date>`clock_timestamp()`)]))
        .executeTakeFirst();
      if (!session) {
        throw new ForbiddenException('Upload grant unavailable');
      }
    } else if (context.kind === 'apiKey') {
      const key = await this.db
        .selectFrom('api_key')
        .select('permissions')
        .where('id', '=', context.id)
        .where('userId', '=', ownerId)
        .executeTakeFirst();
      if (!key || (!key.permissions.includes(Permission.All) && !key.permissions.includes(Permission.AssetUpload))) {
        throw new ForbiddenException('Upload grant unavailable');
      }
    } else {
      const link = await this.db
        .selectFrom('shared_link')
        .select(['type', 'albumId'])
        .where('id', '=', context.id)
        .where('userId', '=', ownerId)
        .where('allowUpload', '=', true)
        .where((eb) => eb.or([eb('expiresAt', 'is', null), eb('expiresAt', '>', sql<Date>`clock_timestamp()`)]))
        .executeTakeFirst();
      if (!link || link.type !== context.type || link.albumId !== context.albumId) {
        throw new ForbiddenException('Shared upload context changed');
      }
      if (link.type === SharedLinkType.Album && link.albumId) {
        const album = await this.db
          .selectFrom('album')
          .innerJoin('album_user', 'album.id', 'album_user.albumId')
          .select('album.id')
          .where('album.id', '=', link.albumId)
          .where('album.deletedAt', 'is', null)
          .where('album_user.userId', '=', ownerId)
          .where('album_user.role', '=', AlbumUserRole.Owner)
          .executeTakeFirst();
        if (!album) {
          throw new ForbiddenException('Shared album unavailable');
        }
      } else if (link.type !== SharedLinkType.Individual || link.albumId !== null) {
        throw new ForbiddenException('Invalid shared upload context');
      }
    }
    return context;
  }

  receive(auth: AuthDto, file: Express.Multer.File, filename: string) {
    const role = file.fieldname === UploadFieldName.SIDECAR_DATA ? 'sidecar' : 'original';
    const received = this.authorize(auth).then(() =>
      this.sources.receive(
        auth.user.id,
        role,
        role === 'sidecar' ? '.xmp' : path.extname(filename).toLowerCase(),
        file.stream,
      ),
    );
    receipts.set(file, received);
    const token = {};
    receipts.set(token, received);
    Object.assign(file, { [receiptProof]: token });
    return received
      .catch((error: unknown) => {
        if (error instanceof Error && error.message === 'Source is empty') {
          throw new BadRequestException('File is empty');
        }
        throw error;
      })
      .then((source): Partial<ImmichFile> => ({
        uuid: source.id,
        sourceReceiptId: source.id,
        path: path.join(source.root, source.name),
        checksum: Buffer.from(source.sha1!, 'hex'),
        size: Number(source.size),
      }));
  }

  async retain(file: object, ownerId?: string): Promise<boolean> {
    const promise = receiptFor(file);
    if (!promise) {
      return false;
    }
    try {
      const proof = await promise;
      const current = await this.journal.get(proof.id, proof.ownerId);
      const metadata = file as { path?: string; originalPath?: string; sourceReceiptId?: string };
      const receivedPath = metadata.originalPath ?? metadata.path;
      if (
        !current ||
        current.ownerId !== ownerId ||
        current.root !== proof.root ||
        current.name !== proof.name ||
        metadata.sourceReceiptId !== proof.id ||
        receivedPath !== path.join(proof.root, proof.name)
      ) {
        throw new Error('Managed source identity mismatch');
      }
    } catch {
      return true;
    }
    return true;
  }

  private async source(file: UploadFile, ownerId: string, role: 'original' | 'sidecar') {
    const proof = await receiptFor(file);
    if (
      !proof ||
      proof.ownerId !== ownerId ||
      proof.role !== role ||
      path.join(proof.root, proof.name) !== file.originalPath
    ) {
      throw new BadRequestException('Trusted upload source required');
    }
    const current = await this.journal.get(proof.id, ownerId);
    if (!current || current.root !== proof.root || current.name !== proof.name || current.state !== 'received') {
      throw new BadRequestException('Upload source unavailable');
    }
    return current;
  }

  async upload(
    auth: AuthDto,
    dto: AssetMediaCreateDto,
    file: UploadFile,
    sidecar?: UploadFile,
    idempotencyKey?: string,
  ): Promise<AssetMediaResponseDto> {
    const context = await this.authorize(auth);
    const original = await this.source(file, auth.user.id, 'original');
    const companion = sidecar ? await this.source(sidecar, auth.user.id, 'sidecar') : undefined;
    const type = mimeTypes.assetType(file.originalPath);
    if (type !== 'IMAGE' && type !== 'VIDEO') {
      throw new BadRequestException('Unsupported asset type');
    }
    const input = snapshotPublicationRequest({
      ownerId: auth.user.id,
      sourceId: original.id,
      ...(companion && { sidecarId: companion.id }),
      ...(idempotencyKey !== undefined && { idempotencyKey }),
      auth: context,
      options: {
        filename: dto.filename || file.originalName,
        type,
        fileCreatedAt: dto.fileCreatedAt.toISOString(),
        fileModifiedAt: dto.fileModifiedAt.toISOString(),
        isFavorite: dto.isFavorite ?? false,
        visibility: dto.visibility ?? AssetVisibility.Timeline,
        duration: dto.duration ?? null,
        livePhotoVideoId: dto.livePhotoVideoId ?? null,
        metadata: dto.metadata ?? [],
      },
    });
    await this.sources.seal(auth.user.id, original.id, dto.fileModifiedAt);
    if (companion) {
      await this.sources.seal(auth.user.id, companion.id, dto.fileModifiedAt);
    }
    const result = await this.publications.publish(input);
    return {
      id: result.id,
      status: result.status === 'created' ? AssetMediaStatus.CREATED : AssetMediaStatus.DUPLICATE,
    };
  }
}
