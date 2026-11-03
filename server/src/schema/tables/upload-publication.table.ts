import { Check, Column, ForeignKeyConstraint, type Generated, PrimaryColumn, Table, Unique } from '@immich/sql-tools';
import { AssetOriginalClaimTable, OriginalStoreIdentityTable } from 'src/schema/tables/asset-original.table.js';
import { AssetTable } from 'src/schema/tables/asset.table.js';
import { ObjectUploadIntentTable } from 'src/schema/tables/object-upload-intent.table.js';
import { ObjectVersionTable } from 'src/schema/tables/object-version.table.js';
import { OwnedStageReceiptTable } from 'src/schema/tables/owned-stage-receipt.table.js';
import { SourceReceiptTable } from 'src/schema/tables/source-receipt.table.js';
import { type StageIdentity } from 'src/schema/tables/staged-object.table.js';
import { UserTable } from 'src/schema/tables/user.table.js';

export type PublicationAuth =
  | { kind: 'session'; id: string }
  | { kind: 'apiKey'; id: string }
  | { kind: 'sharedLink'; id: string; type: string; albumId: string | null };
export interface PublicationOptions {
  filename: string;
  type: 'IMAGE' | 'VIDEO' | 'AUDIO';
  fileCreatedAt: string;
  fileModifiedAt: string;
  isFavorite: boolean;
  visibility: 'timeline' | 'hidden' | 'archive' | 'locked';
  duration: number | null;
  livePhotoVideoId: string | null;
  metadata: { key: string; value: Record<string, unknown> }[];
}
export interface PublicationResult {
  id: string;
  status: 'created' | 'duplicate';
}
export const publicationStateCheck = `state IN ('reserved','prepared','held','published','duplicate') AND ((state IN ('published','duplicate')) = (result IS NOT NULL)) AND ((state = 'published') = ("publishedAssetId" IS NOT NULL)) AND ("publishedAssetId" IS NULL OR "publishedAssetId" = "reservedAssetId") AND (("claimId" IS NOT NULL) = ("publishedAssetId" IS NOT NULL)) AND ("intentId" IS NULL OR "stageId" IS NOT NULL) AND (state <> 'prepared' OR "intentId" IS NOT NULL)`;

@Table('upload_publication')
@Check({
  name: 'upload_publication_source_check',
  expression: `"sourceState" = 'sealed' AND (("sidecarId" IS NULL AND "sidecarHash" IS NULL AND "sidecarSize" IS NULL AND "sidecarIdentity" IS NULL) OR ("sidecarId" IS NOT NULL AND "sidecarHash" IS NOT NULL AND "sidecarSize" IS NOT NULL AND "sidecarIdentity" IS NOT NULL))`,
})
@Unique({ name: 'upload_publication_owner_key_uq', columns: ['ownerId', 'idempotencyKey'] })
@Unique({ name: 'upload_publication_source_uq', columns: ['sourceId'] })
@Unique({ name: 'upload_publication_reserved_uq', columns: ['reservedAssetId'] })
@Unique({ name: 'upload_publication_intent_uq', columns: ['intentId'] })
@Check({ name: 'upload_publication_state_check', expression: publicationStateCheck })
@Check({
  name: 'upload_publication_key_check',
  expression: `octet_length("idempotencyKey") BETWEEN 1 AND 200 AND fingerprint ~ '^v1:[a-f0-9]{64}$'`,
})
@ForeignKeyConstraint({
  columns: ['ownerId'],
  referenceTable: () => UserTable,
  referenceColumns: ['id'],
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
@ForeignKeyConstraint({
  name: 'upload_publication_source_fkey',
  indexName: 'upload_publication_source_identity_idx',
  columns: ['sourceId', 'ownerId', 'sourceState', 'sourceHash', 'sourceSize', 'sourceIdentity'],
  referenceTable: () => SourceReceiptTable,
  referenceColumns: ['id', 'ownerId', 'state', 'sha256', 'size', 'identity'],
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
@ForeignKeyConstraint({
  name: 'upload_publication_sidecar_fkey',
  indexName: 'upload_publication_sidecar_identity_idx',
  columns: ['sidecarId', 'ownerId', 'sourceState', 'sidecarHash', 'sidecarSize', 'sidecarIdentity'],
  referenceTable: () => SourceReceiptTable,
  referenceColumns: ['id', 'ownerId', 'state', 'sha256', 'size', 'identity'],
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
@ForeignKeyConstraint({
  name: 'upload_publication_stage_fkey',
  columns: ['stageId', 'sourceId', 'ownerId'],
  referenceTable: () => OwnedStageReceiptTable,
  referenceColumns: ['stageId', 'sourceId', 'ownerId'],
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
@ForeignKeyConstraint({
  name: 'upload_publication_intent_fkey',
  columns: ['intentId', 'stageId'],
  referenceTable: () => ObjectUploadIntentTable,
  referenceColumns: ['id', 'stageId'],
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
@ForeignKeyConstraint({
  name: 'upload_publication_version_fkey',
  indexName: 'upload_publication_version_identity_idx',
  columns: ['catalogVersionId', 'intentId', 'verification', 'provider', 'storeId', 'objectKey', 'versionId'],
  referenceTable: () => ObjectVersionTable,
  referenceColumns: ['id', 'intentId', 'verification', 'provider', 'storeId', 'key', 'versionId'],
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
@Check({
  name: 'upload_publication_evidence_check',
  expression: `provider = 's3' AND verification = 'verified' AND (result IS NULL OR ("catalogVersionId" IS NOT NULL AND "intentId" IS NOT NULL AND "exactAttempt" IS NOT NULL AND "objectKey" IS NOT NULL AND "versionId" IS NOT NULL))`,
})
@ForeignKeyConstraint({
  name: 'upload_publication_store_fkey',
  columns: ['storeId', 'endpoint', 'bucket'],
  referenceTable: () => OriginalStoreIdentityTable,
  referenceColumns: ['storeId', 'endpoint', 'bucket'],
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
@ForeignKeyConstraint({
  name: 'upload_publication_asset_fkey',
  columns: ['publishedAssetId', 'ownerId'],
  referenceTable: () => AssetTable,
  referenceColumns: ['id', 'ownerId'],
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
@ForeignKeyConstraint({
  name: 'upload_publication_claim_fkey',
  columns: ['claimId', 'publishedAssetId', 'ownerId', 'stageId'],
  referenceTable: () => AssetOriginalClaimTable,
  referenceColumns: ['id', 'assetId', 'ownerId', 'stageId'],
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
export class UploadPublicationTable {
  @PrimaryColumn({ type: 'uuid' })
  id!: string;
  @Column({ type: 'uuid' })
  ownerId!: string;
  @Column()
  idempotencyKey!: string;
  @Column()
  fingerprint!: string;
  @Column({ type: 'uuid' })
  reservedAssetId!: string;
  @Column({ type: 'uuid' })
  sourceId!: string;
  @Column({ default: 'sealed' })
  sourceState!: Generated<'sealed'>;
  @Column()
  sourceHash!: string;
  @Column({ type: 'bigint' })
  sourceSize!: string;
  @Column({ type: 'jsonb' })
  sourceIdentity!: StageIdentity;
  @Column({ nullable: true })
  sidecarHash!: string | null;
  @Column({ type: 'bigint', nullable: true })
  sidecarSize!: string | null;
  @Column({ type: 'jsonb', nullable: true })
  sidecarIdentity!: StageIdentity | null;
  @Column({ type: 'uuid', nullable: true })
  sidecarId!: string | null;
  @Column({ type: 'uuid', nullable: true })
  stageId!: string | null;
  @Column({ type: 'uuid', nullable: true })
  intentId!: string | null;
  @Column({ type: 'uuid', nullable: true })
  exactAttempt!: string | null;
  @Column({ type: 'uuid', nullable: true })
  catalogVersionId!: string | null;
  @Column({ default: 's3' })
  provider!: Generated<'s3'>;
  @Column({ default: 'verified' })
  verification!: Generated<'verified'>;
  @Column({ nullable: true })
  objectKey!: string | null;
  @Column({ nullable: true })
  versionId!: string | null;
  @Column({ type: 'uuid', nullable: true })
  publishedAssetId!: string | null;
  @Column({ type: 'uuid', nullable: true })
  claimId!: string | null;
  @Column()
  storeId!: string;
  @Column()
  endpoint!: string;
  @Column()
  bucket!: string;
  @Column({ type: 'jsonb' })
  options!: PublicationOptions;
  @Column({ type: 'jsonb' })
  auth!: PublicationAuth;
  @Column({ default: 'reserved' })
  state!: Generated<'reserved' | 'prepared' | 'held' | 'published' | 'duplicate'>;
  @Column({ type: 'jsonb', nullable: true })
  result!: PublicationResult | null;
}

@Table('upload_publication_outbox')
@Unique({ name: 'upload_publication_outbox_effect_uq', columns: ['receiptId', 'kind'] })
@Check({
  name: 'upload_publication_outbox_pending_check',
  expression: `state = 'pending' AND ((kind = 'metadata' AND target = 'AssetExtractMetadata') OR (kind IN ('event','workflow') AND target = 'AssetCreate'))`,
})
@ForeignKeyConstraint({
  columns: ['receiptId'],
  referenceTable: () => UploadPublicationTable,
  referenceColumns: ['id'],
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
export class UploadPublicationOutboxTable {
  @PrimaryColumn({ type: 'uuid' })
  id!: string;
  @Column({ type: 'uuid' })
  receiptId!: string;
  @Column()
  kind!: 'metadata' | 'event' | 'workflow';
  @Column()
  target!: 'AssetExtractMetadata' | 'AssetCreate';
  @Column({ default: 'pending' })
  state!: Generated<'pending'>;
  @Column({ type: 'jsonb' })
  payload!: { assetId: string; ownerId: string; quotaAlreadyCharged: true };
}
