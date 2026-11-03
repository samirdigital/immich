import {
  Check,
  Column,
  CreateDateColumn,
  ForeignKeyConstraint,
  type Generated,
  PrimaryColumn,
  Table,
  type Timestamp,
  Unique,
} from '@immich/sql-tools';
import { AssetTable } from 'src/schema/tables/asset.table.js';
import { ObjectUploadIntentTable } from 'src/schema/tables/object-upload-intent.table.js';
import { ObjectVersionTable } from 'src/schema/tables/object-version.table.js';
import { StagedObjectTable } from 'src/schema/tables/staged-object.table.js';

@Table('original_store_identity')
@Unique({ name: 'original_store_identity_binding_uq', columns: ['storeId', 'endpoint', 'bucket'] })
export class OriginalStoreIdentityTable {
  @PrimaryColumn()
  storeId!: string;
  @Column()
  endpoint!: string;
  @Column()
  bucket!: string;
}

@Table('asset_original_claim')
@Unique({ columns: ['assetId'] })
@Unique({ columns: ['stageId'] })
@Unique({ name: 'asset_original_claim_identity_uq', columns: ['id', 'assetId', 'ownerId', 'stageId'] })
@ForeignKeyConstraint({
  name: 'asset_original_claim_asset_fkey',
  columns: ['assetId', 'ownerId'],
  referenceTable: () => AssetTable,
  referenceColumns: ['id', 'ownerId'],
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
@ForeignKeyConstraint({
  name: 'asset_original_claim_stageId_fkey',
  columns: ['stageId'],
  referenceTable: () => StagedObjectTable,
  referenceColumns: ['id'],
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
export class AssetOriginalClaimTable {
  @PrimaryColumn({ type: 'uuid' })
  id!: string;
  @Column({ type: 'uuid' })
  assetId!: string;
  @Column({ type: 'uuid' })
  ownerId!: string;
  @Column({ type: 'uuid' })
  stageId!: string;
}

@Table('asset_original')
@ForeignKeyConstraint({
  name: 'asset_original_store_fkey',
  columns: ['storeId', 'endpoint', 'bucket'],
  referenceTable: () => OriginalStoreIdentityTable,
  referenceColumns: ['storeId', 'endpoint', 'bucket'],
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
@Unique({ columns: ['claimId'] })
@Unique({ columns: ['intentId'] })
@Unique({ columns: ['catalogVersionId'] })
@Check({ name: 'asset_original_provider_check', expression: "provider = 's3'" })
@ForeignKeyConstraint({
  name: 'asset_original_stage_fkey',
  columns: ['stageId', 'hash', 'size'],
  referenceTable: () => StagedObjectTable,
  referenceColumns: ['id', 'hash', 'size'],
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
@Check({ name: 'asset_original_verification_check', expression: "verification = 'verified'" })
@ForeignKeyConstraint({
  name: 'asset_original_claim_fkey',
  columns: ['claimId', 'assetId', 'ownerId', 'stageId'],
  referenceTable: () => AssetOriginalClaimTable,
  referenceColumns: ['id', 'assetId', 'ownerId', 'stageId'],
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
@ForeignKeyConstraint({
  name: 'asset_original_intent_fkey',
  columns: ['intentId', 'stageId'],
  referenceTable: () => ObjectUploadIntentTable,
  referenceColumns: ['id', 'stageId'],
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
@ForeignKeyConstraint({
  name: 'asset_original_version_fkey',
  indexName: 'asset_original_version_identity_idx',
  columns: ['catalogVersionId', 'intentId', 'verification', 'provider', 'storeId', 'key', 'versionId'],
  referenceTable: () => ObjectVersionTable,
  referenceColumns: ['id', 'intentId', 'verification', 'provider', 'storeId', 'key', 'versionId'],
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
export class AssetOriginalTable {
  @PrimaryColumn({ type: 'uuid' })
  assetId!: string;
  @Column({ type: 'uuid' })
  ownerId!: string;
  @Column({ type: 'uuid' })
  stageId!: string;
  @Column({ type: 'uuid' })
  claimId!: string;
  @Column({ type: 'uuid' })
  intentId!: string;
  @Column({ type: 'uuid' })
  catalogVersionId!: string;
  @Column()
  provider!: 's3';
  @Column()
  storeId!: string;
  @Column()
  key!: string;
  @Column()
  endpoint!: string;
  @Column()
  bucket!: string;
  @Column()
  versionId!: string;
  @Column()
  hash!: string;
  @Column({ type: 'bigint' })
  size!: string;
  @Column({ default: 'verified' })
  verification!: Generated<'verified'>;
  @CreateDateColumn()
  publishedAt!: Generated<Timestamp>;
}
