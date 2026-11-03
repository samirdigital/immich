import {
  Check,
  Column,
  CreateDateColumn,
  ForeignKeyColumn,
  ForeignKeyConstraint,
  type Generated,
  PrimaryColumn,
  Table,
  type Timestamp,
  Unique,
} from '@immich/sql-tools';
import { ObjectUploadIntentTable } from 'src/schema/tables/object-upload-intent.table.js';

export interface VerificationObservation {
  outcome: 'verified' | 'failed';
  detail: string;
  observedAt?: string;
}

@Table('object_version')
@Unique({
  name: 'object_version_id_intentId_verification_uq',
  columns: ['id', 'intentId', 'verification', 'provider', 'storeId', 'key', 'versionId'],
})
@ForeignKeyConstraint({
  name: 'object_version_intent_identity_fkey',
  columns: ['intentId', 'provider', 'storeId', 'key'],
  referenceColumns: ['id', 'provider', 'storeId', 'key'],
  referenceTable: () => ObjectUploadIntentTable,
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
@Unique({ columns: ['intentId'] })
@Check({
  name: 'object_version_identity_chk',
  expression: `length("storeId") > 0 AND length(key) > 0 AND ((provider = 's3' AND "versionId" IS NOT NULL AND length("versionId") > 0 AND "versionId" <> 'null') OR (provider = 'local' AND "versionId" IS NULL))`,
})
@Check({ name: 'object_version_verification_chk', expression: `verification IN ('pending','verified','failed')` })
export class ObjectVersionTable {
  @PrimaryColumn({ type: 'uuid' })
  id!: string;

  @ForeignKeyColumn(() => ObjectUploadIntentTable, { onDelete: 'RESTRICT', onUpdate: 'RESTRICT' })
  intentId!: string;

  @Column()
  provider!: 'local' | 's3';

  @Column()
  storeId!: string;

  @Column()
  key!: string;

  @Column({ nullable: true })
  versionId!: string | null;

  @Column({ nullable: true })
  etag!: string | null;

  @Column({ default: 'pending' })
  verification!: Generated<'pending' | 'verified' | 'failed'>;

  @Column({ type: 'jsonb', default: () => "'[]'::jsonb" })
  observations!: Generated<VerificationObservation[]>;

  @CreateDateColumn()
  createdAt!: Generated<Timestamp>;
}
