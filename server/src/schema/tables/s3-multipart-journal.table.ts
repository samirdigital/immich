import {
  Check,
  Column,
  CreateDateColumn,
  ForeignKeyConstraint,
  type Generated,
  PrimaryColumn,
  Table,
  type Timestamp,
} from '@immich/sql-tools';
import { ObjectUploadIntentTable } from 'src/schema/tables/object-upload-intent.table.js';

@Table('s3_multipart_journal')
@ForeignKeyConstraint({
  name: 's3_multipart_journal_intent_fkey',
  columns: ['intentId', 'provider', 'storeId', 'key', 'token'],
  referenceColumns: ['id', 'provider', 'storeId', 'key', 'token'],
  referenceTable: () => ObjectUploadIntentTable,
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
@Check({
  name: 's3_multipart_journal_binding_chk',
  expression: `provider = 's3' AND length("storeId") > 0 AND length(bucket) > 0 AND length(key) > 0`,
})
@Check({
  name: 's3_multipart_journal_state_chk',
  expression: `state IN ('creating','uploading','completing','received') AND (state <> 'creating' OR ("uploadId" IS NULL AND response IS NULL)) AND (state = 'creating' OR ("uploadId" IS NOT NULL AND length("uploadId") > 0 AND octet_length("uploadId") <= 4096)) AND (state NOT IN ('creating','uploading') OR manifest = '[]'::jsonb) AND (state NOT IN ('completing','received') OR jsonb_array_length(manifest) = "partCount") AND (state <> 'received' OR response IS NOT NULL)`,
})
@Check({
  name: 's3_multipart_journal_bounds_chk',
  expression: `size >= 0 AND size <= 9007199254740991 AND hash ~ '^[a-f0-9]{64}$' AND "partSize" BETWEEN 5242880 AND 67108864 AND "partCount" BETWEEN 1 AND 10000`,
})
@Check({
  name: 's3_multipart_journal_evidence_chk',
  expression: `jsonb_typeof(evidence) = 'array' AND jsonb_array_length(evidence) <= 32 AND octet_length(evidence::text) <= 32768 AND jsonb_typeof(manifest) = 'array' AND jsonb_array_length(manifest) <= 10000 AND (response IS NULL OR jsonb_typeof(response) = 'object')`,
})
export class S3MultipartJournalTable {
  @PrimaryColumn({ type: 'uuid' })
  intentId!: string;

  @Column()
  provider!: 's3';

  @Column()
  storeId!: string;

  @Column()
  bucket!: string;

  @Column()
  key!: string;

  @Column({ type: 'uuid' })
  token!: string;

  @Column()
  state!: 'creating' | 'uploading' | 'completing' | 'received';

  @Column({ nullable: true })
  uploadId!: string | null;

  @Column({ type: 'bigint' })
  size!: string;

  @Column()
  hash!: string;

  @Column({ type: 'integer' })
  partSize!: number;

  @Column({ type: 'integer' })
  partCount!: number;

  @Column({ type: 'jsonb', default: () => "'[]'::jsonb" })
  manifest!: Generated<{ PartNumber: number; ETag: string }[]>;

  @Column({ type: 'jsonb', nullable: true })
  response!: { versionId?: string; etag?: string } | null;

  @Column({ type: 'jsonb', default: () => "'[]'::jsonb" })
  evidence!: Generated<Record<string, unknown>[]>;

  @CreateDateColumn()
  createdAt!: Generated<Timestamp>;
}
