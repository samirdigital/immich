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
import { StageIdentity } from 'src/schema/tables/staged-object.table.js';
import { UserTable } from 'src/schema/tables/user.table.js';

@Table('source_receipt')
@Unique({
  name: 'source_receipt_sealed_identity_uq',
  columns: ['id', 'ownerId', 'state', 'sha256', 'size', 'identity'],
})
@Unique({ name: 'source_receipt_owner_identity_uq', columns: ['id', 'ownerId'] })
@Unique({ columns: ['root', 'name'] })
@ForeignKeyConstraint({
  columns: ['ownerId'],
  referenceTable: () => UserTable,
  referenceColumns: ['id'],
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
@Check({ name: 'source_receipt_role_check', expression: "role IN ('original','sidecar')" })
@Check({ name: 'source_receipt_state_check', expression: "state IN ('writing','received','sealing','sealed','held')" })
@Check({
  name: 'source_receipt_name_check',
  expression: "name ~ '^[a-f0-9-]{36}[.][a-z0-9]{1,16}$' AND left(name, 36) = id::text",
})
@Check({
  name: 'source_receipt_root_check',
  expression: `left(root, 1) = '/' AND "rootDev" ~ '^[0-9]+$' AND "rootIno" ~ '^[0-9]+$'`,
})
@Check({
  name: 'source_receipt_hash_check',
  expression: "(sha256 IS NULL OR sha256 ~ '^[a-f0-9]{64}$') AND (sha1 IS NULL OR sha1 ~ '^[a-f0-9]{40}$')",
})
@Check({ name: 'source_receipt_size_check', expression: 'size IS NULL OR (size > 0 AND size <= 9007199254740991)' })
@Check({
  name: 'source_receipt_sealed_check',
  expression:
    "state NOT IN ('received','sealing','sealed') OR (identity IS NOT NULL AND sha256 IS NOT NULL AND sha1 IS NOT NULL AND size IS NOT NULL)",
})
@Check({
  name: 'source_receipt_identity_check',
  expression: `identity IS NULL OR COALESCE(jsonb_typeof(identity) = 'object' AND identity->>'rootDev' = "rootDev" AND identity->>'rootIno' = "rootIno" AND identity->>'size' = size::text AND identity->>'nlink' = '1' AND (jsonb_typeof(identity->'dev') = 'string' AND identity->>'dev' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'ino') = 'string' AND identity->>'ino' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'size') = 'string' AND identity->>'size' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'mtimeNs') = 'string' AND identity->>'mtimeNs' ~ '^-?[0-9]+$') AND (jsonb_typeof(identity->'ctimeNs') = 'string' AND identity->>'ctimeNs' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'mode') = 'string' AND identity->>'mode' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'uid') = 'string' AND identity->>'uid' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'gid') = 'string' AND identity->>'gid' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'nlink') = 'string' AND identity->>'nlink' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'rootDev') = 'string' AND identity->>'rootDev' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'rootIno') = 'string' AND identity->>'rootIno' ~ '^[0-9]+$'), false)`,
})
export class SourceReceiptTable {
  @PrimaryColumn({ type: 'uuid' })
  id!: string;
  @Column({ type: 'uuid' })
  ownerId!: string;
  @Column()
  role!: 'original' | 'sidecar';
  @Column()
  root!: string;
  @Column()
  name!: string;
  @Column()
  rootDev!: string;
  @Column()
  rootIno!: string;
  @Column({ default: 'writing' })
  state!: Generated<'writing' | 'received' | 'sealing' | 'sealed' | 'held'>;
  @Column({ nullable: true })
  sha256!: string | null;
  @Column({ nullable: true })
  sha1!: string | null;
  @Column({ type: 'bigint', nullable: true })
  size!: string | null;
  @Column({ type: 'jsonb', nullable: true })
  identity!: StageIdentity | null;
  @CreateDateColumn()
  createdAt!: Generated<Timestamp>;
}
