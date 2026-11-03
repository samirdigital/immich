import {
  Check,
  Column,
  CreateDateColumn,
  type Generated,
  PrimaryColumn,
  Table,
  Timestamp,
  Unique,
} from '@immich/sql-tools';

export interface StageIdentity {
  dev: string;
  ino: string;
  size: string;
  mtimeNs: string;
  ctimeNs: string;
  mode: string;
  uid: string;
  gid: string;
  nlink: string;
  rootDev: string;
  rootIno: string;
}

@Table('staged_object')
@Unique({ name: 'staged_object_publication_uq', columns: ['id', 'hash', 'size'] })
@Check({
  name: 'staged_object_identity_chk',
  expression: `identity IS NULL OR COALESCE((jsonb_typeof(identity) = 'object' AND (jsonb_typeof(identity->'dev') = 'string' AND identity->>'dev' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'ino') = 'string' AND identity->>'ino' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'size') = 'string' AND identity->>'size' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'mtimeNs') = 'string' AND identity->>'mtimeNs' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'ctimeNs') = 'string' AND identity->>'ctimeNs' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'mode') = 'string' AND identity->>'mode' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'rootDev') = 'string' AND identity->>'rootDev' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'rootIno') = 'string' AND identity->>'rootIno' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'uid') = 'string' AND identity->>'uid' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'gid') = 'string' AND identity->>'gid' ~ '^[0-9]+$') AND identity->>'nlink' = '1' AND identity->>'size' = size::text), false)`,
})
@Check({ name: 'staged_object_pins_chk', expression: `jsonb_typeof(pins) = 'array'` })
@Check({
  name: 'staged_object_state_chk',
  expression: `state IN ('creating','ready','quarantined','deleting','deleted')`,
})
@Check({ name: 'staged_object_hash_chk', expression: `hash IS NULL OR hash ~ '^[a-f0-9]{64}$'` })
@Check({ name: 'staged_object_size_chk', expression: `size IS NULL OR (size >= 0 AND size <= 9007199254740991)` })
@Check({
  name: 'staged_object_ready_chk',
  expression: `state NOT IN ('ready','deleting','deleted') OR (hash IS NOT NULL AND size IS NOT NULL AND identity IS NOT NULL)`,
})
export class StagedObjectTable {
  @PrimaryColumn({ type: 'uuid' })
  id!: string;

  @Column()
  root!: string;

  @Column()
  state!: 'creating' | 'ready' | 'quarantined' | 'deleting' | 'deleted';

  @Column({ nullable: true })
  hash!: string | null;

  @Column({ type: 'bigint', nullable: true })
  size!: string | null;

  @Column({ type: 'jsonb', nullable: true })
  identity!: StageIdentity | null;

  @Column({ type: 'boolean', default: false })
  released!: Generated<boolean>;

  @Column({ type: 'jsonb', default: () => "'[]'::jsonb" })
  pins!: Generated<string[]>;

  @Column({ nullable: true })
  evidence!: string | null;

  @CreateDateColumn()
  createdAt!: Generated<Timestamp>;
}
