import {
  Check,
  Column,
  CreateDateColumn,
  ForeignKeyColumn,
  type Generated,
  PrimaryColumn,
  Table,
  type Timestamp,
  Unique,
} from '@immich/sql-tools';
import { StagedObjectTable } from 'src/schema/tables/staged-object.table.js';

@Table('object_upload_intent')
@Unique({ columns: ['id', 'provider', 'storeId', 'key', 'token'] })
@Unique({ columns: ['id', 'provider', 'storeId', 'key'] })
@Unique({ columns: ['provider', 'storeId', 'key'] })
@Check({
  name: 'object_upload_intent_state_chk',
  expression: `state IN ('prepared','sending','acknowledged','ambiguous')`,
})
@Check({
  name: 'object_upload_intent_identity_chk',
  expression: `provider IN ('local','s3') AND length("storeId") > 0 AND length(key) > 0`,
})
@Check({
  name: 'object_upload_intent_claim_chk',
  expression: `state = 'prepared' OR (token IS NOT NULL AND "leaseUntil" IS NOT NULL)`,
})
export class ObjectUploadIntentTable {
  @PrimaryColumn({ type: 'uuid' })
  id!: string;

  @ForeignKeyColumn(() => StagedObjectTable, { onDelete: 'RESTRICT', onUpdate: 'RESTRICT' })
  stageId!: string;

  @Column()
  provider!: 'local' | 's3';

  @Column()
  storeId!: string;

  @Column()
  key!: string;

  @Column()
  state!: 'prepared' | 'sending' | 'acknowledged' | 'ambiguous';

  @Column({ type: 'uuid', nullable: true })
  token!: string | null;

  @Column({ type: 'timestamp with time zone', nullable: true })
  leaseUntil!: Timestamp | null;

  @Column({ nullable: true })
  evidence!: string | null;

  @CreateDateColumn()
  createdAt!: Generated<Timestamp>;
}
