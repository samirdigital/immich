import { Check, Column, ForeignKeyConstraint, PrimaryColumn, Table, Unique } from '@immich/sql-tools';
import { SourceReceiptTable } from 'src/schema/tables/source-receipt.table.js';
import { type StageIdentity, StagedObjectTable } from 'src/schema/tables/staged-object.table.js';

@Table('owned_stage_receipt')
@Unique({ name: 'owned_stage_receipt_publication_uq', columns: ['stageId', 'sourceId', 'ownerId'] })
@Unique({ columns: ['sourceId'] })
@Unique({ name: 'owned_stage_receipt_owner_identity_uq', columns: ['stageId', 'ownerId'] })
@Check({ name: 'owned_stage_receipt_source_state_check', expression: `"sourceState" = 'sealed'` })
@ForeignKeyConstraint({
  columns: ['stageId'],
  referenceTable: () => StagedObjectTable,
  referenceColumns: ['id'],
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
@ForeignKeyConstraint({
  name: 'owned_stage_receipt_source_fkey',
  indexName: 'owned_stage_receipt_source_identity_idx',
  columns: ['sourceId', 'ownerId', 'sourceState', 'hash', 'size', 'sourceIdentity'],
  referenceTable: () => SourceReceiptTable,
  referenceColumns: ['id', 'ownerId', 'state', 'sha256', 'size', 'identity'],
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
export class OwnedStageReceiptTable {
  @PrimaryColumn({ type: 'uuid' })
  stageId!: string;
  @Column({ type: 'uuid' })
  sourceId!: string;
  @Column({ type: 'uuid' })
  ownerId!: string;
  @Column()
  sourceState!: 'sealed';
  @Column()
  hash!: string;
  @Column({ type: 'bigint' })
  size!: string;
  @Column({ type: 'jsonb' })
  sourceIdentity!: StageIdentity;
}
