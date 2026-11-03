import {
  Check,
  Column,
  ForeignKeyConstraint,
  type Generated,
  Index,
  PrimaryColumn,
  Table,
  type Timestamp,
  TriggerFunction,
  Unique,
  registerFunction,
} from '@immich/sql-tools';
import { UploadPublicationOutboxTable, UploadPublicationTable } from 'src/schema/tables/upload-publication.table.js';
import { UserTable } from 'src/schema/tables/user.table.js';
import { WorkflowTable } from 'src/schema/tables/workflow.table.js';

export const publication_consumer_identity = registerFunction({
  name: 'publication_consumer_identity',
  returnType: 'TRIGGER',
  language: 'PLPGSQL',
  body: `
    BEGIN
      IF TG_OP = 'UPDATE' AND (NEW.id,NEW."originId",NEW."receiptId",NEW."assetId",NEW."ownerId",NEW.kind)
        IS DISTINCT FROM (OLD.id,OLD."originId",OLD."receiptId",OLD."assetId",OLD."ownerId",OLD.kind) THEN
        RAISE EXCEPTION 'Immutable publication consumer identity';
      END IF;
      IF NOT EXISTS (SELECT 1 FROM upload_publication_outbox o JOIN upload_publication p ON p.id=o."receiptId"
        WHERE o.id=NEW."originId" AND p.id=NEW."receiptId" AND p.state='published'
        AND p."publishedAssetId"=NEW."assetId" AND p."ownerId"=NEW."ownerId"
        AND o.payload=jsonb_build_object('assetId',NEW."assetId",'ownerId',NEW."ownerId",'quotaAlreadyCharged',true)
        AND ((NEW.id=o.id AND NEW.kind=o.kind) OR (NEW.kind='metadata-event' AND o.kind='metadata'))) THEN
        RAISE EXCEPTION 'Invalid publication consumer identity';
      END IF;
      RETURN NEW;
    END `,
});

export const publication_run_identity = registerFunction({
  name: 'publication_run_identity',
  returnType: 'TRIGGER',
  language: 'PLPGSQL',
  body: `
    DECLARE effect publication_consumer;
    BEGIN
      IF TG_OP = 'UPDATE' THEN
        IF (NEW.id,NEW."effectId",NEW."workflowId") IS DISTINCT FROM (OLD.id,OLD."effectId",OLD."workflowId") THEN
          RAISE EXCEPTION 'Immutable publication run identity';
        END IF;
        RETURN NEW;
      END IF;
      SELECT * INTO effect FROM publication_consumer WHERE id=NEW."effectId" FOR UPDATE;
      IF effect.state NOT IN ('pending','claimed') OR effect.kind NOT IN ('workflow','metadata-event') OR NOT EXISTS (
        SELECT 1 FROM workflow w WHERE w.id=NEW."workflowId" AND w."ownerId"=effect."ownerId" AND w.enabled
          AND w.trigger=CASE WHEN effect.kind='workflow' THEN 'AssetCreate' ELSE 'AssetMetadataExtraction' END
      ) THEN
        RAISE EXCEPTION 'Invalid or frozen publication workflow cohort';
      END IF;
      RETURN NEW;
    END `,
});

@Table('publication_consumer')
@Check({ name: 'publication_consumer_ack_check', expression: `"acknowledgedAt" IS NULL OR state='done'` })
@Unique({ name: 'publication_consumer_originId_kind_key', columns: ['originId', 'kind'] })
@Check({ name: 'publication_consumer_state_check', expression: "state IN ('pending','claimed','done','held')" })
@Check({
  name: 'publication_consumer_check',
  expression: `(state = 'claimed') = (token IS NOT NULL AND "expiresAt" IS NOT NULL)`,
})
@Index({ name: 'publication_consumer_pending_idx', columns: ['dispatchUntil', 'id'], where: "state = 'pending'" })
@Index({ name: 'publication_consumer_expired_idx', columns: ['expiresAt'], where: "state = 'claimed'" })
@TriggerFunction({
  name: 'publication_consumer_identity',
  timing: 'before',
  actions: ['insert', 'update'],
  scope: 'row',
  function: publication_consumer_identity,
})
@Check({
  name: 'publication_consumer_kind_check',
  expression: "kind IN ('metadata','event','workflow','metadata-event')",
})
@ForeignKeyConstraint({
  name: 'publication_consumer_originId_fkey',
  columns: ['originId'],
  referenceTable: () => UploadPublicationOutboxTable,
  referenceColumns: ['id'],
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
@ForeignKeyConstraint({
  name: 'publication_consumer_receiptId_fkey',
  columns: ['receiptId'],
  referenceTable: () => UploadPublicationTable,
  referenceColumns: ['id'],
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
@ForeignKeyConstraint({
  name: 'publication_consumer_ownerId_fkey',
  columns: ['ownerId'],
  referenceTable: () => UserTable,
  referenceColumns: ['id'],
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
export class PublicationConsumerTable {
  @PrimaryColumn({ type: 'uuid' })
  id!: string;
  @Column({ type: 'uuid' })
  originId!: string;
  @Column({ type: 'uuid' })
  receiptId!: string;
  @Column({ type: 'uuid' })
  assetId!: string;
  @Column({ type: 'uuid' })
  ownerId!: string;
  @Column({ type: 'character varying' })
  kind!: 'metadata' | 'event' | 'workflow' | 'metadata-event';
  @Column({ type: 'character varying', default: 'pending' })
  state!: Generated<'pending' | 'claimed' | 'done' | 'held'>;
  @Column({ type: 'uuid', nullable: true })
  token!: string | null;
  @Column({ type: 'timestamp with time zone', nullable: true })
  expiresAt!: Timestamp | null;
  @Column({ type: 'timestamp with time zone', nullable: true })
  dispatchUntil!: Timestamp | null;
  @Column({ type: 'jsonb', nullable: true })
  result!: Record<string, unknown> | null;
  @Column({ type: 'timestamp with time zone', nullable: true })
  acknowledgedAt!: Timestamp | null;
}

@Table('publication_workflow_run')
@Unique({ name: 'publication_workflow_run_effectId_workflowId_key', columns: ['effectId', 'workflowId'] })
@Check({ name: 'publication_workflow_run_state_check', expression: "state IN ('pending','claimed','done','held')" })
@Check({
  name: 'publication_workflow_run_check',
  expression: `(state = 'claimed') = (token IS NOT NULL AND "expiresAt" IS NOT NULL)`,
})
@Index({ name: 'publication_workflow_run_pending_idx', columns: ['dispatchUntil', 'id'], where: "state = 'pending'" })
@Index({ name: 'publication_workflow_run_expired_idx', columns: ['expiresAt'], where: "state = 'claimed'" })
@TriggerFunction({
  name: 'publication_run_identity',
  timing: 'before',
  actions: ['insert', 'update'],
  scope: 'row',
  function: publication_run_identity,
})
@ForeignKeyConstraint({
  name: 'publication_workflow_run_effectId_fkey',
  columns: ['effectId'],
  referenceTable: () => PublicationConsumerTable,
  referenceColumns: ['id'],
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
@ForeignKeyConstraint({
  name: 'publication_workflow_run_workflowId_fkey',
  columns: ['workflowId'],
  referenceTable: () => WorkflowTable,
  referenceColumns: ['id'],
  onDelete: 'RESTRICT',
  onUpdate: 'RESTRICT',
})
export class PublicationWorkflowRunTable {
  @PrimaryColumn({ type: 'uuid', default: () => 'gen_random_uuid()' })
  id!: Generated<string>;
  @Column({ type: 'uuid' })
  effectId!: string;
  @Column({ type: 'uuid' })
  workflowId!: string;
  @Column({ type: 'character varying', default: 'pending' })
  state!: Generated<'pending' | 'claimed' | 'done' | 'held'>;
  @Column({ type: 'uuid', nullable: true })
  token!: string | null;
  @Column({ type: 'timestamp with time zone', nullable: true })
  expiresAt!: Timestamp | null;
  @Column({ type: 'timestamp with time zone', nullable: true })
  dispatchUntil!: Timestamp | null;
  @Column({ type: 'jsonb', nullable: true })
  result!: Record<string, unknown> | null;
}
