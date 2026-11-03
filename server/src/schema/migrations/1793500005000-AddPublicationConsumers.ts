import { Kysely, sql } from 'kysely';

export async function up(db: Kysely<any>): Promise<void> {
  await sql`CREATE TABLE publication_consumer (
    id uuid PRIMARY KEY,
    "originId" uuid NOT NULL REFERENCES upload_publication_outbox(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    "receiptId" uuid NOT NULL REFERENCES upload_publication(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    "assetId" uuid NOT NULL,
    "ownerId" uuid NOT NULL REFERENCES "user"(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    kind varchar NOT NULL CHECK (kind IN ('metadata','event','workflow','metadata-event')),
    state varchar NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','claimed','done','held')),
    token uuid,
    "expiresAt" timestamptz,
    "dispatchUntil" timestamptz,
    result jsonb,
    "acknowledgedAt" timestamptz,
    CONSTRAINT publication_consumer_ack_check CHECK ("acknowledgedAt" IS NULL OR state='done'),
    UNIQUE ("originId",kind),
    CHECK ((state = 'claimed') = (token IS NOT NULL AND "expiresAt" IS NOT NULL))
  )`.execute(db);
  await sql`CREATE FUNCTION publication_consumer_identity() RETURNS trigger LANGUAGE plpgsql AS $$
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
    END $$;
    CREATE TRIGGER publication_consumer_identity BEFORE INSERT OR UPDATE ON publication_consumer
      FOR EACH ROW EXECUTE FUNCTION publication_consumer_identity()`.execute(db);
  await sql`CREATE TABLE publication_workflow_run (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    "effectId" uuid NOT NULL REFERENCES publication_consumer(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    "workflowId" uuid NOT NULL REFERENCES workflow(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    state varchar NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','claimed','done','held')),
    token uuid,
    "expiresAt" timestamptz,
    "dispatchUntil" timestamptz,
    result jsonb,
    UNIQUE ("effectId","workflowId"),
    CHECK ((state = 'claimed') = (token IS NOT NULL AND "expiresAt" IS NOT NULL))
  )`.execute(db);
  await sql`CREATE FUNCTION publication_run_identity() RETURNS trigger LANGUAGE plpgsql AS $$
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
    END $$;
    CREATE TRIGGER publication_run_identity BEFORE INSERT OR UPDATE ON publication_workflow_run
      FOR EACH ROW EXECUTE FUNCTION publication_run_identity()`.execute(db);
  await sql`CREATE INDEX publication_consumer_pending_idx ON publication_consumer ("dispatchUntil",id) WHERE state='pending';
    CREATE INDEX publication_consumer_expired_idx ON publication_consumer ("expiresAt") WHERE state='claimed';
    CREATE INDEX publication_workflow_run_pending_idx ON publication_workflow_run ("dispatchUntil",id) WHERE state='pending';
    CREATE INDEX publication_workflow_run_expired_idx ON publication_workflow_run ("expiresAt") WHERE state='claimed';
    CREATE INDEX "publication_consumer_originId_idx" ON publication_consumer ("originId");
    CREATE INDEX "publication_consumer_receiptId_idx" ON publication_consumer ("receiptId");
    CREATE INDEX "publication_consumer_ownerId_idx" ON publication_consumer ("ownerId");
    CREATE INDEX "publication_workflow_run_effectId_idx" ON publication_workflow_run ("effectId");
    CREATE INDEX "publication_workflow_run_workflowId_idx" ON publication_workflow_run ("workflowId")`.execute(db);
}

export async function down(db: Kysely<any>): Promise<void> {
  await sql`DROP TABLE publication_workflow_run; DROP TABLE publication_consumer; DROP FUNCTION publication_consumer_identity(); DROP FUNCTION publication_run_identity()`.execute(db);
}
