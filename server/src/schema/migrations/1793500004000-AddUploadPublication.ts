import { Kysely, sql } from 'kysely';

export async function up(db: Kysely<any>): Promise<void> {
  await sql`ALTER TABLE owned_stage_receipt ADD CONSTRAINT owned_stage_receipt_publication_uq UNIQUE ("stageId", "sourceId", "ownerId")`.execute(db);
  await sql`CREATE TABLE upload_publication (
    id uuid PRIMARY KEY,
    "ownerId" uuid NOT NULL,
    "idempotencyKey" varchar NOT NULL,
    fingerprint varchar NOT NULL,
    "reservedAssetId" uuid NOT NULL,
    "sourceId" uuid NOT NULL,
    "sourceState" varchar NOT NULL DEFAULT 'sealed',
    "sourceHash" varchar NOT NULL,
    "sourceSize" bigint NOT NULL,
    "sourceIdentity" jsonb NOT NULL,
    "sidecarHash" varchar,
    "sidecarSize" bigint,
    "sidecarIdentity" jsonb,
    "sidecarId" uuid,
    "stageId" uuid,
    "intentId" uuid,
    "exactAttempt" uuid,
    "catalogVersionId" uuid,
    provider varchar NOT NULL DEFAULT 's3',
    verification varchar NOT NULL DEFAULT 'verified',
    "objectKey" varchar,
    "versionId" varchar,
    "publishedAssetId" uuid,
    "claimId" uuid,
    "storeId" varchar NOT NULL,
    endpoint varchar NOT NULL,
    bucket varchar NOT NULL,
    options jsonb NOT NULL,
    auth jsonb NOT NULL,
    state varchar NOT NULL DEFAULT 'reserved',
    result jsonb,
    CONSTRAINT upload_publication_owner_key_uq UNIQUE ("ownerId", "idempotencyKey"),
    CONSTRAINT upload_publication_source_uq UNIQUE ("sourceId"),
    CONSTRAINT upload_publication_reserved_uq UNIQUE ("reservedAssetId"),
    CONSTRAINT upload_publication_intent_uq UNIQUE ("intentId"),
    CONSTRAINT upload_publication_evidence_check CHECK (provider = 's3' AND verification = 'verified' AND (result IS NULL OR ("catalogVersionId" IS NOT NULL AND "intentId" IS NOT NULL AND "exactAttempt" IS NOT NULL AND "objectKey" IS NOT NULL AND "versionId" IS NOT NULL))),
    CONSTRAINT upload_publication_state_check CHECK (state IN ('reserved','prepared','held','published','duplicate') AND ((state IN ('published','duplicate')) = (result IS NOT NULL)) AND ((state = 'published') = ("publishedAssetId" IS NOT NULL)) AND ("publishedAssetId" IS NULL OR "publishedAssetId" = "reservedAssetId") AND (("claimId" IS NOT NULL) = ("publishedAssetId" IS NOT NULL)) AND ("intentId" IS NULL OR "stageId" IS NOT NULL) AND (state <> 'prepared' OR "intentId" IS NOT NULL)),
    CONSTRAINT upload_publication_source_check CHECK ("sourceState" = 'sealed' AND (("sidecarId" IS NULL AND "sidecarHash" IS NULL AND "sidecarSize" IS NULL AND "sidecarIdentity" IS NULL) OR ("sidecarId" IS NOT NULL AND "sidecarHash" IS NOT NULL AND "sidecarSize" IS NOT NULL AND "sidecarIdentity" IS NOT NULL))),
    CONSTRAINT upload_publication_key_check CHECK (octet_length("idempotencyKey") BETWEEN 1 AND 200 AND fingerprint ~ '^v1:[a-f0-9]{64}$'),
    CONSTRAINT "upload_publication_ownerId_fkey" FOREIGN KEY ("ownerId") REFERENCES "user"(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT upload_publication_source_fkey FOREIGN KEY ("sourceId", "ownerId", "sourceState", "sourceHash", "sourceSize", "sourceIdentity") REFERENCES source_receipt(id, "ownerId", state, sha256, size, identity) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT upload_publication_sidecar_fkey FOREIGN KEY ("sidecarId", "ownerId", "sourceState", "sidecarHash", "sidecarSize", "sidecarIdentity") REFERENCES source_receipt(id, "ownerId", state, sha256, size, identity) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT upload_publication_stage_fkey FOREIGN KEY ("stageId", "sourceId", "ownerId") REFERENCES owned_stage_receipt("stageId", "sourceId", "ownerId") ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT upload_publication_intent_fkey FOREIGN KEY ("intentId", "stageId") REFERENCES object_upload_intent(id, "stageId") ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT upload_publication_version_fkey FOREIGN KEY ("catalogVersionId", "intentId", verification, provider, "storeId", "objectKey", "versionId") REFERENCES object_version(id, "intentId", verification, provider, "storeId", key, "versionId") ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT upload_publication_store_fkey FOREIGN KEY ("storeId", endpoint, bucket) REFERENCES original_store_identity("storeId", endpoint, bucket) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT upload_publication_asset_fkey FOREIGN KEY ("publishedAssetId", "ownerId") REFERENCES asset(id, "ownerId") ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT upload_publication_claim_fkey FOREIGN KEY ("claimId", "publishedAssetId", "ownerId", "stageId") REFERENCES asset_original_claim(id, "assetId", "ownerId", "stageId") ON DELETE RESTRICT ON UPDATE RESTRICT
  )`.execute(db);
  for (const columns of [
    ['ownerId'], ['stageId', 'sourceId', 'ownerId'],
    ['intentId', 'stageId'], ['storeId', 'endpoint', 'bucket'], ['publishedAssetId', 'ownerId'], ['claimId', 'publishedAssetId', 'ownerId', 'stageId'],
  ]) {
    await sql.raw(`CREATE INDEX "upload_publication_${columns.join('_')}_idx" ON upload_publication (${columns.map((column) => `"${column}"`).join(', ')})`).execute(db);
  }
  await sql`CREATE INDEX upload_publication_version_identity_idx ON upload_publication ("catalogVersionId", "intentId", verification, provider, "storeId", "objectKey", "versionId")`.execute(db);
  await sql`CREATE INDEX upload_publication_source_identity_idx ON upload_publication ("sourceId", "ownerId", "sourceState", "sourceHash", "sourceSize", "sourceIdentity")`.execute(db);
  await sql`CREATE INDEX upload_publication_sidecar_identity_idx ON upload_publication ("sidecarId", "ownerId", "sourceState", "sidecarHash", "sidecarSize", "sidecarIdentity")`.execute(db);
  await sql`CREATE TABLE upload_publication_outbox (
    id uuid PRIMARY KEY,
    "receiptId" uuid NOT NULL REFERENCES upload_publication(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    kind varchar NOT NULL,
    target varchar NOT NULL,
    state varchar NOT NULL DEFAULT 'pending',
    payload jsonb NOT NULL,
    CONSTRAINT upload_publication_outbox_effect_uq UNIQUE ("receiptId", kind),
    CONSTRAINT upload_publication_outbox_pending_check CHECK (state = 'pending' AND ((kind = 'metadata' AND target = 'AssetExtractMetadata') OR (kind IN ('event','workflow') AND target = 'AssetCreate')))
  )`.execute(db);
  await sql`CREATE INDEX "upload_publication_outbox_receiptId_idx" ON upload_publication_outbox ("receiptId")`.execute(db);
}

export async function down(db: Kysely<any>): Promise<void> {
  await sql`DROP TABLE upload_publication_outbox; DROP TABLE upload_publication; ALTER TABLE owned_stage_receipt DROP CONSTRAINT owned_stage_receipt_publication_uq`.execute(db);
}
