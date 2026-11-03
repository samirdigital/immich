import { Kysely, sql } from 'kysely';

export async function up(db: Kysely<any>): Promise<void> {
  await sql`CREATE TABLE original_store_identity ("storeId" character varying PRIMARY KEY, endpoint character varying NOT NULL, bucket character varying NOT NULL, CONSTRAINT original_store_identity_binding_uq UNIQUE ("storeId", endpoint, bucket))`.execute(db);
  await sql`ALTER TABLE asset ADD COLUMN "originalAuthority" character varying NOT NULL DEFAULT 'local', ADD CONSTRAINT asset_original_authority_chk CHECK ("originalAuthority" IN ('local','object')), ADD CONSTRAINT "asset_id_ownerId_uq" UNIQUE (id, "ownerId")`.execute(db);
  await sql`ALTER TABLE object_upload_intent ADD CONSTRAINT "object_upload_intent_id_stageId_uq" UNIQUE (id, "stageId")`.execute(db);
  await sql`ALTER TABLE object_version ADD CONSTRAINT "object_version_id_intentId_verification_uq" UNIQUE (id, "intentId", verification, provider, "storeId", key, "versionId")`.execute(db);
  await sql`ALTER TABLE staged_object ADD CONSTRAINT staged_object_publication_uq UNIQUE (id, hash, size)`.execute(db);
  await sql`CREATE TABLE asset_original_claim (
    id uuid PRIMARY KEY,
    "assetId" uuid NOT NULL CONSTRAINT "asset_original_claim_assetId_uq" UNIQUE,
    "ownerId" uuid NOT NULL,
    "stageId" uuid NOT NULL CONSTRAINT "asset_original_claim_stageId_uq" UNIQUE REFERENCES staged_object(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT asset_original_claim_asset_fkey FOREIGN KEY ("assetId", "ownerId") REFERENCES asset(id, "ownerId") ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT asset_original_claim_identity_uq UNIQUE (id, "assetId", "ownerId", "stageId")
  )`.execute(db);
  await sql`CREATE TABLE asset_original (
    "assetId" uuid PRIMARY KEY,
    "ownerId" uuid NOT NULL,
    "stageId" uuid NOT NULL,
    "claimId" uuid NOT NULL CONSTRAINT "asset_original_claimId_uq" UNIQUE,
    "intentId" uuid NOT NULL CONSTRAINT "asset_original_intentId_uq" UNIQUE,
    "catalogVersionId" uuid NOT NULL CONSTRAINT "asset_original_catalogVersionId_uq" UNIQUE,
    provider character varying NOT NULL CHECK (provider = 's3'),
    "storeId" character varying NOT NULL,
    endpoint character varying NOT NULL,
    bucket character varying NOT NULL,
    key character varying NOT NULL,
    "versionId" character varying NOT NULL,
    hash character varying NOT NULL,
    size bigint NOT NULL,
    verification character varying NOT NULL DEFAULT 'verified' CHECK (verification = 'verified'),
    "publishedAt" timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT asset_original_claim_fkey FOREIGN KEY ("claimId", "assetId", "ownerId", "stageId") REFERENCES asset_original_claim(id, "assetId", "ownerId", "stageId") ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT asset_original_intent_fkey FOREIGN KEY ("intentId", "stageId") REFERENCES object_upload_intent(id, "stageId") ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT asset_original_store_fkey FOREIGN KEY ("storeId", endpoint, bucket) REFERENCES original_store_identity("storeId", endpoint, bucket) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT asset_original_stage_fkey FOREIGN KEY ("stageId", hash, size) REFERENCES staged_object(id, hash, size) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT asset_original_version_fkey FOREIGN KEY ("catalogVersionId", "intentId", verification, provider, "storeId", key, "versionId") REFERENCES object_version(id, "intentId", verification, provider, "storeId", key, "versionId") ON DELETE RESTRICT ON UPDATE RESTRICT
  )`.execute(db);
  await sql`CREATE INDEX "asset_original_claim_stageId_idx" ON asset_original_claim ("stageId")`.execute(db);
  await sql`CREATE INDEX "asset_original_claim_assetId_ownerId_idx" ON asset_original_claim ("assetId", "ownerId")`.execute(db);
  await sql`CREATE INDEX "asset_original_version_identity_idx" ON asset_original ("catalogVersionId", "intentId", verification, provider, "storeId", key, "versionId")`.execute(db);
  await sql`CREATE INDEX "asset_original_intentId_stageId_idx" ON asset_original ("intentId", "stageId")`.execute(db);
  await sql`CREATE INDEX "asset_original_claimId_assetId_ownerId_stageId_idx" ON asset_original ("claimId", "assetId", "ownerId", "stageId")`.execute(db);
  await sql`CREATE INDEX "asset_original_stageId_hash_size_idx" ON asset_original ("stageId", hash, size)`.execute(db);
  await sql`CREATE INDEX "asset_original_storeId_endpoint_bucket_idx" ON asset_original ("storeId", endpoint, bucket)`.execute(db);
}

export async function down(db: Kysely<any>): Promise<void> {
  await sql`DROP TABLE asset_original`.execute(db);
  await sql`DROP TABLE original_store_identity`.execute(db);
  await sql`DROP TABLE asset_original_claim`.execute(db);
  await sql`ALTER TABLE staged_object DROP CONSTRAINT staged_object_publication_uq`.execute(db);
  await sql`ALTER TABLE object_version DROP CONSTRAINT "object_version_id_intentId_verification_uq"`.execute(db);
  await sql`ALTER TABLE object_upload_intent DROP CONSTRAINT "object_upload_intent_id_stageId_uq"`.execute(db);
  await sql`ALTER TABLE asset DROP CONSTRAINT "asset_id_ownerId_uq", DROP CONSTRAINT asset_original_authority_chk, DROP COLUMN "originalAuthority"`.execute(db);
}
