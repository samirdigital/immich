import { Kysely, sql } from 'kysely';

export async function up(db: Kysely<any>): Promise<void> {
  await sql`CREATE TABLE staged_object (
    id uuid PRIMARY KEY,
    root character varying NOT NULL,
    state character varying NOT NULL,
    hash character varying,
    size bigint,
    identity jsonb,
    released boolean NOT NULL DEFAULT false,
    pins jsonb NOT NULL DEFAULT '[]'::jsonb,
    evidence character varying,
    "createdAt" timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT staged_object_identity_chk CHECK (identity IS NULL OR COALESCE((jsonb_typeof(identity) = 'object' AND (jsonb_typeof(identity->'dev') = 'string' AND identity->>'dev' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'ino') = 'string' AND identity->>'ino' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'size') = 'string' AND identity->>'size' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'mtimeNs') = 'string' AND identity->>'mtimeNs' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'ctimeNs') = 'string' AND identity->>'ctimeNs' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'mode') = 'string' AND identity->>'mode' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'rootDev') = 'string' AND identity->>'rootDev' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'rootIno') = 'string' AND identity->>'rootIno' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'uid') = 'string' AND identity->>'uid' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'gid') = 'string' AND identity->>'gid' ~ '^[0-9]+$') AND identity->>'nlink' = '1' AND identity->>'size' = size::text), false)),
    CONSTRAINT staged_object_pins_chk CHECK (jsonb_typeof(pins) = 'array'),
    CONSTRAINT staged_object_state_chk CHECK (state IN ('creating','ready','quarantined','deleting','deleted')),
    CONSTRAINT staged_object_hash_chk CHECK (hash IS NULL OR hash ~ '^[a-f0-9]{64}$'),
    CONSTRAINT staged_object_size_chk CHECK (size IS NULL OR (size >= 0 AND size <= 9007199254740991)),
    CONSTRAINT staged_object_ready_chk CHECK (state NOT IN ('ready','deleting','deleted') OR (hash IS NOT NULL AND size IS NOT NULL AND identity IS NOT NULL))
  )`.execute(db);
  await sql`CREATE TABLE object_upload_intent (
    id uuid PRIMARY KEY,
    "stageId" uuid NOT NULL REFERENCES staged_object(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    provider character varying NOT NULL,
    "storeId" character varying NOT NULL,
    key character varying NOT NULL,
    state character varying NOT NULL,
    token uuid,
    "leaseUntil" timestamptz,
    evidence character varying,
    "createdAt" timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT object_upload_intent_id_provider_storeId_key_uq UNIQUE (id, provider, "storeId", key),
    CONSTRAINT object_upload_intent_provider_storeId_key_uq UNIQUE (provider, "storeId", key),
    CONSTRAINT object_upload_intent_state_chk CHECK (state IN ('prepared','sending','acknowledged','ambiguous')),
    CONSTRAINT object_upload_intent_identity_chk CHECK (provider IN ('local','s3') AND length("storeId") > 0 AND length(key) > 0),
    CONSTRAINT object_upload_intent_claim_chk CHECK (state = 'prepared' OR (token IS NOT NULL AND "leaseUntil" IS NOT NULL))
  )`.execute(db);
  await sql`CREATE TABLE object_version (
    id uuid PRIMARY KEY,
    "intentId" uuid NOT NULL REFERENCES object_upload_intent(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    provider character varying NOT NULL,
    "storeId" character varying NOT NULL,
    key character varying NOT NULL,
    "versionId" character varying,
    etag character varying,
    verification character varying NOT NULL DEFAULT 'pending',
    observations jsonb NOT NULL DEFAULT '[]'::jsonb,
    "createdAt" timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT object_version_intent_identity_fkey FOREIGN KEY ("intentId", provider, "storeId", key) REFERENCES object_upload_intent(id, provider, "storeId", key) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT object_version_intentId_uq UNIQUE ("intentId"),
    CONSTRAINT object_version_identity_chk CHECK (length("storeId") > 0 AND length(key) > 0 AND ((provider = 's3' AND "versionId" IS NOT NULL AND length("versionId") > 0 AND "versionId" <> 'null') OR (provider = 'local' AND "versionId" IS NULL))),
    CONSTRAINT object_version_verification_chk CHECK (verification IN ('pending','verified','failed'))
  )`.execute(db);
  await sql`CREATE INDEX "object_upload_intent_stageId_idx" ON object_upload_intent ("stageId")`.execute(db);
  await sql`CREATE INDEX "object_version_intentId_idx" ON object_version ("intentId")`.execute(db);
  await sql`CREATE INDEX "object_version_intentId_provider_storeId_key_idx" ON object_version ("intentId", provider, "storeId", key)`.execute(db);
}

export async function down(db: Kysely<any>): Promise<void> {
  await sql`DROP TABLE object_version`.execute(db);
  await sql`DROP TABLE object_upload_intent`.execute(db);
  await sql`DROP TABLE staged_object`.execute(db);
}
