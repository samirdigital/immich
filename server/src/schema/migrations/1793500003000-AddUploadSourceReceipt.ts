import { Kysely, sql } from 'kysely';

export async function up(db: Kysely<any>): Promise<void> {
  await sql`CREATE TABLE source_receipt (
    id uuid PRIMARY KEY,
    "ownerId" uuid NOT NULL,
    role character varying NOT NULL,
    root character varying NOT NULL,
    name character varying NOT NULL,
    "rootDev" character varying NOT NULL,
    "rootIno" character varying NOT NULL,
    state character varying NOT NULL DEFAULT 'writing',
    sha256 character varying,
    sha1 character varying,
    size bigint,
    identity jsonb,
    "createdAt" timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT source_receipt_owner_identity_uq UNIQUE (id, "ownerId"),
    CONSTRAINT source_receipt_sealed_identity_uq UNIQUE (id, "ownerId", state, sha256, size, identity),
    CONSTRAINT source_receipt_root_name_uq UNIQUE (root, name),
    CONSTRAINT "source_receipt_ownerId_fkey" FOREIGN KEY ("ownerId") REFERENCES "user"(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT source_receipt_role_check CHECK (role IN ('original','sidecar')),
    CONSTRAINT source_receipt_state_check CHECK (state IN ('writing','received','sealing','sealed','held')),
    CONSTRAINT source_receipt_name_check CHECK (name ~ '^[a-f0-9-]{36}[.][a-z0-9]{1,16}$' AND left(name, 36) = id::text),
    CONSTRAINT source_receipt_root_check CHECK (left(root, 1) = '/' AND "rootDev" ~ '^[0-9]+$' AND "rootIno" ~ '^[0-9]+$'),
    CONSTRAINT source_receipt_hash_check CHECK ((sha256 IS NULL OR sha256 ~ '^[a-f0-9]{64}$') AND (sha1 IS NULL OR sha1 ~ '^[a-f0-9]{40}$')),
    CONSTRAINT source_receipt_size_check CHECK (size IS NULL OR (size > 0 AND size <= 9007199254740991)),
    CONSTRAINT source_receipt_sealed_check CHECK (state NOT IN ('received','sealing','sealed') OR (identity IS NOT NULL AND sha256 IS NOT NULL AND sha1 IS NOT NULL AND size IS NOT NULL)),
    CONSTRAINT source_receipt_identity_check CHECK (identity IS NULL OR COALESCE(jsonb_typeof(identity) = 'object' AND identity->>'rootDev' = "rootDev" AND identity->>'rootIno' = "rootIno" AND identity->>'size' = size::text AND identity->>'nlink' = '1' AND (jsonb_typeof(identity->'dev') = 'string' AND identity->>'dev' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'ino') = 'string' AND identity->>'ino' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'size') = 'string' AND identity->>'size' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'mtimeNs') = 'string' AND identity->>'mtimeNs' ~ '^-?[0-9]+$') AND (jsonb_typeof(identity->'ctimeNs') = 'string' AND identity->>'ctimeNs' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'mode') = 'string' AND identity->>'mode' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'uid') = 'string' AND identity->>'uid' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'gid') = 'string' AND identity->>'gid' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'nlink') = 'string' AND identity->>'nlink' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'rootDev') = 'string' AND identity->>'rootDev' ~ '^[0-9]+$') AND (jsonb_typeof(identity->'rootIno') = 'string' AND identity->>'rootIno' ~ '^[0-9]+$'), false))
  )`.execute(db);
  await sql`CREATE INDEX "source_receipt_ownerId_idx" ON source_receipt ("ownerId")`.execute(db);
  await sql`CREATE TABLE owned_stage_receipt (
    "stageId" uuid PRIMARY KEY,
    "sourceId" uuid NOT NULL CONSTRAINT "owned_stage_receipt_sourceId_uq" UNIQUE,
    "ownerId" uuid NOT NULL,
    "sourceState" character varying NOT NULL,
    hash character varying NOT NULL,
    size bigint NOT NULL,
    "sourceIdentity" jsonb NOT NULL,
    CONSTRAINT owned_stage_receipt_owner_identity_uq UNIQUE ("stageId", "ownerId"),
    CONSTRAINT owned_stage_receipt_source_state_check CHECK ("sourceState" = 'sealed'),
    CONSTRAINT "owned_stage_receipt_stageId_fkey" FOREIGN KEY ("stageId") REFERENCES staged_object(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT owned_stage_receipt_source_fkey FOREIGN KEY ("sourceId", "ownerId", "sourceState", hash, size, "sourceIdentity") REFERENCES source_receipt(id, "ownerId", state, sha256, size, identity) ON DELETE RESTRICT ON UPDATE RESTRICT
  )`.execute(db);
  await sql`CREATE INDEX "owned_stage_receipt_stageId_idx" ON owned_stage_receipt ("stageId")`.execute(db);
  await sql`CREATE INDEX "owned_stage_receipt_source_identity_idx" ON owned_stage_receipt ("sourceId", "ownerId", "sourceState", hash, size, "sourceIdentity")`.execute(db);
}

export async function down(db: Kysely<any>): Promise<void> {
  await sql`DROP TABLE owned_stage_receipt`.execute(db);
  await sql`DROP TABLE source_receipt`.execute(db);
}
