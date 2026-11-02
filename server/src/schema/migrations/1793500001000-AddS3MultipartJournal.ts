import { Kysely, sql } from 'kysely';

export async function up(db: Kysely<any>): Promise<void> {
  await sql`ALTER TABLE object_upload_intent ADD CONSTRAINT "object_upload_intent_id_provider_storeId_key_token_uq" UNIQUE (id, provider, "storeId", key, token)`.execute(db);
  await sql`CREATE TABLE s3_multipart_journal (
    "intentId" uuid PRIMARY KEY,
    provider character varying NOT NULL,
    "storeId" character varying NOT NULL,
    bucket character varying NOT NULL,
    key character varying NOT NULL,
    token uuid NOT NULL,
    state character varying NOT NULL,
    "uploadId" character varying,
    size bigint NOT NULL,
    hash character varying NOT NULL,
    "partSize" integer NOT NULL,
    "partCount" integer NOT NULL,
    manifest jsonb NOT NULL DEFAULT '[]'::jsonb,
    response jsonb,
    evidence jsonb NOT NULL DEFAULT '[]'::jsonb,
    "createdAt" timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT s3_multipart_journal_intent_fkey FOREIGN KEY ("intentId", provider, "storeId", key, token) REFERENCES object_upload_intent(id, provider, "storeId", key, token) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT s3_multipart_journal_binding_chk CHECK (provider = 's3' AND length("storeId") > 0 AND length(bucket) > 0 AND length(key) > 0),
    CONSTRAINT s3_multipart_journal_state_chk CHECK (state IN ('creating','uploading','completing','received') AND (state <> 'creating' OR ("uploadId" IS NULL AND response IS NULL)) AND (state = 'creating' OR ("uploadId" IS NOT NULL AND length("uploadId") > 0 AND octet_length("uploadId") <= 4096)) AND (state NOT IN ('creating','uploading') OR manifest = '[]'::jsonb) AND (state NOT IN ('completing','received') OR jsonb_array_length(manifest) = "partCount") AND (state <> 'received' OR response IS NOT NULL)),
    CONSTRAINT s3_multipart_journal_bounds_chk CHECK (size >= 0 AND size <= 9007199254740991 AND hash ~ '^[a-f0-9]{64}$' AND "partSize" BETWEEN 5242880 AND 67108864 AND "partCount" BETWEEN 1 AND 10000),
    CONSTRAINT s3_multipart_journal_evidence_chk CHECK (jsonb_typeof(evidence) = 'array' AND jsonb_array_length(evidence) <= 32 AND octet_length(evidence::text) <= 32768 AND jsonb_typeof(manifest) = 'array' AND jsonb_array_length(manifest) <= 10000 AND (response IS NULL OR jsonb_typeof(response) = 'object'))
  )`.execute(db);
  await sql`CREATE INDEX "s3_multipart_journal_intentId_provider_storeId_key_token_idx" ON s3_multipart_journal ("intentId", provider, "storeId", key, token)`.execute(db);
}

export async function down(db: Kysely<any>): Promise<void> {
  await sql`DROP TABLE s3_multipart_journal`.execute(db);
  await sql`ALTER TABLE object_upload_intent DROP CONSTRAINT "object_upload_intent_id_provider_storeId_key_token_uq"`.execute(db);
}
