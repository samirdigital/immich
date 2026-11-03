# Experimental managed upload HTTP wiring

This slice connects the existing authenticated `POST /assets` route to the bounded publication core described in `upload-publication-core.md`. It supersedes those foundation documents' statements that no HTTP wiring exists. It is disabled by default and is not deployment approval.

## Server-only configuration

`IMMICH_OBJECT_UPLOAD_ENABLED` defaults to false. Only literal `true` enables it; invalid values fail startup with a configuration error that does not include supplied values. Enabling requires the reviewed original-read registry (`IMMICH_OBJECT_READ_ENABLED=true` and its existing explicit endpoint/bucket/credentials configuration), plus pre-provisioned canonical, service-owned 0700 `IMMICH_OBJECT_SOURCE_ROOT` and `IMMICH_OBJECT_CATALOG_ROOT`. The roots must differ. No directory is silently created and no ambient credential chain is introduced. The reviewed read factory supplies the exact fixed SDK-backed publication binding. Construction/import does not contact the provider.

## Request path

The existing AuthGuard runs before both upload interceptors. Enabled uploads skip the legacy checksum-header shortcut, so actual multipart bytes, DTO validation, and server hashes participate in idempotency. `x-immich-upload-idempotency-key` is passed internally to the core, which validates its bounded printable-ASCII form and detects changed fingerprints.

Before receive writes bytes, the adapter checks current SQL owner and session/API-key/shared-link grants, including current shared-album owner membership. Authentication provenance contains only trusted IDs and applicable SQL-resolved shared-link type/album context; body fields never supply it. Multer receives originals and sidecars through OwnedSourceService. Only after complete DTO validation does the adapter seal both sources with the validated modification time, normalize all publication options, and call the unchanged core. Managed dispatch is outside the legacy upload catch. Created/replayed-created results use 201; checksum duplicates use 200. Local duplicate winners retain local authority.

Profile uploads always retain the legacy path, even when managed asset uploads are enabled. Disabled managed uploads preserve the existing local route and checksum shortcut.

## Cleanup and disconnects

A private symbol carries a server-minted object token through Multer's shallow copy. A WeakMap binds that token to the actual receive operation; request-body receipt/path/managed flags cannot manufacture proof. The public receipt-ID metadata is informational, not authority. Cleanup reads the current owner-scoped source row and compares receipt/name/root/owner/path metadata. Every proven managed source is retained, including mismatch, unavailable SQL, partial write, validation failure, held publication, and postcommit disconnect. No raw unlink, FileDelete job, or asset rollback is authorized for these files. The core owns receive/seal failure journaling. Disconnecting a response does not invent a core cancellation API or permit another remote attempt.

## Pending consumers and qualification

The core commits three pending-only effects with each created asset. This adapter deliberately does not emit AssetCreate or queue metadata/workflow jobs: durable consumer completion and stable workflow fanout identities are not implemented in this slice. Published original GET works through the existing original-reader mapping while effects remain pending.

`test/catalog/upload-publication-http.spec.ts` uses actual existing controllers, AuthGuard, upload interceptors, Zod validation, owned-source filesystem operations, private PostgreSQL, publication core, actual SDK loopback and independent GET verification. AuthService.authenticate is controlled, not full production authentication. GET owner-access is controlled; its original lookup/reader/SDK are real. Local/profile cases use controlled legacy persistence/profile dependencies. This is not full upstream schema, Redis, production authentication, cloud service, consumer workflow, tombstone, or deployment qualification.
