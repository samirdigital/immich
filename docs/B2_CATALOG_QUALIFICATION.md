# Actual catalog/provider B2 qualification

This is a **one-shot, one-object synthetic probe**, not a migration command or recovery CLI. It exercises the real `ObjectCatalogService`, `ObjectCatalogRepository`, `S3ObjectStoreRepository`, AWS SDK, and the existing PostgreSQL multipart journal. Offline tests use a loopback SDK endpoint and a private disposable PostgreSQL server; they do not qualify B2 itself. Independent review and separate live authorization are required before live use.

## Pure dry plan (no credentials needed)

From `server/`, after `MISE_AUTO_INSTALL=0 mise exec -- pnpm build`:

```sh
MISE_AUTO_INSTALL=0 mise exec -- node dist/bin/b2-catalog-qualify.js \
  --bucket synthetic-bucket \
  --endpoint https://s3.us-west-004.backblazeb2.com \
  --region us-west-004 \
  --prefix immich-b2-qualification/77c6420b-96d8-46bb-9d76-c0ac51546298/ \
  --workspace /nonexistent/catalog-probe \
  --pg-socket /private/postgres \
  --pg-port 55441 \
  --disposable-local-pg-opt-in
```

That UUID is a **dry-plan example only**. Live operation requires a freshly generated UUID and approved bucket, region, prefix, and workspace. Without `--synthetic-test-opt-in`, the command only validates syntax and prints the plan. It does not stat paths, open stdin, create files, connect to PostgreSQL, instantiate an SDK client, or contact the cloud. Unknown/duplicate flags and all credential flags are rejected. Endpoint and region validation reuse the standalone qualification exports: only the exact HTTPS B2 regional endpoint is accepted.

## Live preconditions (not executed during implementation)

- Explicit `--synthetic-test-opt-in` **and** `--disposable-local-pg-opt-in`.
- Linux, a private owned canonical Unix-socket directory (0700), owned PostgreSQL socket, explicit unprivileged port, and local OS-user PostgreSQL role with disposable-database creation rights. No connection URL, TCP hostname, production DB name, or environment DB settings are accepted.
- A nonexistent workspace under an owned canonical parent not writable by group/others. The runner exclusively creates a 0700 workspace, 0600 append-only/fsynced `manifest.jsonl`, 0700 staging root, and 0400 source/staged files. Directory entries are fsynced.
- Credentials delivered directly by a separately reviewed secret bridge to **private anonymous stdin pipe** (owned FIFO, mode 0600, `/proc/self/fd/0` is `pipe:[…]`). TTYs, regular files, sockets and named FIFO paths are rejected before credential parsing. The bounded existing JSON credential parser is reused. No credentials in argv, env, temporary files, logs, debug output, or this document. Do not use shell tracing, `tee`, or copy/paste credentials into commands.
- The bridge must provide exactly one JSON credential object with `accessKeyId` and `secretAccessKey`. Use an already approved scoped key, never broad production credentials. The CLI does not fetch credentials or create buckets/keys.

Only the explicit CLI path constructs the hardened SDK client through `createQualificationClient`: single attempt, no endpoint/region redirects, HTTPS verification, one socket, bounded transport timeouts, checksums `WHEN_REQUIRED`. The injectable client in the exported runner is for offline actual-SDK testing; it is **not** a user-selectable CLI endpoint bypass.

## Concrete sequence and ownership

1. Create and retain random `immich_catalog_live_test_<UUID-without-hyphens>` DB, after fsyncing its name to the manifest. Apply **only** `AddObjectCatalog` and `AddS3MultipartJournal`. No upstream application schema or production DB is used.
2. Generate exactly 17 MiB in seventeen 1 MiB chunks; fsync, seal source readonly, stage with the actual service, and compare the source SHA-256 and byte count to the actual staged hash.
3. Call actual repository `prepare` first. Assert the persisted generated key equals `<approved-prefix>/<stage UUID>/<intent UUID>`. Fsync exact-key enrollment **before** invoking `sendPrepared`. No key rewriting, provider adapters or alternate durable journal.
4. Actual provider uploads 8/8/1 MiB via real SDK multipart commands. The narrow SDK composition permits only the enrolled bucket/key, acknowledged upload ID, three bounded sequential parts, and exact acknowledged part manifest. All list/head/abort/PUT commands, bare-key deletes, and foreign handles are denied before transport. The existing provider's error-path abort request is also denied: this probe deliberately keeps even a known unfinished upload for manual review rather than introducing cleanup ambiguity.
5. Fsync an intent before every remote send; fsync only selected ACK fields before adopting returned upload/version IDs or passing results to the provider/catalog. Any append/hook/fsync failure freezes **all** later remote sends, including cleanup. GET body errors and cancellation are owned during the ACK barrier, and all response streams are disposed/settled on failure. No raw SDK error is logged.
6. Assert actual intent is acknowledged, journal is received with three parts and the same key/version, and version is unverified. Release the stage reference and prove actual stage cleanup is blocked. Run **actual `service.verify` full GET**; it checks exact version, length and SHA-256 and records DB verification. Require verified DB state before actual stage cleanup can succeed. Assert stage is deleted.
7. Authorize deletion only along this verified sequence. Call actual provider `delete` with exactly the returned version, fsync its successful ACK receipt, then call actual provider `get` with that version and range `0-0`. Only a precise missing-key/version 404 proves absence. No bare-key delete, listing, guessed version, re-upload, or retry.

Budgets are fixed at 24 SDK sends, 64 MiB of object payload, and a 180-second cooperative deadline. Two sends and 1 MiB are reserved from normal work for exact cleanup/absence. The happy path uses eight sends and 34 MiB object payload. Source buffers are 1 MiB, provider upload buffers at most 8 MiB, GET chunks at most 1 MiB and full GET payload at most 17 MiB. The SDK abort signal combines runtime/user cancellation with provider cancellation; the runner awaits in-flight provider/service settlement, closes GET streams, and closes DB connections. This is cooperative cancellation, not a hard process kill; an in-flight bounded local SQL statement may take its timeout to settle.

## Evidence and uncertainty

Both DB and workspace are **always retained**; the runner never drops DBs or removes the evidence workspace. Only actual successful service cleanup removes the owned staged copy. The readonly source is retained even after success. IDs live in the protected manifest, DB, and independent `received-observations.jsonl`, never stdout. Stdout contains only status, phase, budgets, and manifest path. Exit zero means the complete synthetic sequence qualified; `held` or `refused` exits nonzero.

Before any remote request, the runner exclusively prepares `received-observations.jsonl` (0600) inside the owned 0700 workspace, fsyncs the file and directory entries. On Create/Complete receipt it validates and captures the returned handle **before** the main ACK persistence hook/append/fsync. It independently appends and fsyncs a bounded observation snapshot: only the exact enrolled key, `receivedCreateUploadId`, and, when received, `receivedCompleteVersionId`. There are at most two records; each ID is at most 1024 printable ASCII characters. No SDK response, headers, secrets, or raw errors enter this file. An operator should privately inspect its last complete JSON line alongside `manifest.jsonl` and the retained DB named in the manifest; do not copy IDs to stdout/logs. This file is observation evidence, **not canonical ACK, catalog version, verification, or cleanup authorization**. Only a successful main ACK barrier permits the provider/catalog to progress. A failed Complete ACK still leaves the intent ambiguous and creates no `object_version`, even if its received version ID is known.

On any failed verification or uncertain upload result, the stage is held and no remote cleanup is attempted. A failed Complete can mean an unknown remote version: do not infer it from a key or list results. If a failure occurs after legitimate verified stage cleanup, the retained source/DB/manifest remain evidence; the runner does not claim the stage still exists. Any main-journal or observation-write failure freezes all subsequent remote commands, including cleanup; even the final held marker may be absent. The exported runner's structured held result retains a private `receivedObservation` memory snapshot, with `persisted: true` only after that complete snapshot's independent fsync succeeds. If secondary storage also fails, `persisted: false` makes no durability claim: a previous record or partial write is not the newest durable snapshot, and process loss can lose the memory-only handle. A private in-process caller/bridge must preserve that result through its own protected channel; the CLI deliberately does not serialize IDs and cannot guarantee their recovery when all storage fails. The last intent and DB journal are evidence, **not proof of remote absence**. Observed handles never clear an ambiguous hold or authorize another call. No automatic replay, force cleanup, or recovery exists; fresh independent review and separate authorization remain required before live use.

## Offline verification

```sh
CATALOG_TEST_DISPOSABLE=YES \
CATALOG_TEST_SOCKET=/home/samir-agent/.cache/immich-b2-pg \
CATALOG_TEST_PORT=55441 \
MISE_AUTO_INSTALL=0 mise exec -- pnpm test:catalog --run
MISE_AUTO_INSTALL=0 mise exec -- pnpm test --run
MISE_AUTO_INSTALL=0 mise exec -- pnpm check
MISE_AUTO_INSTALL=0 mise exec -- pnpm lint
MISE_AUTO_INSTALL=0 mise exec -- pnpm format
MISE_AUTO_INSTALL=0 mise exec -- pnpm build
```

The new catalog spec automatically matches the existing catalog test configuration. It runs the exported production runner through actual SDK loopback transport and real PG, including Create/Complete/GET ACK-persistence failure, GET disposal, corrupt full GET, and unknown Complete. Unit tests cover dry planning and no secret reads, input guards, exact-key fencing, call freeze, and GET ACK-barrier ownership. Offline probe databases/workspaces are retained intentionally, just like live evidence; remove them only after separately reviewing test evidence and ownership.
