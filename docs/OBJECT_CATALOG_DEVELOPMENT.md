# Durable staging and object catalog

Development-only, directly callable subsystem. It is not connected to application uploads, BaseService, routes, queues, StorageService deletion, or real media. Existing file-only PUT/GET/delete behavior remains; narrow optional sealed-identity and typed S3 multipart capabilities have been added. No live B2 qualification is implied.

## Calling contract

Construct `ObjectCatalogRepository(kysely)` and `ObjectCatalogService(repository, privateRoot)` explicitly. The database must already have the catalog and multipart journal migrations and durable PostgreSQL WAL/commit settings (`fsync=on`, `synchronous_commit=on`); disabling database durability defeats the creating-before-copy guarantee. The service deliberately has no application DI registration.

The staging root must be a **pre-provisioned, durable, canonical absolute directory**, owned by the service account with mode 0700, dedicated exclusively to catalog staging. It must be separate from Immich temporary files, uploads, provider object roots, and caller-managed sources. Do not point an existing temp/upload directory at this API. The caller provisions and durably creates the root and its parents; this subsystem does not create or age-sweep them. The root's ancestor namespace must remain trusted. This is a Linux/POSIX filesystem implementation, not a Windows or network-filesystem guarantee.

A `CatalogStoreBinding` supplies `provider: 'local' | 's3'`, a non-secret immutable `storeId`, and the corresponding ObjectStore instance. Assign store IDs from trusted configuration to a specific physical store (for S3: endpoint/account/bucket; for local: the fixed private provider namespace). Never reuse an ID for another store or mislabel a binding. ObjectStore does not expose a physical-store fingerprint, so the catalog cannot authenticate this configuration itself. Persisted provider/store identity is checked before resuming or verifying. Keys contain stage UUID plus unique attempt UUID, not caller paths or reusable asset IDs.

Typical sequence:

```ts
const catalog = new ObjectCatalogRepository(db);
const staging = new ObjectCatalogService(catalog, dedicatedPrivateRoot);
const stage = await staging.stage(syntheticCallerPath);
const version = await staging.upload(stage.id, binding);
const verified = await staging.verify(stage.id, version.id, binding);
if (verified) {
  await catalog.releaseStage(stage.id);
  await staging.cleanup(stage.id);
}
```

Processing callers must `pin(stageId)` before accessing owned bytes and retain the returned UUID until all use has stopped, then `unpin(stageId, token)`. Pins are durable and have no automatic expiration. A crashed process requires operator review before releasing its pin. A lease timeout does **not** prove a worker stopped reading. `releaseStage` permanently closes the stage to new upload preparation; it does not waive verification or unresolved-attempt/pin checks. Verification remains allowed after release, while the stage is ready.

## Persisted state and invariants

- `staged_object`: `creating -> ready -> deleting -> deleted`; uncertainty transitions to terminal `quarantined`. No adoption or automatic unquarantine exists. UUID filename, canonical root, lossless decimal device/inode/size/mode/uid/gid/nlink/mtimeNs/ctimeNs and root device/inode, SHA256, explicit release, durable processing pins, and diagnostic evidence are retained. PostgreSQL bigint sizes remain strings; the provider's safe-integer size limit is enforced by CHECK.
- `object_upload_intent`: `prepared -> sending -> acknowledged`, or `sending -> ambiguous`. Each record has immutable provider/store/key and creation time. Sending claims have a UUID fencing token and lease deadline computed from **database time**. No attempt is overwritten by a new attempt or automatically replayed after sending.
- `s3_multipart_journal`: one S3-specific journal per exact intent/provider/store/key/claim token, with a composite RESTRICT foreign key. The bucket is snapshotted from the provider's bound typed `s3Multipart` capability, not a caller type assertion or bucket guessed from `storeId`. `creating` commits before Create, `uploading` stores the returned UploadId before parts, `completing` commits the ordered opaque-ETag manifest before Complete, and `received` records the actual response before acknowledgement. Missing response fields remain explicit evidence of an invalid response, not success. No journal means no multipart fallback.
- `object_version`: one exact acknowledged identity per intent, opaque ETag, independent verification state `pending | verified | failed`, and append-only observations stamped with database time. Acknowledgement and insertion of the complete returned identity occur in one SQL transaction. An invalid/missing S3 version or SQL insertion failure rolls back acknowledgement. S3 needs nonempty, non-`null` version ID; local identities are intentionally unversioned. Composite RESTRICT foreign keys prevent mismatched intent/store/key identities. There is no asset FK, evidence cascade, generic state-update API, or catalog evidence-deletion API.

Stage preparation and cleanup/pin claims serialize on the stage row; sending is an explicit compare-and-set. A completion requires the original token, sending state, and an unexpired DB-time lease. Expired workers cannot acknowledge. Lease duration defaults to one minute and is configurable up to one hour. A serialized heartbeat runs every min(10 seconds, one third of the lease), using fenced DB-time renewal that cannot revive expired/stale tokens. Renewal locks the row before evaluating current DB time, avoiding a PostgreSQL lock-wait stale-predicate race. Forward journal transactions recheck DB time after their writes and roll back if expired. Renewal statements have a bounded database statement timeout; a separate conservative renewal deadline cooperatively aborts provider work if renewal stalls. The service clears timers and awaits outstanding renewal work on every exit. A hung database connection/driver may still delay settlement; no work is abandoned by a Promise race. Event-loop suspension is not worker termination. There is no takeover or retry worker, and every uncertain outcome stays held.

SQL transactions contain only database operations, never provider calls, hashing, copying, or filesystem operations.

## Owned-copy durability

`stage(source)` persists creating **before** opening/copying any file. It rejects symlink components, nonregular files, hardlinked sources, and sources inside the owned staging root. It opens the source with NOFOLLOW/NONBLOCK, creates a different private inode with exclusive creation and mode 0600, copies with bounded streaming buffers and SHA256, checks source identity/stat stability, seals the owned inode mode 0400, fsyncs the inode, and fsyncs the parent directory. Only then does it record identity/hash/size and CAS creating to ready. It never renames, adopts, hardlinks, deletes, or changes permissions on caller sources.

Upload and cleanup recheck canonical private root and stored root identity, exact inode metadata, regular/non-hardlinked status, and streamed stage hash before use. Unexpected replacements, links, missing bytes, or corruption quarantine the row and retain files. Quarantine is a **logical hold**, not a move or deletion that could destroy evidence. Unknown files are reported, never swept based on age.

After service validation, upload passes an immutable expected bigint fingerprint to `openObjectFile`. The helper compares all nine lossless inode/stat fields plus regular/single-link/owner/non-writable checks on the same descriptor that it streams, before any remote effect. Identical bytes/hash on a replacement inode are rejected. Invalid fingerprints and mismatches close the acquired descriptor. The root is still validated by the service; the provider does not reopen the source path.

This controlled-root immutability assumption prevents cooperative workers from modifying owned bytes; mode 0400 is not a defense against a malicious process with the same Unix UID, root privileges, a preexisting writable descriptor, or a filesystem that lies about fsync. This subsystem does not claim that descriptor/path checks eliminate every hostile same-user TOCTOU race.

## Verification and cleanup

A PUT source hash is **not remote proof**. Acknowledgement alone cannot release local bytes. `verify` pins the stage, issues an exact-identity **full GET** without ranges, checks the returned identity and declared size, counts and SHA256-hashes the actual stream without buffering the object, and records success/failure evidence. Missing versions, wrong identity, wrong declared/actual size, wrong hash, GET exceptions, and stream errors all hold the stage. Observations remain after rechecks; latest failure blocks cleanup even after earlier success. A SQL failure persisting an observation leaves its durable verification pin in place for review.

Default cleanup requires explicit release, zero processing/verification pins, at least one attempt, **all** attempts acknowledged and represented by versions, and **all** versions independently verified. This is deliberately conservative for stages with multiple destinations. Claiming deletion changes ready to deleting transactionally **before unlink** and prevents new use. Cleanup validates root/inode/hash, unlinks only that owned name, fsyncs its directory, then marks deleted. SQL/fsync failures after unlink leave deleting recoverable. Repeated cleanup claims do not delete again. No remote deletion is performed anywhere in this subsystem.

Cleanup and deletion recovery take the same per-stage PostgreSQL **session advisory lock** before reading/claiming or re-reading deletion state, and retain it through validation, unlink, directory fsync and deleted acknowledgement. Kysely reserves one connection for the operation; the callback's catalog queries and short claim transaction use that same connection, including with a one-connection pool. No SQL transaction spans filesystem work. The lock is explicitly released in `finally`, and PostgreSQL releases it when a crashed process's session disconnects; there is no expiry/takeover timer. Recovery re-reads the stage under the lock, so a waiter never validates a stale deleting snapshot after its peer finished or quarantined it. All cooperating cleanup/recovery workers must use this API against the same catalog database with session-affine connections (not transaction-mode pooling). Do not forcibly terminate a live worker's database session independently of the worker while it is doing filesystem work; advisory coordination depends on that session remaining owned until the operation stops. Unexpected connection loss or a database restart can also release the lock while a worker continues filesystem work; this implementation does not fence that surviving worker. Stop affected cleanup/recovery workers and confirm they have stopped before resuming recovery after session loss.

A successful full GET is an observation of a specific version at that time, not a promise of indefinite provider durability, retention, replication, or backup sufficiency. Callers still need backup/restore policy.

## Recovery

Run `recover()` after stopping old stage-creation workers; creation has no takeover lease. Recovery conservatively quarantines creating rows, even if the copied file looks complete. It never guesses that fsync/DB commit occurred. It resumes only **already claimed** deleting rows, validating identity when present, accepting an absent file at that boundary, fsyncing the directory and recording deleted. Repeated/concurrent deletion recovery is idempotent. A reappeared deleted filename is reported as unknown, not deleted again.

Sending rows with expired DB-time leases become ambiguous, and remain pinned permanently. Nonexpired sending is also held. Prepared intents remain pinned and may be explicitly resumed with `sendPrepared(intentId, matchingBinding)`; sending/ambiguous/acknowledged intents cannot use that path. `recover()` returns unknown names plus held rows, processing tokens, unresolved attempts, multipart journals (including unknown-Create-ID, completing, received-response and known UploadId evidence), and unverified versions. Query methods retain all attempts/versions and verification observations.

Unknown Create IDs and unknown Complete outcomes are **hold/report only**. Recovery never lists/discovers, retries Create/parts/Complete, aborts a multipart upload, or releases pins merely because a lease expired. Only the provider may attempt bounded pre-Complete cleanup of an ID it actually received. Successful/failed abort observations assert neither remote absence nor safe release. Secondary reasons are classified without raw error messages/headers/stacks or credentials; at most eight secondary classifications and 32 observations within a 30 KB JSON budget are retained (database cap 32 KiB). Initial history and latest observation are retained when bounded truncation is needed. Exact bounded response IDs/ETags are retained separately when available; database failure can prevent extra observations but cannot remove the previous durable pin.

Remote-success-before-ack is fundamentally ambiguous if the exact version was never committed. **The existing provider contract cannot discover an unknown version** (no list-versions/lookup-by-attempt capability). Do not GET an unversioned S3 key, blindly retry, release the stage, or issue compensating deletes. This milestone provides durable hold/report only, not an invented reconciliation operation. A late received identity is retained as attempt evidence when possible but cannot bypass fencing or become acknowledged automatically. Database unavailability can prevent writing that extra evidence; the previously durable sending pin still protects the stage.

Quarantined rows and abandoned processing pins require manual investigation; there is intentionally no force-delete, cancellation, ambiguity-resolution, automatic retry, or unquarantine shortcut. Recovery stops and surfaces an error on an invalid deleting inode or persistence failure rather than sweeping past uncertainty.

## Standalone real PostgreSQL tests

Existing upstream Docker medium configuration is unchanged. This suite uses installed `postgres`/Kysely dependencies, no new packages. It requires explicit permission and an absolute private Unix socket directory:

```sh
CATALOG_TEST_DISPOSABLE=YES \
CATALOG_TEST_SOCKET=/absolute/private/postgres/socket \
CATALOG_TEST_PORT=55441 \
MISE_AUTO_INSTALL=0 mise run //server:test-catalog --run
```

Equivalent from server: `mise exec -- pnpm run test:catalog --run`. The task builds first because crash subprocesses exercise compiled code. The fixture creates a random `immich_catalog_test_<uuid>` database, applies **only** `1793500000000-AddObjectCatalog` and `1793500001000-AddS3MultipartJournal`, and drops that exact database. The role must be allowed to create disposable databases. No supplied application DB URL is used. The subprocess validates the disposable name and permission guard again. A force-killed test runner may leave its random database for manual removal; do not broadly drop matching databases while another suite is running.

Tests exercise real PostgreSQL concurrency and transactional rollback, owned synthetic files and local-provider roundtrips, full GET failure modes, stale fencing, DB failures, and subprocess exits after creating, file fsync, durable copy before ready, sending before PUT, remote success before ack, deleting before unlink, and unlink before deleted acknowledgement. Process exits are not emulated kernel/power-loss tests. The verified local runtime is PostgreSQL 18.6 with fsync on; SQL deliberately uses PG14-compatible features, but this is not a PG14 execution claim or a full upstream migration-chain/medium-suite claim.

The added SDK loopback tests stream synthetic owned files through actual sequential multipart HTTP plus real catalog SQL, then independently GET the exact completed bytes before cleanup. Controlled compiled subprocess exits cover before Create, Create response before UploadId persistence (irretrievably unknown ID), persisted UploadId before parts, completing before Complete, and remote completion with received response before acknowledgement. These are local protocol fixtures, not cloud qualification.

Remaining scope: application integration, retention configuration enforcement, authenticated physical-store registration, multipart resume, remote discovery/reconciliation, cancelled-intent resolution, operational repair tools, backup/restore qualification, and live B2 validation.
