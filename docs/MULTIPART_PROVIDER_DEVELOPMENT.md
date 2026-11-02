# Sequential multipart provider: development contract

**NOT live qualified. Disabled by default.** The directly callable catalog now supplies a PostgreSQL journal adapter. This does not enable application uploads, declare cloud durability, or resume uploads. Provider-only synthetic callbacks are still not durable storage. See [the catalog operating contract](OBJECT_CATALOG_DEVELOPMENT.md).

## Opt-in API and bounds

`S3ObjectStoreRepository(client, { bucket, multipart? })` retains the existing file-only `putFile` API. Omitting `multipart` leaves the single-PUT path and its 5 GiB guard unchanged. Empty files and files strictly below the configured threshold use that same single-PUT path and need no multipart hooks.

The opt-in `multipart` object requires all of:

| Field              | Constraint                                                                                              |
| ------------------ | ------------------------------------------------------------------------------------------------------- |
| `threshold`        | Safe integer, at least `partSize`, at most 5 GiB; multipart is selected at equality.                    |
| `partSize`         | Safe integer, 5–64 MiB inclusive. **8 MiB is the unqualified candidate**, not a B2 compatibility claim. |
| `maxObjectSize`    | Safe integer, at least `threshold`, at most 5 TiB. Each write must also fit in at most 10,000 parts.    |
| `cleanupTimeoutMs` | Integer, 1–60,000 ms inclusive; bounds the separate cooperative AbortMultipartUpload request.           |

The effective object ceiling is `min(maxObjectSize, partSize * 10000)`. Invalid settings, object sizes and part plans fail before opening a file or creating an upload. Configuration, key, size and expected digest are snapshotted before awaits. There are no defaults for these opt-in limits; enabling the feature is an explicit caller decision.

Multipart writes additionally require `S3ObjectWriteOptions`:

- `multipartAttemptToken`: nonempty opaque string, identifying an already prepared, uniquely fenced attempt. Never reuse it for a retry.
- `multipartJournal(event): Promise<void>`: mandatory asynchronous persistence boundary. Missing hooks/token reject before open/Create. The provider cannot establish whether an implementation really commits durably; a no-op callback is suitable only for an explicitly synthetic test, never production.

No arbitrary caller `Readable`, file handle, stream factory, or whole-file buffer overload was added. The provider still opens an absolute owned staging path once, rejects final-component symlinks/nonregular/wrong-size files, and uses the same descriptor for all bytes. It does not delete the staging file.

The injected client must use `maxAttempts: 1`; multipart rejects a different resolved setting before opening the source. Custom SDK retry strategies/middleware and injected transports must honor single-attempt semantics, checksum compatibility and cooperative cancellation, including cleanup. The native Node SDK handler is exercised in loopback fixtures with `requestChecksumCalculation: 'WHEN_REQUIRED'` and `responseChecksumValidation: 'WHEN_REQUIRED'`. A transport or journal that never settles is outside the contract; the provider does not disguise it with a race that abandons live work.

## Journal protocol (not an acknowledgement protocol)

`S3MultipartJournalEvent` is a discriminated union. Every event carries the exact `bucket`, `key` and `attemptToken`. The catalog adapter binds the callback to the exact durable store identity (including endpoint/account configuration), staging record and attempt token, validates the event against that intent, fences state transitions, and resolves **only after the transaction is durable**. Bucket names alone are not globally unique store identities.

| Phase            | Meaning and required ordering                                                                                                                                                                                                                                                                                                                                                                               |
| ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `beforeCreate`   | Carries `size`, `expectedHash`, `partSize`, `partCount`. Persist a creating/sending intent before Create can have any remote effect. A crash or lost Create response can leave an upload whose ID is unknown.                                                                                                                                                                                               |
| `uploadCreated`  | Carries the exact returned `uploadId`. Persist it before any source reading or UploadPart request. Failure here stops parts and attempts bounded cleanup of that known ID.                                                                                                                                                                                                                                  |
| `beforeComplete` | Carries `uploadId` and the frozen, ordered `parts` array of exact `{ PartNumber, ETag }` entries. Full-source EOF size/hash validation has completed. Persist the completion intent/manifest before Complete dispatch. This checkpoint alone must remain conservatively ambiguous after a crash.                                                                                                            |
| `receivedResult` | Carries `uploadId`, `versionId?`, `etag?` from the actual Complete result, including missing fields. **Provider-response evidence only**, not an independently verified object or a catalog acknowledgement. The provider requires nonempty ETag and a nonempty, non-`null` VersionId before it can return success. This hook is attempted even if the caller has cancelled just as a response is received. |
| `abortResult`    | Carries known `uploadId` and `outcome: { ok: true }` or `{ ok: false, reason }`. Secondary cleanup evidence only; a successful abort is not proof that no remote version exists.                                                                                                                                                                                                                            |
| `failure`        | Carries the failed boundary `at`, optional known `uploadId`, `completeDispatched`, exact first `reason` (including falsey values), `secondary` cleanup/bookkeeping failures and optional `receivedResult` evidence. Do not turn uncertainty into an authoritative absent-object state.                                                                                                                      |

All forward-progress hooks are awaited. A hook rejection stops forward progress. The first failure identity is preserved even for `false`, `0`, `''`, `null` and `undefined`; cleanup errors do not replace it. `abortResult` bookkeeping failure is included in the subsequent failure event. Failure-event persistence is best effort once a primary failure already exists: if the journal is unavailable, no durable evidence is claimed, and the previously persisted intent must remain pinned for reconciliation. There is no in-memory journal fallback and no claim that a callback invocation implies a database acknowledgement.

The catalog adapter preserves prepared/sending/exact-ack invariants and requires independent, full, exact-version GET size/hash verification before cleanup. The exact accepted identity and acknowledgement remain atomic. The S3-specific journal retains creating, known UploadId, ordered completing manifest, exact received response, and bounded secondary observations. Lease expiry is never evidence that an old process stopped.

## Transfer and failure lifecycle

Parts are strictly sequential, numbered from one. The provider owns one part buffer at a time, plus the source/validator 64 KiB high-water-mark queues and bounded iterator read-ahead. There is one manifest with at most 10,000 entries; the SDK additionally serializes that bounded manifest. There is no file-sized allocation, repeated part concatenation, per-part path reopen, or concurrent part queue. SDK/socket overhead and buffers deliberately retained by a custom injected client/journal are outside this provider-owned memory bound. The final part may be short; an exact multiple produces no empty trailing part.

Every source byte traverses the existing full-size/hash validator through EOF before `beforeComplete`/Complete. A source/read/hash/size failure cannot dispatch Complete. A source error or caller cancellation aborts the operation controller, destroys owned source/validator streams, and cancels the running SDK request. SDK work and pipeline settlement are awaited; owned descriptor and listeners are closed on every exit. Native SDK HTTP-200 embedded Complete errors are deserialized as errors, not accepted as successful completion.

Before Complete dispatch, failure with a known UploadId attempts exactly one AbortMultipartUpload using a separate bounded cleanup controller, **not the already-aborted caller signal**. No ID means no guessed abort. After Complete has been dispatched, a transport failure, cancellation, malformed response or journal failure remains completion uncertainty/known-response evidence: the provider does not retry Complete, send an abort as an absence proof, or automatically delete any object version. Always retain staging and recoverable intent.

## Remaining gates

- Continue holding the staging-inode immutability assumption throughout provider settlement. `expectedFileIdentity` is a narrow readonly decimal-string bigint fingerprint; the helper snapshots it and compares device/inode/size/timestamps/mode/uid/gid/nlink on the same provider-owned descriptor before any remote effect. A regular, single-link, service-owned, non-writable inode is required. Root identity remains a service check. This is not protection against malicious same-UID writers or preexisting writable descriptors. No caller handles, callbacks or stream factories are accepted.
- Independently verify complete exact-version remote bytes; source hashing and opaque ETags are not remote integrity evidence.
- Independently review the integrated implementation before publishing.
- Qualify actual B2 multipart limits, VersionId behavior, checksums, cancellation/cleanup and ambiguous completion against a private disposable scope. No credentials were read and no cloud calls were made for this slice. Later authorized credential retrieval must keep secrets out of logs/repository. Never touch Ente objects or change Ente configuration. Prefer a separate bucket if permitted; otherwise obtain scope approval for a dedicated isolated run prefix with a strict allowlist, never a bucket-wide sweep. Do not assume an existing key's permissions.

## Offline verification

Run from `server` using the pinned tools without auto-installing anything:

```sh
MISE_AUTO_INSTALL=0 /home/samir-agent/.local/bin/mise exec -- pnpm test --run src/repositories/s3-multipart.repository.spec.ts src/repositories/s3-object-store.repository.spec.ts src/repositories/local-object-store.repository.spec.ts
MISE_AUTO_INSTALL=0 /home/samir-agent/.local/bin/mise exec -- pnpm test --run
MISE_AUTO_INSTALL=0 /home/samir-agent/.local/bin/mise exec -- pnpm check
MISE_AUTO_INSTALL=0 /home/samir-agent/.local/bin/mise exec -- pnpm lint
MISE_AUTO_INSTALL=0 /home/samir-agent/.local/bin/mise exec -- pnpm format
MISE_AUTO_INSTALL=0 /home/samir-agent/.local/bin/mise exec -- pnpm build
```
