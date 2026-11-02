# Native object-storage foundation

Status: development-only groundwork for Backblaze B2 via its S3-compatible API. This branch does **not** yet store Immich uploads in B2 and is not ready for a personal-media import.

The isolated [durable staging and object catalog](OBJECT_CATALOG_DEVELOPMENT.md) now adds owned staging, durable intents, exact-version verification and conservative cleanup/recovery. It is not wired into the application; the provider contract below remains unchanged.

## What exists

- `server/src/repositories/object-store.repository.ts`: explicit object identities, readable GETs, file-only PUTs and deletes, with strict inclusive byte ranges. Shared file-opening and byte-validation helpers live alongside this unwired contract rather than in a new dependency layer.
- `server/src/repositories/local-object-store.repository.ts`: private local object namespace, exclusive same-directory temporary writes, flush before close, complete size/hash validation before atomic publication. Existing objects survive failed replacement writes; colliding temporary files that the provider did not create are not removed. Keys map to flat filenames; they are not arbitrary filesystem paths.
- `server/src/repositories/s3-object-store.repository.ts`: official AWS SDK transport with an injected client and bucket. Successful writes require a nonempty, non-`null` version ID. Reads/deletes require an exact version, avoiding an unversioned delete request that can leave older B2 versions behind.
- Colocated tests use real temporary staging files, explicit synthetic SDK HTTP handlers, and the SDK's default Node HTTP handler against real loopback servers. They do not contact a real bucket.

ETags are opaque provider data, not declared content checksums. A returned version ID or HEAD metadata is not independent verification of an original's content.

## File-backed write contract

`putFile(object, stagingPath, { size, expectedHash: { algorithm, hex }, signal? })` replaces the arbitrary-readable upload API; no stream overload remains. `stagingPath` must be an absolute string, not a URL, caller `FileHandle`, stream, or stream factory. `size` must be a nonnegative safe integer. The expected hash is required: SHA-1 uses exactly 40 hexadecimal characters, SHA-256 exactly 64, with either hex case accepted. These boundary checks occur before acquiring source descriptors or sending HTTP requests; pre-aborted writes also acquire no descriptor.

The provider opens the final staging component with `O_RDONLY | O_NOFOLLOW | O_NONBLOCK`, verifies a regular file and its declared size using that descriptor, and constructs its own `autoClose: true`, `emitClose: true` file stream from the same handle. It never reopens the path for transfer. Directories, final-component symlinks and nonregular files are not upload sources. Source descriptors are closed on validation/read/transport/cancellation failures. The persistent staging file is never deleted by either provider. `O_NOFOLLOW` does not secure parent directories: staging parents and the local destination root must be trusted.

**Future orchestrator precondition:** hold an immutability lease on the staging inode from expected-hash calculation through provider settlement. Do not overwrite, truncate, append to, or reuse that inode during a write. A pinned descriptor prevents path-replacement races; it does not freeze bytes against concurrent writers. Runtime size/hash validation detects many changes but does not replace a lease, durable intent, or independent integrity evidence. The providers do not implement this lease or the orchestrator.

A bounded transform counts and hashes every source byte, checks size while reading and at EOF, and checks the expected digest before its readable end. Source reads and transform queues use 64 KiB high-water marks and respect backpressure; the complete file is not buffered in memory. Local writes finish this validation and flush the exclusive temporary output before renaming. S3 writes require both complete validated body consumption and successful SDK transfer before returning a required version identity. An early response with an unread body is rejected, including an early HTTP success: it is not a validated upload success.

Source/transform errors abort a provider-owned controller, combined with the caller signal without aborting the caller's controller. This explicitly cancels the SDK's native Node HTTP request, which uses piping rather than automatically propagating source errors. Transport failures and cancellation tear down the owned reader and validator too. Both the SDK promise and the source pipeline settle before return/rejection; the first failure object is retained and owned stream/signal listeners are cleaned up. No arbitrary-stream destroy callback, polling loop, or cleanup timeout is involved. Test deadlines are only failure guards.

**A matching source hash is not remote-byte verification.** A response, version ID, or opaque ETag cannot establish that durable remote bytes match the original. An interrupted, hash-rejected, early-response, or otherwise ambiguous PUT can still have created a remote version; there is no automatic unpinned compensating delete. Retain staging data and recoverable operation intent until a future orchestrator reconciles the exact provider outcome and independently verifies content.

## Not implemented

- Application upload/download integration, asset-to-object mappings and application dependency injection.
- Application-facing integrity/recovery jobs; the isolated catalog now implements verification and durable state.
- Multipart uploads, resumable uploads and retry-safe replay of consumed streams.
- Materialization leases/cache policy for native media tools and generated-media upload.
- A complete backup/restore/export path or verified B2 endpoint configuration.

The S3 writer currently accepts known-length single PUT only, with a 5 GiB guard. Large-video support requires a separate multipart implementation before application integration. A failed or ambiguous remote PUT is not a claim that no object exists: the caller must retain recoverable intent and reconcile provider outcomes.

The injected S3 client owns endpoint, region, scoped credentials, request/response checksum compatibility, transport deadlines and connection pooling. Configure `maxAttempts: 1` for this non-replayable single-use body; retry-safe reconstruction is not implemented. SDK failures must settle normally on abort, as the default Node HTTP handler does; an injected handler that ignores its abort signal is outside this contract. B2 compatibility must be qualified against a disposable real B2 bucket; synthetic SDK transport tests do not establish it. Never enable public bucket access as a shortcut.

The local root must exist and be private/trusted. Atomic rename gives atomic visibility, not a guarantee of directory-journal crash durability. A processing cache is not a backup.

## Verification

Use the checkout's pinned tools and workspace packages:

```sh
mise run //server:install
mise run //:plugins
mise run //server:test --run src/repositories/local-object-store.repository.spec.ts src/repositories/s3-object-store.repository.spec.ts
mise run //server:test --run
mise run //server:check
mise run //server:lint
mise run //server:build
```

The plugin build needs the pinned Extism JavaScript tool and Binaryen. Container/database integration tests require additional infrastructure and are not covered by the unit-suite result.

## Next integration gate

Before acknowledging an upload as cloud-durable, implement provider-aware object metadata, recoverable operation receipts, end-to-end integrity evidence and a staging/materialization lease. Permanent deletion needs durable version-specific intent coupled to asset removal. Preserve existing local storage and client authorization throughout. Do not move production originals simply because these provider tests pass.
