# Guarded B2 synthetic qualification

**Status: implemented and tested offline only. No live B2 qualification has been performed.**

This is a standalone, one-shot S3-protocol qualification probe for an explicitly authorized **existing** Backblaze B2 bucket. It does not create buckets, alter bucket settings, inspect account-wide state, enumerate bucket contents, or read existing Ente objects. The two generated keys must first return exact-key HEAD absence. Access denial is not absence.

## Approval and scope before execution

Obtain approval for the exact bucket, endpoint, region, fresh run UUID prefix, two synthetic keys, and bounded billable traffic. Existing credentials must already be restricted appropriately. This tool cannot establish credential scope or bucket privacy. `HeadBucket` checks authorized metadata access only; AES256 requests and GET response evidence check SSE for the synthetic object, not bucket ACLs or public accessibility. Do not relax permissions to make a probe pass.

The only accepted endpoint spelling is `https://s3.<region>.backblazeb2.com`, with a matching B2-style region. Userinfo, ports, paths, query strings, fragments, HTTP, arbitrary hosts, region redirects, and alternate endpoint configuration are not accepted. The SDK is constructed with `maxAttempts: 1`, explicit credentials, path-style addressing, redirects disabled, and an HTTPS agent requiring certificate verification. No SDK logger is configured. Never enable debug logging, tracing of stdin/headers, shell tracing, or recording of a secret-entry terminal.

The only keys are:

- `immich-b2-qualification/<fresh-v4-UUID>/sample`
- `immich-b2-qualification/<fresh-v4-UUID>/incomplete`

No caller-provided object keys, upload IDs, versions, manifests, cleanup/replay commands, or arbitrary SDK options are accepted by the CLI. Never reuse a prefix, even with a new workspace. HEAD absence is not an atomic create lock: use a fresh cryptographically generated UUID and ensure no other writer shares the prefix.

## Build and dry plan

From `server/`, use the repository-pinned tools; no installation or package changes are necessary:

```sh
MISE_AUTO_INSTALL=0 mise exec -- pnpm build
RUN_UUID=$(MISE_AUTO_INSTALL=0 mise exec -- node -p 'crypto.randomUUID()')
MISE_AUTO_INSTALL=0 mise exec -- node dist/bin/b2-qualify.js \
  --bucket EXISTING_APPROVED_BUCKET \
  --endpoint https://s3.us-west-004.backblazeb2.com \
  --region us-west-004 \
  --prefix "immich-b2-qualification/$RUN_UUID/" \
  --workspace "$HOME/b2-qualification-$RUN_UUID"
```

The UUID and workspace are nonsecret. The workspace must **not exist**; its parent must exist, resolve without symlinks, and be trusted against hostile directory replacement. The live runner creates a new owner-only `0700` directory and exclusive `0600` `manifest.jsonl`. It fsyncs the manifest before every remote request, then fsyncs the directory and parent before the first remote request. Each result is recorded and fsynced before progressing. Existing workspaces and symlink workspace targets are refused, not resumed. A dry plan neither opens stdin credentials nor creates a workspace, client, or remote request.

## Future live invocation: secure stdin bridge only

**Do not run this until scope approval and a reviewed secret bridge are in place.** Add the explicit write authorization flag to the same compiled command:

```sh
MISE_AUTO_INSTALL=0 mise exec -- node dist/bin/b2-qualify.js \
  --bucket EXISTING_APPROVED_BUCKET \
  --endpoint https://s3.us-west-004.backblazeb2.com \
  --region us-west-004 \
  --prefix "immich-b2-qualification/$RUN_UUID/" \
  --workspace "$HOME/b2-qualification-$RUN_UUID" \
  --synthetic-test-opt-in
```

This command must be launched by a reviewed credential bridge with a **non-TTY stdin pipe**. The bridge retrieves the authorized credential in memory and writes one UTF-8 JSON object containing exactly the string fields `accessKeyId` and `secretAccessKey`, followed by a newline, then closes the pipe. There is deliberately no example containing credential values. Never use shell variables, environment variables, argv, heredocs, temporary files, or inline shell commands for the credential payload. Do not paste it into a normal terminal. A PTY with disabled echo is not accepted directly by this CLI: use a bridge that supplies a private pipe to the child.

Input is limited to 8192 bytes and ten seconds. The first structured JSON line (or EOF payload) is consumed and stdin closed immediately. Extra fields and malformed/multiple payloads are refused. Credentials are supplied directly to the SDK; no default credential provider chain is used. JavaScript cannot guarantee zeroization of immutable strings or SDK credential copies; protect the process, disable core dumps and instrumentation, and do not persist its memory.

## Probe and budgets

The unqualified candidate multipart part size is 8 MiB, a conservative baseline pending live B2 qualification, not a verified B2 minimum. A deterministic 17 MiB buffer produces three sequential parts of 8, 8, and 1 MiB. The probe:

1. Checks authorized bucket metadata and exact HEAD absence of both generated keys.
2. Creates an AES256 multipart upload; records its returned exact upload ID.
3. Uploads three real parts and completes using their returned ETags.
4. Records the exact non-null version ID, then verifies a bounded full GET SHA-256 and two-byte ranges straddling both part boundaries. Version, length, range status/header, and SSE evidence must agree.
5. Deletes **only that recorded version**, then requires an exact-version one-byte-range GET to return `NoSuchKey` or `NoSuchVersion` with HTTP 404. Authorization failures, timeouts, or generic 404s are not proof.
6. Creates a separate incomplete upload, sends one 8 MiB part, aborts the exact upload ID, and requires `ListParts` with that exact key/ID and `MaxParts: 1` to return HTTP 404 `NoSuchUpload`. An Abort ACK alone, empty successful list, or access denial never qualifies.

Defaults are 24 requests, 64 MiB charged object payload, and 180 seconds. Four requests, 1 MiB, and the final five seconds are reserved from forward progress for bounded cleanup. SDK operations are deadline-aborted after at most 30 seconds, also bounded by remaining run time. Read bodies have independent bounded-length/hash validation and deadline cancellation. These are logical object-payload/request budgets, **not** a byte-accurate cap on TLS, HTTP headers, XML errors, or kernel receive buffers. There is no pagination or object listing; `ListParts` is a single capped request. In-process tests can tighten budgets within validated bounds; the CLI has no budget override.

GET body ownership begins as soon as the SDK send resolves, before the ACK barrier. Errors are latched until the bounded reader takes over, and the error observer remains until asynchronous destruction and close settle. This harness accepts the owned Node SDK response stream lifecycle (including a `close` event after destruction); arbitrary injected streams configured with `emitClose: false` or a destruction callback that never settles are outside its contract. Waiting for actual close, rather than a cleanup timeout, ensures no owned listeners are removed while destruction can still emit an error.

SIGINT/SIGTERM stop forward progress. Known owned versions are eligible for one exact-version delete plus absence proof, and known incomplete uploads for one exact-ID abort plus absence proof, within the same total budget. Cleanup is never retried. If cancellation or network delay consumes the reserve, the manifest remains for an operator. SIGKILL, power loss, and filesystem hangs cannot guarantee automatic cleanup or a wall-clock exit deadline.

## Failure, journal, and operator reconciliation

The append-only manifest records run scope, sample digest, each pre-dispatch intent, sanitized acknowledgements with returned IDs/ETags, verified observations, and held/remaining cleanup handles. It contains no credential payload, SDK errors, request bodies, authorization headers, or raw SDK responses. Protect it as operational metadata. A failed persistence barrier freezes **all** subsequent remote calls, including cleanup. A truncated trailing JSONL record after a crash is not authoritative; retain earlier durable records, and conservatively treat an unmatched intent as possibly dispatched.

Lost Create replies may leave an upload whose ID is unknown; lost Complete replies may leave a completed version whose ID is unknown. Both are held for manual scoped reconciliation. Completion ambiguity is never treated as an incomplete upload to abort or an object safe to delete. No retries, replay, bare-key deletes, bucket sweeps, automatic recovery, or loading of operator-edited manifests exists. Even a cleanup ACK remains unresolved until its separate absence proof succeeds. Exact returned IDs may already be present in an ACK if a later barrier fails; inspect durable records rather than assuming a final summary is complete.

The CLI prints only sanitized phase/error class, remaining count, and manifest location, not returned opaque IDs. The in-process result and protected manifest retain exact allowlisted key/upload/version identifiers. For remaining handles, an operator must review the manifest and obtain approval for any additional scoped read/reconciliation or exact-version cleanup. Do not expand authorization or list a shared bucket to resolve an unknown outcome. Never infer ownership from prefix alone when selecting versions to delete.

## What this does not qualify

Offline tests use injected SDK fixtures and a real-SDK synthetic redirect handler, never B2. A future `synthetic-qualified` result means only that this standalone protocol probe completed against the approved scope. It is **not** production rollout approval, whole-account qualification, verification of private bucket ACLs, latency/cost characterization, provider crash recovery, or PostgreSQL catalog/journal validation.

The existing provider API was reviewed: `S3ObjectStoreRepository` requires exact versions for reads/deletes; multipart `putFile` takes `multipartAttemptToken` and `multipartJournal`, with `beforeCreate`, `uploadCreated`, `beforeComplete`, `receivedResult`, `abortResult`, and `failure` events. Its journal does not expose a before-every-part barrier. This harness therefore uses direct bounded SDK commands and its own fsynced manifest instead of claiming to validate the production provider's multipart/cancellation/catalog integration. Those paths retain their separate offline and PostgreSQL tests and require a separately reviewed live integration probe if needed.

## Offline verification

```sh
MISE_AUTO_INSTALL=0 mise exec -- pnpm test --run src/utils/b2-qualification.spec.ts
MISE_AUTO_INSTALL=0 mise exec -- pnpm test --run
MISE_AUTO_INSTALL=0 mise exec -- pnpm check
MISE_AUTO_INSTALL=0 mise exec -- pnpm lint
MISE_AUTO_INSTALL=0 mise exec -- pnpm format
MISE_AUTO_INSTALL=0 mise exec -- pnpm build
```

Never interpret offline success as a live B2 result.
