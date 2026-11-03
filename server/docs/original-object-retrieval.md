# Original-object retrieval: first application slice

This slice adds an internal, owned-stage publication binding and cloud retrieval on the existing `GET /assets/:id/original` route (including Express HEAD handling). It does **not** integrate new uploads. No external API accepts store IDs, object keys, provider version IDs, catalog version IDs or ownership claims.

## Default and server-only configuration

`IMMICH_OBJECT_READ_ENABLED` is absent/`false` by default. In that mode the factory creates no S3 client, reads no credential fields, does no filesystem provisioning and uses no AWS credential chain. Assets whose `originalAuthority` is `local` continue through the unchanged `sendFile` helper. An asset marked `object` fails closed if configuration or its mapping is unavailable; disabling reads does not restore local authority.

Apply the application migration with reads disabled before enabling this flag. Enabled startup requires the durable store-identity table; a missing migration is a startup failure, not a reason to create tables ad hoc or fall back locally.

Enabling reads requires all of these server environment values:

- `IMMICH_OBJECT_STORE_ID`: stable, bounded identifier, never repurposed for another physical store.
- `IMMICH_OBJECT_ENDPOINT`: HTTPS origin only, without userinfo, path, query or fragment.
- `IMMICH_OBJECT_REGION` and `IMMICH_OBJECT_BUCKET`.
- `IMMICH_OBJECT_PROCESSING_ROOT`: already-provisioned, absolute canonical directory, owned by the service UID, mode 0700. Symlinked/non-private roots fail validation. This reserves a validated processing boundary; this slice does not move native jobs to it.
- `IMMICH_OBJECT_ACCESS_KEY_ID` and `IMMICH_OBJECT_SECRET_ACCESS_KEY`: explicit server-only credentials. They are never public system config, request input, URLs or logged configuration errors.

The injected registry snapshots configuration and durably registers store ID / endpoint / bucket. Reusing a store ID with another endpoint or bucket fails startup. Published mappings retain this tuple with a restrictive composite FK. Client construction is lazy at Nest provider initialization, not module import, and performs no S3 operation. Nest destruction closes the owned client once. The current factory registers one configured store; an unconfigured historical store ID fails closed, never routes to a replacement store.

Reads and existing bindings are independent of any future new-upload switch. The factory deliberately does not consult `IMMICH_OBJECT_NEW_UPLOAD_ENABLED`; no upload coordinator exists in this slice. The current cloud transfer deadline is 30 seconds, covering opening GET and streaming, in addition to SDK connection/request limits. Slow/failed transfers abort rather than silently switch sources.

## Trusted ownership receipt contract for the next coordinator

Existing `ObjectCatalogService.stage` and catalog rows are **not owner-aware**. `registerClaim` does not authenticate a caller or infer ownership from a catalog ID. Before calling it, a future trusted coordinator must establish all of the following:

1. The authenticated ingestion operation has upload access, quota and a durable asset belonging to the intended owner. The staged bytes are the original representation for that asset, not another asset, an edited representation or an arbitrary preexisting catalog object.
2. The coordinator just created/sealed the stage from that operation's owned source. It must call `AssetOriginalRepository.registerClaim({ assetId, ownerId, stageId })` at first use, **before preparing or sending any upload intent**. This records durable ownership/stage association and returns a claim receipt ID. It is not an API for adopting old uploads.
3. Registration locks the stage, sharing the lock used by existing `catalog.prepare`. Any existing intent (even an acknowledged one), released stage or non-ready stage is rejected. Asset/owner composite FK and unique asset/stage claims reject cross-owner and duplicate claims, including concurrent registration.
4. Use the trusted registry's unchanged provider/store binding to upload that stage, acknowledge its exact provider version, and perform the existing full-GET size/SHA256 verification. A successful PUT or opaque S3 ETag is not verification. The internal catalog observation writer is a trusted boundary; never expose it to request input.
5. Publish with `AssetOriginalRepository.bind({ claimId, catalogVersionId })`. The latter is the catalog row UUID, **not** a physical provider version ID. The binder accepts only an acknowledged, verified S3 version from the claimed stage, with a registered physical store. It atomically inserts the asset binding and changes `originalAuthority` from `local` to `object`. It is insert-only, rejects repeated publication and has no rebind operation.

Composite restrictive FKs retain asset-owner/claim-stage, intent-stage, exact version/provider/store/key/version ID/verification, stage SHA256/length, and physical store identity evidence. Changing or deleting referenced evidence fails; verification cannot be downgraded behind a published binding. Deleting a mapping alone does not change the asset's explicit authority: original reads return a sanitized failure, not retained-local bytes. Privileged direct SQL and migration rollback remain operator actions, not supported publication or recovery APIs.

The coordinator's authenticated provenance and failure/recovery orchestration remain future work. Do not call this complete upload integration or use `registerClaim` to retrofit ownership after an upload.

## HTTP behavior and stream ownership

`AssetMediaService.downloadOriginal` retains the real `requireAccess(AssetDownload)` check and `getForOriginal` selection **before** mapping lookup. Owner, shared-album, partner and shared-link permissions retain their existing access branches. Shared links still force edited selection; a selected edited file stays local and never consults the original-object binding. Original/fullsize redirects still terminate at the existing authorized original route.

Only the original controller uses the discriminated original response and dedicated sender. Local original responses still use the untouched legacy helper, including its legacy error-to-404 flattening (such as a local unsatisfiable range). Thumbnail, video and other local endpoints are not globally changed.

Cloud metadata is resolved from the durable publication before opening GET. Content ETag is the verified SHA256, **not** the provider's opaque ETag. Last-Modified is stable publication time. Inline filename, MIME and private cache policy follow the existing original response. HEAD, 304, 412 and 416 do not open GET. Preconditions precede range evaluation; Express's combined-range parser supports inclusive, open-ended and suffix ranges, merges adjacent/overlapping ranges, treats unsupported disjoint multi-ranges as full responses, and never forwards raw client Range text. Unsatisfiable ranges return `Content-Range: bytes */N`.

The sender registers disconnect handlers before awaiting metadata or GET, owns a returned body immediately, checks exact key/version/body length/total length, and counts streamed bytes. It withholds one byte until EOF so an overlong stream cannot appear successfully complete solely by reaching Content-Length. Backpressure uses bounded Node stream buffers and writable drain, not whole-original buffering. Cancellation aborts upstream; all owned streams are closed and awaited and their listeners cleaned. Before headers, failures are sanitized 503; after headers, the response is destroyed rather than emitting JSON into media bytes. No presigns or public object URLs are generated.

## Verification boundaries and exclusions

Tests include disposable real PostgreSQL with a minimal user/asset schema plus the actual catalog/multipart/new application migrations; real staged copies; ownership, exact binding, composite FK rejection, atomic rollback, concurrent registration/publication, defaults and missing mapping. Synthetic verification observations in binding tests isolate this trusted contract; existing catalog tests separately exercise actual full-GET verification.

The Nest/Express HTTP component uses the real controller, service, access-decision branching, reader and sender, controlled authentication/access/database fixtures, actual Node Readables and real loopback HTTP clients for cancellation and broken streams. It is not a full production authentication, Redis, Docker or whole-server-stack qualification.

Explicitly excluded: new upload publication/coordinator, ZIP download, video original fallback, HLS, native processing changes, cloud deletion and local eviction. Claims/mappings intentionally restrict destructive asset/evidence deletion; a durable cloud deletion workflow must be designed before enabling normal cloud-backed asset deletion. Existing `originalPath` files remain on disk so existing jobs keep working. This is **not** the final bounded-disk design and is not permission to delete originals or deploy without independent review.
