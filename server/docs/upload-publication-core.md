# Bounded internal upload publication core

This is an internal coordinator, not an enabled upload route. No POST, interceptor, Multer cleanup hook, queue dispatcher, event emission, workflow execution, source eviction, remote deletion, or deployment enablement is included. Sources, staged copies, and exact remote versions remain retained. Normal object-backed deletion still requires tombstones before deployment.

## Trusted entry point

Construct `UploadPublicationService` with the actual catalog repository/service and one trusted, fixed `PublicationStoreBinding`: `provider: 's3'`, stable `storeId`, physical endpoint/bucket, and the matching SDK-backed store. These are server wiring, never upload DTO fields. The constructor snapshots the binding. It does not read configuration or credentials.

`publish` accepts an authenticated owner, original sealed source receipt ID, optional sealed sidecar receipt ID, complete validated publication options, and trusted authentication provenance. Provenance contains only a session/API-key/shared-link ID, plus shared-link type and album context when applicable. It must come from the future protected adapter, not from request body fields. No caller-supplied asset UUID, stage ID, intent ID, object key, version ID, path, or storage credentials are accepted.

Complete options contain filename, asset type, creation/modification dates, favorite, visibility, duration or null, live-photo video ID or null, and metadata entries. The future DTO adapter must supply these defaults explicitly. Dates are normalized, metadata keys are sorted and bounded, nested JSON is snapshotted, and filename/path traversal is rejected. The modification timestamp must already have been applied by source sealing to both original and sidecar.

## Receipt and fingerprint

Migration `1793500004000-AddUploadPublication` adds the hidden `upload_publication` and `upload_publication_outbox` tables and the owned-stage composite identity needed by the new foreign key. Decorated tables, indexes, constraints, migration ORDER, and real migration up/down/up are exercised against PostgreSQL.

The `v1:` SHA256 fingerprint covers owner, server-sealed original SHA256/size, explicit sidecar SHA256/size or absence, every normalized option, credential-free authentication context, and fixed physical store identity. An explicit printable ASCII idempotency key is bounded to 200 bytes; otherwise the fingerprint is the key. The owner/key pair and original source reservation are unique. A retry must retain the same key. An alternate key for an already reserved source is rejected rather than acknowledged without persisting a key alias. Same-key changed input returns conflict. Identical content/options received again with the same key joins the existing receipt; the additional source remains independently retained.

The receipt reserves a new asset UUID before staging without inserting an asset row. Restrictive composite foreign keys pin sealed original and sidecar inode/hash/size evidence, owner/source/stage association, intent/stage, physical store, published reserved asset/owner, original claim identity, and exact verified object version. The terminal result is stored in the same transaction as publication. `get(receiptId, ownerId)` exposes owner-scoped state for the future protected cleanup adapter. `PublicationHeld.receiptId` allows reconciliation without treating a coordinator error as cleanup permission.

## Safe state transitions

1. Reserve the receipt and future asset UUID in SQL.
2. `stageOwned` validates the publication owner/source and commits the receipt's owned-stage association in the same transaction that reserves the creating stage, before copying. A ready stage is reused; incomplete creation stays held without recopying.
3. `prepareOwned` locks the receipt and creates or returns its one intent, binding the fixed store before any provider call. Generic prepare, generic sending without the receipt permit, and legacy `registerClaim` continue rejecting receipt-owned stages.
4. Owned sending atomically records the sending intent token on the receipt before dispatching the real SDK. The existing DB-time renewal, multipart journal, acknowledgement fencing and exact received-handle retention remain authoritative.
5. Prepared intents can be sent once. Sending/ambiguous intents are held, even after lease expiry. Acknowledged intents resume only independent exact-version full GET verification; no automatic PUT follows an acknowledgement or uncertain outcome. Unknown multipart Complete retains its UploadId/manifest/evidence and stays held.
6. Only a successful full streamed GET through the actual injected store permits SQL promotion.

Workers use a nonblocking session advisory lock. Contention returns held instead of occupying SQL connections waiting for another worker. The lock's pinned connection is reused for all worker SQL, including catalog operations, so a one-connection pool can make progress. No SQL transaction spans filesystem copy or network IO. Loss of a session lock is not proof an old network worker stopped: owned-stage creation and the one-attempt intent/CAS journal remain the safety barriers. There is no expired-lease takeover authorizing another PUT.

## Atomic publication transaction

The transaction locks the receipt, current owner/quota row, sealed sources, current applicable auth grants, stage/intent/version and live-photo dependencies in a consistent order. Quota uses `FOR NO KEY UPDATE`, serializing quota changes without unnecessarily blocking the key-share lock of a concurrent legacy asset insertion. Session/shared-link expiry is evaluated with DB time after acquiring the auth-row lock, and authorization is checked again after the final writes.

Promotion rechecks active/nondeleted owner, session ownership/expiry or current API-key upload/all permission, or current shared-link owner/type/album/allowUpload/expiry. Album links also require a current nondeleted album and the link owner's owner membership. Live-photo videos must still be owned, nondeleted videos. No credential columns are needed by these checks.

It proves the receipt/owned source/stage hash and size, acknowledged intent token, S3 store/key/version, and independently verified catalog version agree. It then inserts the reserved asset with its retained original source path, EXIF size, metadata, sidecar asset file, shared-link/album membership, and live-photo hiding. A dedicated transaction-local promotion creates the receipt-backed claim, exact verified original binding and object authority without weakening legacy claim registration. Quota is charged once. The terminal result and pending outbox effects commit atomically. There is no Redis, event, workflow, filesystem operation or network call inside this transaction.

A checksum conflict rolls the whole transaction back and resolves the winner in a fresh transaction. Existing local and object duplicates return the unchanged winner, with no authority upgrade, remapping, quota charge, live-photo hiding or new outbox effects. A local duplicate is **not** a claim of cloud durability. Losing source/stage/version evidence remains held; there is no remote delete or overwriting of a winning version.

After any coordinator or serialization-release error, the coordinator returns a persisted terminal result if readable, otherwise a held reconciliation result. It never removes a published asset, source or stage.

## Durable effects only

Each successful new publication creates one pending row per metadata/event/workflow effect with persisted effect UUID, receipt identity, target and immutable payload containing asset ID, owner ID and `quotaAlreadyCharged: true`. Targets are `AssetExtractMetadata` and `AssetCreate` (the event and workflow identities are separate). No consumer completion is asserted; the schema currently allows pending only. Duplicates create no effects.

The next phase must implement consumer SQL receipts and workflow-run/fanout identities, not blindly replay the existing event bus. Existing `AssetCreate` handlers can trigger workflows, so a dedicated workflow effect plus an unfiltered generic event replay would duplicate fanout. Event delivery must omit `file`, because SQL has already charged quota. Queue acceptance/job IDs are not consumer completion. Ambiguous metadata/workflow execution needs a held policy; arbitrary workflow HTTP effects cannot universally be exactly once.

## Qualification boundary

The publication suite uses a disposable private PostgreSQL database with an explicitly minimal asset/user/auth/EXIF/metadata/file/membership fixture and the actual catalog, multipart, original, source and publication migrations. It uses actual source receive/seal/copy, catalog intent/journal/verify, and the actual AWS SDK over loopback HTTP. Publication tests do not manufacture verified rows. Their valid single-PUT loopback response is gated on the actual SDK input body's public `end` event, so the fixture does not accidentally simulate an early provider acknowledgement before the local validator consumes EOF. The production store's conservative early-response rejection remains unchanged. This is not qualification of the entire upstream migration chain, Docker/Redis stack, authenticated POST path, real cloud service, or deployment.
