# Upload publication foundation — incomplete application vertical

This is an experimental source/stage foundation with a separate [bounded internal publication core](upload-publication-core.md), **not authenticated POST new-upload publication** and **not deployment-ready**. `POST /assets`, its interceptors, DTO handling, upload service and upload configuration dispatch have not been changed. No new-upload flag is enabled or implemented here. Normal deletion of cloud-backed assets still requires a future durable tombstone/deletion workflow.

## Implemented boundary

`OwnedSourceService.receive` validates an already-provisioned canonical service-owned 0700 source root, persists an owner-scoped `source_receipt`, then exclusively creates a server-allocated UUID filename with `O_EXCL`/`O_NOFOLLOW`. It computes server SHA256, legacy SHA1 and actual size from consumed bytes; it fsyncs the file and directory before recording received inode evidence. Original and sidecar roles are supported. Sources are not asset rows.

`seal` first claims a received receipt in SQL. It reopens without following symlinks, checks recorded inode/stat identity and full SHA256, applies the caller's validated modification timestamp, makes the file readonly, fsyncs file and directory, and records the final identity. The caller must supply a validated DTO timestamp; this helper does not perform HTTP DTO validation. A failed or interrupted receive/seal stays attributable to its receipt. There is no unlink/delete/release API. A failed database acknowledgment can leave writing/received/sealing/sealed evidence; all states intentionally retain files rather than guessing whether a write committed.

`ObjectCatalogService.stageOwned` accepts a source receipt ID and authenticated owner ID, not a path. `reserveOwnedStage` locks the sealed source and atomically inserts a creating catalog stage plus restrictive `owned_stage_receipt` before any copy. The copy core is shared with the existing `stage` method and verifies the source's sealed stat/hash/size. A retry reuses a ready reserved stage; an existing incomplete reservation is held, not automatically recopied or replaced. It never adopts an existing arbitrary stage.

The new migration registers both tables and ownership/evidence checks, composite restrictive foreign keys, and supporting indexes. Receipt-backed stages are guarded from catalog cleanup in every state. Generic `prepare` and legacy `registerClaim` reject receipt-owned stages: no generic upload or post-upload adoption is available. The original fresh legacy-stage claim contract remains available for legacy stages. The separate publication core now supplies a receipt-bound one-attempt intent and transaction-bound promotion API; see its document for its trusted internal interface and qualification boundary.

## Deliberate retention and trust boundary

Roots must be provisioned by the operator, private to the service UID, and not concurrently manipulated by other privileged/service-UID processes. This is not a defense against a hostile service UID or direct privileged SQL. Source and staged copies intentionally remain retained, including partial/held writes. No automatic remote cleanup, source cleanup, release, eviction or deletion is introduced. These helpers are not registered into the multipart interceptor and cannot protect existing upload error hooks until the coordinator slice integrates them.

## Remaining application work

The complete requested route remains unfinished:

- Existing authorized multipart route dispatch before the legacy catch; checksum shortcut bypass prevention; durable source-aware cleanup hooks.
- Durable metadata/event/workflow consumers and completion identities for the core's pending-only outbox. Workflow execution currently creates random run IDs and allows arbitrary HTTP side effects. Stable Redis job IDs alone cannot make this replay-safe. The next slice needs a durable trigger/run identity and an explicit ambiguous external-effect policy, not a generic replayed `AssetCreate` event carrying the file (which also charges usage again).
- Actual authenticated POST multipart/SDK loopback/original GET vertical, disconnect and postcommit queue-failure coverage.
- Tombstones/deletion and retention bounds before deployment.

The bounded internal core implements the receipt/fingerprint, one-attempt reconciliation, full SDK GET verification, atomic publication/quota/binding/outbox, and rollback-safe duplicate policy; these are not evidence of POST or consumer integration.

The catalog/SDK/GET regression suite is not evidence that this unfinished POST publication vertical works. Parent review is required before any publication or enablement.
