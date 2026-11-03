# Managed upload publication consumers

Managed uploads retain the existing immutable `upload_publication_outbox` records. The new `publication_consumer` table is the authoritative consumer receipt/acknowledgement ledger; the original outbox's `pending` state is not rewritten. Initial consumer IDs equal their immutable originating outbox IDs. Database triggers bind each consumer to the exact originating effect, published receipt, asset and owner, and prohibit identity changes. Derived metadata events receive a new persisted ID, unique by originating metadata effect and consumer kind.

## Activation and worker roles

`PublicationConsumerService` is registered in the application services registry; its bootstrap/shutdown hooks run only in the microservices role, after plugin loading. Only `IMMICH_OBJECT_UPLOAD_ENABLED=true` starts polling. The default disabled configuration does not query these tables or start timers. Concurrent bootstrap calls share the pending tick. Shutdown stops scheduling and awaits that tick. Dispatch failures are caught by the background loop and remain SQL work for later ticks.

Polling reconciles SQL effects and run records, not Redis job history. Dispatch leases are distinct from execution claims. A successful Bull enqueue never marks an effect or run complete. Pending work is redispatched after the SQL dispatch lease expires, including jobs accepted by a queue and subsequently lost/removed. Publication Bull IDs are stable, removable optimizations; context-free jobs retain existing options. `acknowledgedAt` can only be populated for SQL-done consumers. If a committed consumer's dispatch acknowledgement fails, replay observes SQL-done and does not repeat its metadata or workflow execution.

## Metadata

Only jobs carrying a publication identity enter the new path. The identity is verified against the SQL ledger, not trusted merely because it appeared in job data. The consumer is claimed before metadata is read or modified. Done, active and held claims skip execution. A missing/deleted asset is recorded as an explicit skipped result, not successful extraction.

The existing metadata extraction handler processes the retained source path, with existing motion-photo/tag/face/native operations unchanged. It does not emit the generic metadata-extracted event or independently publish successful job status in the publication path. Successful extraction commits metadata status, consumer completion and a stable derived event in one transaction. Lock waits are followed by database-clock expiry checks, including a last conditional completion update after status persistence. No queue, event emission, native extraction or plugin call runs inside a SQL transaction.

Partial failures, unreported/Failed results, expired started metadata work, or failed final fencing are held for manual investigation. Metadata side effects (especially motion photos and tags) are not assumed replayable. There is no automatic reset of held metadata. Renewal is singleflight, uses database time, and its pending operation is awaited before final completion/cleanup. `JobComplete` and Redis completion are not inputs to the ledger.

## Workflow identity and cohorts

Each asset-create workflow effect and each derived metadata event freezes its enabled, owned workflow cohort and marks the fanout consumer complete in the same SQL transaction. Empty cohorts are completed too: replay cannot enroll workflows created later. The separate run ledger has one persisted UUID per originating consumer/workflow pair. Database checks reject cross-owner and post-freeze enrollment and identity mutation. Workflow logs remain optional/deletable diagnostics, not deduplication records.

Publication workflow jobs load/verify their persisted run and claim it before executing the existing workflow steps using that same run UUID for logs. Run result and completion are fenced SQL writes. Done, active and held runs do not execute again. Context-free workflow jobs continue to generate a fresh UUID each time and use existing logging behavior.

External plugin HTTP may already have been accepted when the process crashes or the lease expires. Such runs become held; there is no automatic external HTTP retry, fresh UUID retry, or automatic resume. No manual replay/reset API is implemented. An operator must investigate external outcomes before any separately designed recovery action. This is not universal exactly-once delivery.

## Events and quota

Managed asset-create events carry optional publication context and deliberately omit `file`: publication already charged quota transactionally. The workflow event handler suppresses generic fanout for these events because the independent workflow effect owns the cohort. Derived metadata events likewise suppress generic workflow fanout; their persisted effect owns its frozen cohort. Existing local/file events, local quota accounting, context-free jobs and optional workflow logging remain unchanged.

Other event handlers (notifications, websocket handlers, storage follow-ups) are **at least once**. A crash after event delivery and before consumer completion can duplicate those handlers. They are not universally idempotent, and this implementation does not claim otherwise.

## Qualification boundary

The new PostgreSQL suite runs the actual new migration (including down/up), existing subsystem migrations, and a minimal application schema. Its vertical test uses actual authenticated multipart HTTP, the existing SDK loopback publication fixture, the registered worker implementation's ticks, real metadata/workflow service handlers and SQL ledgers. Queue acceptance/loss, metadata-native results, plugin implementations and peripheral repositories are controlled fixtures. A plugin failure test makes an actual loopback HTTP request before throwing. This does not qualify the complete production Redis/native/WASM stack or the entire historical migration chain.

No source retention, object deletion, remote object cleanup, HTTP/core publication behavior, dependencies or lockfiles are changed.
