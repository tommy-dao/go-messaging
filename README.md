# go-messaging

[![Go Reference](https://pkg.go.dev/badge/github.com/tommy-dao/go-messaging.svg)](https://pkg.go.dev/github.com/tommy-dao/go-messaging)
[![Go Version](https://img.shields.io/badge/go-1.25%2B-blue)](https://go.dev/dl/)

A lightweight transactional inbox/outbox messaging library for PostgreSQL, written in Go. Uses only `database/sql`.

Two entry points — `Receive` (idempotent, dedup-backed) and `Enqueue` (idempotent via `message_id`) — funnel into one shared hot table, drained by `ProcessBatch` and routed to handlers by `Message.Topic`.

---

## Features

- **Idempotent `Receive`** — dedup table (`message_id` is the global dedup key) prevents duplicate processing across restarts and redeliveries.
- **Transactional `Receive`/`Enqueue`/`EnqueueBatch`** — participate in the caller's `*sql.Tx` so the message and the business write commit atomically.
- **At-least-once processing** — `ProcessBatch` claims rows with `FOR UPDATE SKIP LOCKED`, routes by topic, retries with backoff.
- **Ownership-guarded terminal transitions** — a worker whose row was reclaimed by `RecoverStuck` can't clobber the new owner's result.
- **FAILED rows stay in hot as a DLQ** — not archived immediately. `RetryFailed`/`RetryOne` re-queue them; `ArchiveExhausted` archives them after `Config.FailedRetention`.
- **Admin/query API** — `DeadLetter`, `RetryFailed`, `RetryAllFailed`, `RetryOne`, `GetMetrics`, `ListFailed`/`ListPending`, `FindByMessageID`, `ListArchived`, `IsDuplicate`, `RemoveDedup`.
- **Pluggable backoff** — `FixedBackoff`, `LinearBackoff`, `ExponentialBackoff`, or your own.
- **Pluggable metrics** — `Metrics` interface; bring Prometheus, OpenTelemetry, or anything else.
- **Pluggable logger** — `Logger` interface; structured events at DEBUG / INFO / WARN / ERROR.
- **Versioned at-rest encryption** — `WithCiphers` encrypts payload+headers for the whole instance; ciphertext carries a version prefix so key rotation needs no schema change, and a row that fails to decrypt is dead-lettered individually instead of failing the whole batch.
- **Optional built-in `Runner`** — an interval-mode polling loop, or drive `ProcessBatch` yourself from your own cron.

---

## Installation

```bash
go get github.com/tommy-dao/go-messaging
```

---

## Quick start

### 1. Run the migration

```go
import (
    "context"
    "database/sql"

    _ "github.com/lib/pq"
    message "github.com/tommy-dao/go-messaging"
)

db, _ := sql.Open("postgres", "postgres://user:pass@localhost/app?sslmode=disable")

// Creates: message_order_dedup, message_order_hot, message_order_archive
if err := message.Migrate(context.Background(), db, "order"); err != nil {
    log.Fatal(err)
}
```

`Migrate` is idempotent — every statement uses `IF NOT EXISTS`. To use your own migration tool (golang-migrate, goose …) call `message.Schema(name)` to get the raw DDL strings. `message.DropSchema(ctx, db, name)` drops all 3 tables (destructive — test/decommission only).

### 2. Build the instance

```go
cfg := message.Config{
    Name:              "order",       // scopes tables: message_order_*
    DefaultMaxRetries: 5,
    DefaultBackoff:    message.ExponentialBackoff{BaseDelay: time.Second, MaxDelay: 5 * time.Minute},
    ClaimTimeout:      5 * time.Minute,
    DedupTTL:          24 * time.Hour,
    FailedRetention:   7 * 24 * time.Hour,
    WorkerID:          hostname,
    Concurrency:       10, // max goroutines per ProcessBatch call
}

m, err := message.New(db, cfg)
```

---

## API reference

### Constructor

| Function | What it does |
|---|---|
| `message.New(db, cfg, opts...)` | Build a `*Messaging` instance. Returns an error for an invalid `cfg.Name` or cipher config. |
| `message.Migrate(ctx, db, name)` | Apply DDL idempotently. |
| `message.Schema(name)` | Return raw DDL strings. |
| `message.DropSchema(ctx, db, name)` | Drop all 3 tables (destructive). |

### Methods on `*Messaging`

| Method | What it does |
|---|---|
| `Receive(ctx, tx, msg)` | Idempotently store an inbound message. Returns `isNew bool`. `tx` optional. |
| `Enqueue(ctx, tx, msg)` | Insert a message, idempotent via `message_id`. `tx` nil → own internal tx. |
| `EnqueueBatch(ctx, tx, msgs)` | Insert multiple messages in one round-trip. |
| `ProcessBatch(ctx, size, handler, opts...)` | Claim up to `size` rows and process each — `handler` explicit, or `nil` to route by topic via `Register`/`RegisterDefault`. |
| `Register(topic, handler, opts...)` | Bind a handler to a topic for the registry-routed `ProcessBatch(ctx, size, nil)` path. |
| `RegisterDefault(handler, opts...)` | Fallback handler for unregistered topics. |
| `DeadLetter(ctx, id, reason)` | Force a row to `FAILED` (admin, no ownership check). |
| `RetryFailed(ctx, opts)` | `FAILED` → `PENDING`, bounded (default limit 1000). |
| `RetryAllFailed(ctx, opts)` | Drains all `FAILED` rows present as of the call, batched. |
| `RetryOne(ctx, id)` | `FAILED` → `PENDING` for one row. |
| `GetMetrics(ctx, opts)` | Row counts by status. |
| `ListFailed`/`ListPending(ctx, opts, page)` | Page hot rows by status. |
| `FindByMessageID(ctx, id)` | One hot row, or `nil`. |
| `ListArchived(ctx, opts, page)` | Page archive rows. |
| `IsDuplicate(ctx, id)` / `RemoveDedup(ctx, id)` | Dedup table lookup/removal. |
| `PurgeExpiredDedup(ctx, batchSize)` | Delete dedup rows past `expire_at`, batched. |
| `RecoverStuck(ctx)` | Reset rows stuck in `PROCESSING` longer than `ClaimTimeout`. |
| `ArchiveExhausted(ctx, batchSize)` | Archive `FAILED` rows older than `FailedRetention`. |
| `PurgeOldArchive(ctx, retentionMillis, batchSize)` | Delete old archive rows, batched. |
| `RunAll(ctx)` | One pass of `PurgeExpiredDedup` + `RecoverStuck` + `ArchiveExhausted`. |

### Options

| Option | What it does |
|---|---|
| `WithBackoff(b)` | Override the default backoff strategy. |
| `WithMetrics(m)` | Set a `Metrics` implementation. |
| `WithLogger(l)` | Set a structured logger. |
| `WithCiphers(currentVersion, ciphers...)` | Enable at-rest encryption for every message this instance writes. |

---

## Receive / Enqueue / ProcessBatch

```go
// In the Kafka consumer goroutine — store only, no processing yet.
isNew, err := m.Receive(ctx, tx, &message.Message{
    MessageID:    string(kafkaMsg.Key), // dedup key
    Topic:        "order.created",
    PartitionKey: "billing-service",    // metadata only, not part of any key
    Payload:      kafkaMsg.Value,
    Source:       "kafka",
})

// Atomic with a business write — same tx.
tx, _ := db.BeginTx(ctx, nil)
defer tx.Rollback()
tx.ExecContext(ctx, `INSERT INTO orders ...`)
m.Enqueue(ctx, tx, &message.Message{
    MessageID: orderID, // required — enqueue's idempotency key
    Topic:     "order.confirmed",
    Payload:   []byte(`{"order_id":1}`),
})
tx.Commit()

// Worker — claim and process both kinds, routed by topic.
m.Register("order.created", func(ctx context.Context, msg *message.Message) error {
    return chargeCustomer(ctx, msg.Payload)
})
m.Register("order.confirmed", func(ctx context.Context, msg *message.Message) error {
    return kafkaWriter.WriteMessages(ctx, kafka.Message{Topic: msg.Topic, Value: msg.Payload})
})
m.ProcessBatch(ctx, 50, nil) // nil handler -> route via Register
```

`handler` return-value semantics:

| Return | Effect |
|---|---|
| `nil` | Archived as `PROCESSED`. Never re-delivered. |
| `error` | Flipped to `RETRY` with backoff. Retried on the next claim. |
| `error` after `MaxRetries` | `FAILED` — **stays in hot as a DLQ**, not archived immediately. |

Handlers can also be passed explicitly (bypassing the registry): `m.ProcessBatch(ctx, 50, handlerFn)`.

---

## Dead-letter queue

FAILED rows stay in the hot table (visible via `ListFailed`) until you re-queue or archive them:

```go
// Re-queue one message after fixing the root cause.
m.RetryOne(ctx, messageID)

// Re-queue up to 1000 oldest FAILED rows for a topic.
m.RetryFailed(ctx, message.RetryFailedOptions{FilterOptions: message.FilterOptions{Topic: "order.created"}})

// Drain everything currently FAILED.
m.RetryAllFailed(ctx, message.RetryAllFailedOptions{})

// Force a row to FAILED (e.g. from an admin tool).
m.DeadLetter(ctx, messageID, "operator override")
```

---

## Cleanup

Wire `RunAll` into your scheduler (ticker or Kubernetes CronJob), or call the individual methods:

```go
if _, err := m.RunAll(ctx); err != nil {
    log.Println("cleanup:", err)
}
// Equivalent to:
//   m.PurgeExpiredDedup(ctx, 0)
//   m.RecoverStuck(ctx)
//   m.ArchiveExhausted(ctx, 0)
```

`ArchiveExhausted` and `PurgeExpiredDedup`/`PurgeOldArchive` process one batch per call — call periodically rather than expecting a single call to drain everything.

---

## Built-in Runner (optional)

```go
runner := message.NewRunner(m, message.RunnerOptions{BatchSize: 50}, nil)
runner.Start(ctx)
defer runner.Stop()
```

Interval-mode only: busy (last batch non-empty) re-polls after `PollInterval` (default `0` — immediate re-poll for throughput); idle re-polls after `IdleInterval` (default `200ms`). No cron mode is built in — use a cron library calling `ProcessBatch` directly if you need wall-clock scheduling.

---

## Backoff

Three built-in strategies — or implement `Backoff` yourself.

```go
type Backoff interface {
    Next(retryCount int) time.Duration
}
```

| Type | Formula |
|---|---|
| `FixedBackoff{Interval}` | `Interval` |
| `LinearBackoff{BaseDelay}` | `BaseDelay × retryCount` |
| `ExponentialBackoff{BaseDelay, MaxDelay}` | `min(BaseDelay × 2^(n-1), MaxDelay)` |

```go
m, _ := message.New(db, cfg, message.WithBackoff(
    message.ExponentialBackoff{BaseDelay: 500 * time.Millisecond, MaxDelay: 2 * time.Minute},
))
```

Note: `RecoverStuck` does not apply backoff to rows it reclaims (they become immediately claimable) — this bounds a crashing handler via `retry_count`/`max_retry` rather than delaying recovery.

---

## Metrics

Implement `message.Metrics` to plug in Prometheus, OpenTelemetry, etc. Default is `NoopMetrics{}`.

```go
type Metrics interface {
    MessageProcessed(topic string)
    MessageRetry(topic string)
    MessageFailed(topic string)
    DedupHit()  // Receive() returned isNew=false (duplicate)
    DedupMiss() // Receive() returned isNew=true (new)
}
```

```go
m, _ := message.New(db, cfg, message.WithMetrics(&PromMetrics{...}))
```

---

## Logger

Implement `message.Logger` to receive structured events. Default is `NoopLogger{}` — nothing logged.

```go
type Logger interface {
    Log(ctx context.Context, level LogLevel, event string, msg *Message)
}
```

`LogLevel` values: `DEBUG`, `INFO`, `WARN`, `ERROR`.

Use `LoggerFunc` to wire up any function without defining a type:

```go
m, _ := message.New(db, cfg, message.WithLogger(message.LoggerFunc(
    func(ctx context.Context, level message.LogLevel, event string, msg *message.Message) {
        slog.InfoContext(ctx, event,
            "level",      level.String(),
            "message_id", msg.MessageID,
            "topic",      msg.Topic,
            "status",     string(msg.Status),
        )
    },
)))
```

Events emitted:

| Event | Level | Trigger |
|---|---|---|
| `enqueue` | INFO | `Enqueue` succeeded |
| `enqueue.error` | ERROR | `Enqueue` failed |
| `receive` | INFO | `Receive` stored a new message |
| `receive.duplicate` | DEBUG | `Receive` deduplicated (no-op) |
| `receive.error` | ERROR | `Receive` failed |
| `processed` | INFO | Handler returned nil, message archived |
| `process.retry` | WARN | Handler returned error, retry scheduled |
| `process.failed` | ERROR | Max retries exceeded, FAILED (stays in hot as DLQ) |
| `process.unhandled` | ERROR | No handler registered for topic, dead-lettered |
| `process.decrypt_failed` | ERROR | Row could not be decrypted, dead-lettered |

---

## Encryption

Encryption is instance-wide — on iff at least one cipher is provided to `WithCiphers`, never toggled per message or per handler.

```go
cipher := message.NewDefaultCipher(secret) // AES-256-GCM, version "v0.0.0"
m, _ := message.New(db, cfg, message.WithCiphers(cipher.Version(), cipher))
```

Stored ciphertext carries its cipher version as a prefix (`"<version>:<base64>"`, itself a valid JSONB scalar), so **key rotation needs no schema change and no backfill** — add a new cipher version and point `currentVersion` at it, keeping old versions in the set as long as any stored row (including DLQ) still uses them:

```go
v1 := message.NewAES256GCMCipher("v1", oldSecret)
v2 := message.NewAES256GCMCipher("v2", newSecret)
m, _ := message.New(db, cfg, message.WithCiphers("v2", v1, v2)) // v2 encrypts new writes; v1 still decrypts old rows
```

A row that fails to decrypt (missing cipher version, wrong key, auth failure) is **dead-lettered individually** during `ProcessBatch` — it does not fail the rest of the batch. Query methods (`ListFailed`, `FindByMessageID`, ...) use best-effort decryption and return the raw ciphertext on failure rather than erroring, so a broken cipher never hides DLQ rows from listing.

Implement `MessageCipher` yourself for KMS/HSM-backed keys — the engine only calls `Version()`/`Encrypt()`/`Decrypt()`.

---

## Database schema

Three tables, scoped by `Config.Name` (empty name → unscoped `message_hot`/`message_dedup`/`message_archive`):

| Table | Purpose |
|---|---|
| `message_<name>_dedup` | `Receive()`-only idempotency — `message_id TEXT PRIMARY KEY` (global dedup). Pruned by `PurgeExpiredDedup`. |
| `message_<name>_hot` | Live queue + DLQ — `message_id TEXT PRIMARY KEY`. Holds `PENDING`, `PROCESSING`, `RETRY`, and `FAILED` rows. |
| `message_<name>_archive` | Append-only history — surrogate `id BIGINT` PK, `message_id` **not** unique (a message reprocessed after its dedup TTL expires adds a new history row). Holds `PROCESSED` and archived-`FAILED` rows. |

Full DDL: [`schema.go`](schema.go).

---

## Message lifecycle

```
Receive(ctx, tx, msg)          Enqueue(ctx, tx, msg)
    │  dedup + hot insert           │  hot insert (idempotent via message_id)
    └───────────────┬────────────────┘
                     ▼
              message_<name>_hot (status=PENDING)
                     │
                     ▼  ProcessBatch: claim (FOR UPDATE SKIP LOCKED), status=PROCESSING
          ┌──────────┼───────────────────┐
       handler ok   retry left        retry exhausted
          │             │                    │
          ▼             ▼                    ▼
   archive PROCESSED  status=RETRY      status=FAILED
   + DELETE from hot  + next_retry_at   (stays in hot as DLQ)
```

FAILED rows are re-queued via `RetryFailed`/`RetryOne`/`RetryAllFailed`, or archived via `ArchiveExhausted` after `Config.FailedRetention`. All terminal transitions (`markDoneAndArchive`/`markRetry`/`markFailed`) are guarded by `processing_by = $worker AND status = 'PROCESSING'`, so a worker that lost ownership to `RecoverStuck` can't overwrite the new owner's result.

---

## Testing

Requires Docker (testcontainers spins up a real PostgreSQL 16 container):

```bash
go test ./...
go test ./... -race
```

---

## License

Released under the MIT License — see [LICENSE](LICENSE).
