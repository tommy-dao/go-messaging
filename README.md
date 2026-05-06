# go-message

[![Go Reference](https://pkg.go.dev/badge/github.com/tommy-dao/go-message.svg)](https://pkg.go.dev/github.com/tommy-dao/go-message)
[![Go Version](https://img.shields.io/badge/go-1.25%2B-blue)](https://go.dev/dl/)

A lightweight **Inbox / Outbox** messaging library for PostgreSQL, written in Go.

It provides:

- **Inbox** — idempotent message receiving with deduplication, retry, and archival.
- **Outbox** — transactional event publishing with relay, retry, and archival.
- **Cleanup** — dedup expiry and stuck-claim recovery.

Both Inbox and Outbox share the same set of database tables (`message_hot`, `message_archive`), differentiated by a `direction` column. Table names can be prefixed for multi-service isolation.

> **Module path:** `github.com/tommy-dao/go-message`
> **Go version:** 1.25+
> **Database:** PostgreSQL (uses `FOR UPDATE SKIP LOCKED`, `JSONB`)

---

## Why this library

Most outbox/inbox libraries either:

1. Run their own background goroutines (hard to integrate with existing schedulers), or
2. Force a particular ORM / query builder.

`go-message` does neither. It exposes the primitives — `Receive`, `Add`, `ProcessBatch`, `PublishBatch`, `Cleanup` — and lets the caller decide *when* and *how often* to invoke them. It uses only `database/sql`, so it works with any driver (`lib/pq`, `pgx/stdlib`, etc.).

Key features:

- Idempotent inbox via a separate dedup table (`(consumer_group, message_id)` unique).
- Transactional outbox: `Outbox.Add` participates in the caller's `*sql.Tx`, so the event becomes visible only when the business transaction commits.
- Pluggable retry backoff: `FixedBackoff`, `LinearBackoff`, `ExponentialBackoff`, or your own.
- Worker scaling via `FOR UPDATE SKIP LOCKED` — many workers can claim batches concurrently without blocking each other.
- No background goroutines — the caller controls scheduling.
- Pluggable metrics (`Metrics` interface) and table prefix (multi-tenant / multi-service).

---

## Installation

```bash
go get github.com/tommy-dao/go-message
```

## Documentation

Full API reference is on **[pkg.go.dev](https://pkg.go.dev/github.com/tommy-dao/go-message)**.
This README covers the high-level shape and runnable examples; godoc has every type and method.

---

## API summary

Everything the caller touches — at a glance.
**Click any name in column 1 to jump to its example below.** Column 2 links to the source file.

### Constructor & migration

| Function                                                                      | File                          | What it does                                                                  |
| ----------------------------------------------------------------------------- | ----------------------------- | ----------------------------------------------------------------------------- |
| [`message.New(db, cfg, opts...)`](#2-build-the-messaging-instance)            | [messaging.go](messaging.go)  | Build the `Messaging` aggregate (`.Inbox`, `.Outbox`, `.Cleanup`).            |
| [`message.Migrate(ctx, db, prefix)`](#1-run-the-migration)                    | [migrate.go](migrate.go)      | Apply all DDL idempotently (uses `IF NOT EXISTS`).                            |
| [`message.Schema(prefix)`](#1-run-the-migration)                              | [schema.go](schema.go)        | Return the raw DDL strings (for golang-migrate / goose / etc.).               |

### Inbox methods

| Method                                                                                          | File                  | What it does                                                                  |
| ----------------------------------------------------------------------------------------------- | --------------------- | ----------------------------------------------------------------------------- |
| [`m.Inbox.Receive(ctx, InboxMessage)`](#receiving-a-message-idempotent)                         | [inbox.go](inbox.go)  | Idempotently store an inbound message. Duplicates return `nil`.               |
| [`m.Inbox.ProcessBatch(ctx, consumerGroup, size, Handler)`](#processing-a-batch)                | [inbox.go](inbox.go)  | Claim up to `size` rows for the group and run the handler on each.            |

### Outbox methods

| Method                                                                                          | File                    | What it does                                                                       |
| ----------------------------------------------------------------------------------------------- | ----------------------- | ---------------------------------------------------------------------------------- |
| [`m.Outbox.Add(ctx, tx, OutboxEvent)`](#adding-an-event-inside-a-business-transaction)          | [outbox.go](outbox.go)  | Insert event in the **caller's** `*sql.Tx`. `tx` must be non-nil.                  |
| [`m.Outbox.AddDirect(ctx, OutboxEvent)`](#adding-an-event-without-a-transaction)                | [outbox.go](outbox.go)  | Insert event using its own short-lived transaction (no business tx).               |
| [`m.Outbox.PublishBatch(ctx, size, Publisher)`](#relaying-events-to-a-broker)                   | [outbox.go](outbox.go)  | Claim up to `size` events and run the publisher on each.                           |

### Cleanup methods

| Method                                                       | File                       | What it does                                                  |
| ------------------------------------------------------------ | -------------------------- | ------------------------------------------------------------- |
| [`m.Cleanup.PurgeExpiredDedup(ctx)`](#one-shot)              | [cleanup.go](cleanup.go)   | Delete dedup rows past `expire_at`.                           |
| [`m.Cleanup.RecoverStuckInbox(ctx)`](#one-shot)              | [cleanup.go](cleanup.go)   | Flip stale `CLAIMED` inbox rows back to `RETRY`.              |
| [`m.Cleanup.RecoverStuckOutbox(ctx)`](#one-shot)             | [cleanup.go](cleanup.go)   | Same for outbox.                                              |

### Options (passed to `New`)

| Option                                                | File                    | What it does                                  |
| ----------------------------------------------------- | ----------------------- | --------------------------------------------- |
| [`message.WithBackoff(b)`](#example-using-a-built-in-strategy)         | [config.go](config.go)  | Override `Config.DefaultBackoff`.             |
| [`message.WithMetrics(metrics)`](#example-prometheus-adapter)          | [config.go](config.go)  | Set a `Metrics` implementation.               |

### Built-in backoff strategies

| Type                                                                              | File                       | Formula                                         |
| --------------------------------------------------------------------------------- | -------------------------- | ----------------------------------------------- |
| [`FixedBackoff{Interval}`](#backoff-strategies)                                   | [backoff.go](backoff.go)   | `Interval`                                      |
| [`LinearBackoff{BaseDelay}`](#backoff-strategies)                                 | [backoff.go](backoff.go)   | `BaseDelay * retryCount`                        |
| [`ExponentialBackoff{BaseDelay, MaxDelay}`](#example-using-a-built-in-strategy)   | [backoff.go](backoff.go)   | `min(BaseDelay * 2^(retryCount-1), MaxDelay)`   |

### Sentinel errors

| Error                              | File                     | When                                                                 |
| ---------------------------------- | ------------------------ | -------------------------------------------------------------------- |
| [`ErrDuplicate`](#errors)          | [errors.go](errors.go)   | Dedup key already exists. `Receive` swallows this and returns `nil`. |
| [`ErrMaxRetry`](#errors)           | [errors.go](errors.go)   | Message has exceeded `MaxRetries`.                                   |
| [`ErrNotFound`](#errors)           | [errors.go](errors.go)   | Requested message was not found.                                     |

---

## Quick start

### 1. Run the migration

```go
import (
    "context"
    "database/sql"

    _ "github.com/lib/pq"
    message "github.com/tommy-dao/go-message"
)

db, _ := sql.Open("postgres", "postgres://user:pass@localhost/app?sslmode=disable")

// Creates: order_message_dedup, order_message_hot, order_message_archive (+ indexes)
if err := message.Migrate(context.Background(), db, "order"); err != nil {
    log.Fatal(err)
}
```

`Migrate` is idempotent — every statement uses `IF NOT EXISTS`.

If you'd rather manage migrations with your own tool (golang-migrate, goose, etc.), call [`message.Schema(prefix)`](schema.go) to get the raw DDL strings:

```go
// Example: dump the DDL into a goose migration file.
stmts := message.Schema("order")
for _, sql := range stmts {
    fmt.Println(sql + ";")
}
```

### 2. Build the Messaging instance

```go
hostname, _ := os.Hostname()
cfg := message.Config{
    TablePrefix:       "order",                                    // -> order_message_*
    DefaultMaxRetries: 5,
    DefaultBackoff:    message.ExponentialBackoff{
        BaseDelay: time.Second,
        MaxDelay:  5 * time.Minute,
    },
    ClaimTimeout: 5 * time.Minute,                                 // stuck-claim recovery threshold
    DedupTTL:     24 * time.Hour,                                  // dedup row expiry
    WorkerID:     hostname,                                        // identifies this instance
}

m := message.New(db, cfg)
// m.Inbox, m.Outbox, m.Cleanup are ready to use
```

---

## Inbox usage

### Receiving a message (idempotent)

`Receive` inserts both a dedup row and a hot row in the same transaction. A duplicate `(consumer_group, message_id)` returns `nil` — duplicate is **not** an error.

```go
err := m.Inbox.Receive(ctx, message.InboxMessage{
    ConsumerGroup: "order-service",
    MessageID:     "kafka-offset-12345",     // unique idempotency key
    EventType:     "order.created",
    Payload:       []byte(`{"order_id":1}`),
    Source:        "checkout-api",
})
```

### Processing a batch

`ProcessBatch` claims up to N pending/retry messages for the given consumer group, runs your handler on each, then archives successes or schedules retries on failure.

```go
processed, err := m.Inbox.ProcessBatch(ctx, "order-service", 50,
    func(ctx context.Context, msg *message.Message) error {
        return doBusinessLogic(ctx, msg.Payload)
    },
)
```

The claim uses `FOR UPDATE SKIP LOCKED`, so multiple workers (or pods) can call `ProcessBatch` simultaneously and each will get a disjoint set of rows.

Run it on whatever schedule you like — every second in a `time.Ticker`, on every HTTP poll, on a Kubernetes CronJob, etc.

---

## Outbox usage

### Adding an event inside a business transaction

```go
tx, _ := db.BeginTx(ctx, nil)
defer tx.Rollback()

// 1. Your business write
if _, err := tx.ExecContext(ctx, `INSERT INTO orders ...`); err != nil {
    return err
}

// 2. Outbox event in the same tx
err := m.Outbox.Add(ctx, tx, message.OutboxEvent{
    EventID:   "order-1-confirmed",
    EventType: "order.confirmed",
    Payload:   []byte(`{"order_id":1}`),
    Source:    "order-service",
})
if err != nil {
    return err
}

// 3. Commit makes both visible atomically
return tx.Commit()
```

If `tx.Rollback()` is called, the outbox event vanishes too — guaranteeing exactly the events your business writes intended.

> **`Add` requires a non-nil `tx`.** Passing `nil` will panic — by design, so accidental non-transactional publishes are caught loudly. If you genuinely have no business transaction to bind to, use `AddDirect` (below) instead.

### Adding an event without a transaction

When you have no business write to bind to (e.g. a standalone publish), use `AddDirect`. It opens its own short-lived transaction internally:

```go
err := m.Outbox.AddDirect(ctx, message.OutboxEvent{
    EventID:   "heartbeat-2026-05-06T12:00",
    EventType: "service.heartbeat",
    Payload:   []byte(`{}`),
    Source:    "order-service",
})
```

Prefer `Add` whenever you *do* have a business transaction — that's the whole point of the outbox pattern.

### Relaying events to a broker

```go
published, err := m.Outbox.PublishBatch(ctx, 100,
    func(ctx context.Context, msg *message.Message) error {
        return kafkaProducer.Send(ctx, msg.EventType, msg.Payload)
    },
)
```

Same `FOR UPDATE SKIP LOCKED` semantics — many relay workers can run in parallel.

---

## Cleanup

The library does **not** start its own goroutines. Wire these into your scheduler.

### One-shot

```go
purged, _   := m.Cleanup.PurgeExpiredDedup(ctx)   // delete dedup rows past expire_at
recoveredIn, _  := m.Cleanup.RecoverStuckInbox(ctx)  // reset CLAIMED rows older than ClaimTimeout
recoveredOut, _ := m.Cleanup.RecoverStuckOutbox(ctx)
log.Printf("cleanup: purged=%d, recoveredInbox=%d, recoveredOutbox=%d",
    purged, recoveredIn, recoveredOut)
```

### Periodic (ticker)

```go
go func() {
    t := time.NewTicker(5 * time.Minute)
    defer t.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-t.C:
            m.Cleanup.PurgeExpiredDedup(ctx)
            m.Cleanup.RecoverStuckInbox(ctx)
            m.Cleanup.RecoverStuckOutbox(ctx)
        }
    }
}()
```

### Kubernetes CronJob (alternative)

If you'd rather not run an in-process goroutine, build a tiny binary that calls the three methods once and exits, and schedule it as a `CronJob`. The library is stateless across calls.

If a worker crashes mid-process, its rows stay in `CLAIMED` status. `RecoverStuckInbox` / `RecoverStuckOutbox` flip them back to `RETRY` so another worker can pick them up.

---

## Configuration reference

Defined in [config.go](config.go).

| Field               | Default                              | Purpose                                                  |
| ------------------- | ------------------------------------ | -------------------------------------------------------- |
| `TablePrefix`       | `""`                                 | Prefix for all tables (e.g. `"order"` → `order_message_hot`). |
| `DefaultMaxRetries` | `5`                                  | Used when `InboxMessage.MaxRetries` / `OutboxEvent.MaxRetries` is 0. |
| `DefaultBackoff`    | `LinearBackoff{BaseDelay: 1s}`       | Strategy used when no `WithBackoff` option is passed.    |
| `ClaimTimeout`      | `5m`                                 | Stuck-claim recovery threshold.                          |
| `DedupTTL`          | `24h`                                | How long dedup rows live before being purged.            |
| `WorkerID`          | `"default"`                          | Written into `claimed_by`. Useful for debugging.         |

### Options

```go
m := message.New(db, cfg,
    message.WithBackoff(message.ExponentialBackoff{BaseDelay: time.Second, MaxDelay: time.Minute}),
    message.WithMetrics(myMetrics),
)
```

---

## Backoff strategies

Three built-in strategies plus a `Backoff` interface for custom ones (see the [API summary](#built-in-backoff-strategies) for the formulas). Defined in [backoff.go](backoff.go).

```go
type Backoff interface {
    Next(retryCount int) time.Duration
}
```

### Example: using a built-in strategy

```go
m := message.New(db, cfg,
    message.WithBackoff(message.ExponentialBackoff{
        BaseDelay: 500 * time.Millisecond,
        MaxDelay:  2 * time.Minute,
    }),
)
```

### Example: custom strategy (jittered exponential)

```go
type JitteredBackoff struct {
    Base, Max time.Duration
    Rand      *rand.Rand
}

func (b JitteredBackoff) Next(retryCount int) time.Duration {
    if retryCount < 1 {
        retryCount = 1
    }
    d := time.Duration(float64(b.Base) * math.Pow(2, float64(retryCount-1)))
    if b.Max > 0 && d > b.Max {
        d = b.Max
    }
    // Add up to 25% jitter to avoid thundering-herd retries.
    jitter := time.Duration(b.Rand.Int63n(int64(d) / 4))
    return d + jitter
}

m := message.New(db, cfg,
    message.WithBackoff(JitteredBackoff{
        Base: time.Second,
        Max:  time.Minute,
        Rand: rand.New(rand.NewSource(time.Now().UnixNano())),
    }),
)
```

---

## Metrics

Implement [`message.Metrics`](metrics.go) to plug in Prometheus, OpenTelemetry, etc.

```go
type Metrics interface {
    InboxProcessed(consumerGroup, eventType string)
    InboxRetry(consumerGroup, eventType string)
    InboxFailed(consumerGroup, eventType string)
    OutboxPublished(eventType string)
    OutboxRetry(eventType string)
    OutboxFailed(eventType string)
}
```

The default is `NoopMetrics{}`. Pass a custom one via `WithMetrics`.

### Example: Prometheus adapter

```go
type PromMetrics struct {
    inboxProcessed *prometheus.CounterVec
    inboxRetry     *prometheus.CounterVec
    inboxFailed    *prometheus.CounterVec
    outboxPub      *prometheus.CounterVec
    outboxRetry    *prometheus.CounterVec
    outboxFailed   *prometheus.CounterVec
}

func (p *PromMetrics) InboxProcessed(group, et string) { p.inboxProcessed.WithLabelValues(group, et).Inc() }
func (p *PromMetrics) InboxRetry(group, et string)     { p.inboxRetry.WithLabelValues(group, et).Inc() }
func (p *PromMetrics) InboxFailed(group, et string)    { p.inboxFailed.WithLabelValues(group, et).Inc() }
func (p *PromMetrics) OutboxPublished(et string)       { p.outboxPub.WithLabelValues(et).Inc() }
func (p *PromMetrics) OutboxRetry(et string)            { p.outboxRetry.WithLabelValues(et).Inc() }
func (p *PromMetrics) OutboxFailed(et string)           { p.outboxFailed.WithLabelValues(et).Inc() }

m := message.New(db, cfg, message.WithMetrics(promMetrics))
```

### Example: structured-log adapter

```go
type LogMetrics struct{ Logger *slog.Logger }

func (l LogMetrics) InboxProcessed(g, et string) { l.Logger.Info("inbox.processed", "group", g, "type", et) }
func (l LogMetrics) InboxRetry(g, et string)     { l.Logger.Warn("inbox.retry",     "group", g, "type", et) }
func (l LogMetrics) InboxFailed(g, et string)    { l.Logger.Error("inbox.failed",   "group", g, "type", et) }
func (l LogMetrics) OutboxPublished(et string)   { l.Logger.Info("outbox.published","type", et) }
func (l LogMetrics) OutboxRetry(et string)       { l.Logger.Warn("outbox.retry",    "type", et) }
func (l LogMetrics) OutboxFailed(et string)      { l.Logger.Error("outbox.failed",  "type", et) }
```

---

## Errors

Three sentinel errors live in [errors.go](errors.go) — see the [API summary](#sentinel-errors) for the full list. Use `errors.Is(err, message.ErrXxx)` to check.

```go
if err := m.Inbox.Receive(ctx, msg); err != nil {
    if errors.Is(err, message.ErrDuplicate) {
        // unreachable in practice — Receive swallows this and returns nil
    }
    return err
}
```

---

## Database schema

Three tables are created (with the configured prefix):

### `<prefix>_message_dedup`
Inbox-only. Tracks `(consumer_group, message_id)` to deduplicate incoming messages. Rows are purged based on `expire_at` (driven by `Config.DedupTTL`).

### `<prefix>_message_hot`
Working set for both directions. Holds `PENDING` / `CLAIMED` / `RETRY` / `FAILED` / `DONE` rows. Indexed for fast claim queries.

- Partial unique index `(direction, consumer_group, message_id) WHERE direction = 'INBOX'`
- Partial unique index `(direction, event_id) WHERE direction = 'OUTBOX'`
- Claim index `(direction, status, next_retry_at) WHERE status IN ('PENDING','RETRY')`

### `<prefix>_message_archive`
Long-term history of every successfully processed (`PROCESSED` / `PUBLISHED`) or permanently failed (`FAILED`) message. Rows are moved here atomically when terminal status is reached.

Full DDL lives in [schema.go](schema.go).

---

## Lifecycle

```
                 INBOX                                   OUTBOX
   ┌─────────────────────────────┐         ┌──────────────────────────────┐
   │  Receive() ─► dedup + hot   │         │  Add(tx) ─► hot (in caller's │
   │                             │         │             transaction)     │
   └────────────┬────────────────┘         └──────────────┬───────────────┘
                │                                         │
                ▼                                         ▼
   ┌─────────────────────────────┐         ┌──────────────────────────────┐
   │  ProcessBatch()             │         │  PublishBatch()              │
   │  ─ claim FOR UPDATE SKIP    │         │  ─ claim FOR UPDATE SKIP     │
   │  ─ run handler              │         │  ─ run publisher             │
   └────┬────────────────────┬───┘         └──────┬─────────────────┬─────┘
        │ ok                 │ err               │ ok              │ err
        ▼                    ▼                   ▼                 ▼
    archive(PROCESSED)   markRetry/         archive(PUBLISHED)  markRetry/
                         markFailed                              markFailed
                              │                                        │
                              ▼                                        ▼
                     (retries until                        (retries until
                      max → archive(FAILED))                max → archive(FAILED))
```

In parallel:

```
Cleanup.PurgeExpiredDedup    — deletes expired dedup rows
Cleanup.RecoverStuckInbox    — flips stale CLAIMED rows back to RETRY
Cleanup.RecoverStuckOutbox
```

---

## Testing

The test suite uses [testcontainers-go](https://github.com/testcontainers/testcontainers-go) to spin up a real PostgreSQL 16 container. Docker must be running.

```bash
go test ./...
```

See [testutil_test.go](testutil_test.go) for the harness.

---

## License

Released under the MIT License — see [LICENSE](LICENSE).
