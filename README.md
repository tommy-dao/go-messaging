# go-message

[![Go Reference](https://pkg.go.dev/badge/github.com/tommy-dao/go-message.svg)](https://pkg.go.dev/github.com/tommy-dao/go-message)
[![Go Version](https://img.shields.io/badge/go-1.25%2B-blue)](https://go.dev/dl/)

A lightweight **Inbox / Outbox** messaging library for PostgreSQL, written in Go.

It provides:

- **Inbox** — idempotent message receiving with deduplication, retry, and archival.
- **Outbox** — transactional event publishing with relay, retry, and archival.
- **Cleanup** — dedup expiry and stuck-claim recovery.

Both Inbox and Outbox share the same set of database tables (`message_hot`, `message_archive`), differentiated by a `direction` column. Table names can be prefixed for multi-service isolation.

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
| [`m.Cleanup.PurgeExpiredDedup(ctx)`](#purge-expired-dedup)   | [cleanup.go](cleanup.go)   | Delete dedup rows past `expire_at`.                           |
| [`m.Cleanup.RecoverStuckInbox(ctx)`](#recover-stuck-claims)  | [cleanup.go](cleanup.go)   | Flip stale `CLAIMED` inbox rows back to `RETRY`.              |
| [`m.Cleanup.RecoverStuckOutbox(ctx)`](#recover-stuck-claims) | [cleanup.go](cleanup.go)   | Same for outbox.                                              |

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

### 3. A quick round-trip

Receive an inbound message, process it, add an outbound event, and publish it — the four calls that anchor the whole library:

```go
ctx := context.Background()

// 1. Inbound: idempotently store
m.Inbox.Receive(ctx, message.InboxMessage{
    ConsumerGroup: "billing-service",
    MessageID:     "msg-1",
    EventType:     "order.created",
    Payload:       []byte(`{"order_id":1}`),
})

// 2. Process the inbox (handler returns nil → archive PROCESSED)
m.Inbox.ProcessBatch(ctx, "billing-service", 10,
    func(ctx context.Context, msg *message.Message) error {
        log.Printf("got %s: %s", msg.EventType, msg.Payload)
        return nil
    })

// 3. Outbound: add an event inside your business transaction
tx, _ := db.BeginTx(ctx, nil)
m.Outbox.Add(ctx, tx, message.OutboxEvent{
    EventID:   "evt-1",
    EventType: "order.confirmed",
    Payload:   []byte(`{"order_id":1}`),
})
tx.Commit()

// 4. Publish (publisher returns nil → archive PUBLISHED)
m.Outbox.PublishBatch(ctx, 10,
    func(ctx context.Context, msg *message.Message) error {
        log.Printf("publishing %s", msg.EventType)
        return nil
    })
```

For production patterns (worker loops, Kafka integration, error handling, scaling) jump to [Inbox usage](#inbox-usage), [Outbox usage](#outbox-usage), and [End-to-end Kafka example](#end-to-end-kafka-example).

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

#### Single call

```go
processed, err := m.Inbox.ProcessBatch(ctx, "order-service", 50,
    func(ctx context.Context, msg *message.Message) error {
        return doBusinessLogic(ctx, msg.Payload) // your own function
    },
)
```

#### Worker loop (typical production setup)

Usually you want `ProcessBatch` running continuously. Drive it with a ticker:

```go
import (
    "context"
    "encoding/json"
    "errors"
    "log"
    "time"

    message "github.com/tommy-dao/go-message"
)

type OrderCreated struct {
    OrderID int64   `json:"order_id"`
    Total   float64 `json:"total"`
}

func startInboxWorker(ctx context.Context, m *message.Messaging, group string) {
    handler := func(ctx context.Context, msg *message.Message) error {
        switch msg.EventType {
        case "order.created":
            var evt OrderCreated
            if err := json.Unmarshal(msg.Payload, &evt); err != nil {
                // Bad payload — return error so it retries; will eventually hit
                // MaxRetries and be archived as FAILED for human inspection.
                return errors.Join(errors.New("bad payload"), err)
            }
            return chargeCustomer(ctx, evt.OrderID, evt.Total) // your business logic

        default:
            log.Printf("inbox: unknown event %q, skipping", msg.EventType)
            return nil // no-op → archive as PROCESSED
        }
    }

    go func() {
        t := time.NewTicker(time.Second)
        defer t.Stop()
        for {
            select {
            case <-ctx.Done():
                return
            case <-t.C:
                if _, err := m.Inbox.ProcessBatch(ctx, group, 50, handler); err != nil {
                    log.Printf("inbox: process batch: %v", err)
                }
            }
        }
    }()
}
```

#### Handler return-value semantics

| Return       | Effect                                                                          |
| ------------ | ------------------------------------------------------------------------------- |
| `nil`        | Success → row archived as `PROCESSED`; never re-delivered.                      |
| `error`      | Failure → row flipped to `RETRY` with backoff; retried on a future tick.        |
| `error` after `MaxRetries` retries | Archived as `FAILED` (so `MaxRetries=5` yields 6 total attempts: the initial try + 5 retries). `Metrics.InboxFailed` is counted; no more retries. |
| Handler panics | Goroutine dies — row stays `CLAIMED` until `Cleanup.RecoverStuckInbox` resets it. **Wrap in `recover()` if you don't trust the handler.** |

The handler **must be idempotent**: the same message can be delivered more than once if a worker crashes mid-handler (after the row was claimed but before it was archived).

#### Scaling

The claim uses `FOR UPDATE SKIP LOCKED`, so multiple goroutines (or pods) can call `ProcessBatch` concurrently — each gets a disjoint set of rows. Scale by running the worker on multiple replicas, or starting N goroutines in the same process. The database serializes claim transactions, so workers never see the same row.

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
        return kafkaProducer.Send(ctx, msg.EventType, msg.Payload) // your own client
    },
)
```

Same `FOR UPDATE SKIP LOCKED` semantics — many relay workers can run in parallel. Returning an error from the publisher closure flips the row to `RETRY` with the configured backoff; the message stays in `*_message_hot` until it succeeds or hits `MaxRetries`.

For a complete worked example, see [End-to-end Kafka example](#end-to-end-kafka-example) below.

---

## End-to-end Kafka example

A complete producer + consumer setup using [`segmentio/kafka-go`](https://github.com/segmentio/kafka-go). The same pattern works for any broker (NATS, RabbitMQ, SQS, …) — only the publisher/reader call changes.

### Producer side — relaying outbox events to Kafka

```go
import (
    "context"
    "log"
    "time"

    "github.com/segmentio/kafka-go"
    message "github.com/tommy-dao/go-message"
)

func startRelay(ctx context.Context, m *message.Messaging) {
    writer := &kafka.Writer{
        Addr:         kafka.TCP("kafka:9092"),
        Balancer:     &kafka.Hash{},        // partition by key → per-entity ordering
        RequiredAcks: kafka.RequireAll,     // strongest durability
    }
    defer writer.Close()

    publisher := func(ctx context.Context, msg *message.Message) error {
        headers := make([]kafka.Header, 0, len(msg.Headers))
        for k, v := range msg.Headers {
            headers = append(headers, kafka.Header{Key: k, Value: []byte(v)})
        }
        return writer.WriteMessages(ctx, kafka.Message{
            Topic:   topicFor(msg.EventType),   // your mapping, e.g. "order.created" → "orders"
            Key:     []byte(msg.EventID),       // same entity → same partition
            Value:   msg.Payload,
            Headers: headers,
        })
    }

    go func() {
        t := time.NewTicker(time.Second)
        defer t.Stop()
        for {
            select {
            case <-ctx.Done():
                return
            case <-t.C:
                if _, err := m.Outbox.PublishBatch(ctx, 100, publisher); err != nil {
                    log.Printf("publish batch: %v", err)
                }
            }
        }
    }()
}
```

Key design choices:

- **`EventID` as Kafka key**: events for the same business entity hash to the same partition, preserving per-entity ordering downstream. (Inbox messages use `MessageID` as the dedup key; outbox events use `EventID` — pick the field that's populated for your direction.)
- **`Headers` propagation**: trace IDs, correlation IDs, schema versions — anything you set on `OutboxEvent.Headers` flows through unchanged.
- **`RequiredAcks: RequireAll`** + at-least-once outbox = downstream consumers must dedupe by `EventID` (the consumer side below does this automatically via `Inbox`).
- **Returning error = retry**: broker outage doesn't lose messages — they queue in the hot table and drain when Kafka recovers.

### Consumer side — receiving from Kafka into the inbox

```go
reader := kafka.NewReader(kafka.ReaderConfig{
    Brokers: []string{"kafka:9092"},
    Topic:   "orders",
    GroupID: "billing-service",
})
defer reader.Close()

for {
    msg, err := reader.ReadMessage(ctx)
    if err != nil {
        return err
    }

    headers := make(map[string]string, len(msg.Headers))
    for _, h := range msg.Headers {
        headers[h.Key] = string(h.Value)
    }

    err = m.Inbox.Receive(ctx, message.InboxMessage{
        ConsumerGroup: "billing-service",
        MessageID:     string(msg.Key),     // dedup key — redeliveries no-op
        EventType:     msg.Topic,
        Payload:       msg.Value,
        Headers:       headers,
        Source:        "kafka",
    })
    if err != nil {
        return err
    }
}
```

The Kafka consumer **only stores** into the inbox — actual processing runs separately via [`m.Inbox.ProcessBatch`](#processing-a-batch) on its own ticker. This split is what makes the pattern robust:

- Crash mid-processing → message stays in `*_message_hot`, picked up on restart.
- Kafka rebalance → same offset re-delivered → dedup table no-ops the duplicate.
- Scale consumer goroutine and processor goroutine independently.
- Retry processing without re-reading from Kafka.

---

## Cleanup

The library does **not** start its own goroutines. If a worker crashes mid-process, its rows stay in `CLAIMED` status — `RecoverStuckInbox` / `RecoverStuckOutbox` flip them back to `RETRY` so another worker can pick them up. Dedup rows accumulate over time and need pruning. Wire these three methods into your own scheduler.

### Purge expired dedup

Deletes dedup rows whose `expire_at` is past (driven by `Config.DedupTTL`). Returns the number of rows deleted.

```go
purged, _ := m.Cleanup.PurgeExpiredDedup(ctx)
log.Printf("cleanup: purged %d dedup rows", purged)
```

### Recover stuck claims

Resets rows that have been `CLAIMED` longer than `Config.ClaimTimeout`. Returns the count.

```go
inbox, _  := m.Cleanup.RecoverStuckInbox(ctx)
outbox, _ := m.Cleanup.RecoverStuckOutbox(ctx)
log.Printf("cleanup: recovered inbox=%d outbox=%d", inbox, outbox)
```

### Running on a schedule

Pick the pattern that fits your deployment.

#### In-process ticker

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

#### Kubernetes CronJob (out-of-process)

If you'd rather not run an in-process goroutine, build a tiny binary that calls the three methods once and exits, and schedule it as a `CronJob`. The library is stateless across calls — safe to run from anywhere with database access.

---

## Configuration reference

`Config` fields (defined in [config.go](config.go)):

| Field               | Default                              | Purpose                                                              |
| ------------------- | ------------------------------------ | -------------------------------------------------------------------- |
| `TablePrefix`       | `""`                                 | Prefix for all tables (e.g. `"order"` → `order_message_hot`).        |
| `DefaultMaxRetries` | `5`                                  | Used when `InboxMessage.MaxRetries` / `OutboxEvent.MaxRetries` is 0. |
| `DefaultBackoff`    | `LinearBackoff{BaseDelay: 1s}`       | Strategy used when no `WithBackoff` option is passed.                |
| `ClaimTimeout`      | `5m`                                 | Stuck-claim recovery threshold.                                      |
| `DedupTTL`          | `24h`                                | How long dedup rows live before being purged.                        |
| `WorkerID`          | `"default"`                          | Written into `claimed_by`. Useful for debugging.                     |

To override defaults at construction, see [`WithBackoff`](#example-using-a-built-in-strategy) and [`WithMetrics`](#example-prometheus-adapter).

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
               m.Inbox                                 m.Outbox
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

In parallel, on the `m.Cleanup` side:

```
m.Cleanup.PurgeExpiredDedup    — deletes expired dedup rows
m.Cleanup.RecoverStuckInbox    — flips stale CLAIMED rows back to RETRY
m.Cleanup.RecoverStuckOutbox
```

---

## Testing

### Running the library's own tests

The test suite uses [testcontainers-go](https://github.com/testcontainers/testcontainers-go) to spin up a real PostgreSQL 16 container. Docker must be running.

```bash
go test ./...
```

See [testutil_test.go](testutil_test.go) for the harness.

### Testing your own code that uses go-message

Because `New(db, ...)` takes a plain `*sql.DB`, two patterns work:

- **Integration test** — spin up a real Postgres (testcontainers, in-memory `pg`-flavored mock, or a shared CI database) and exercise `Receive` / `ProcessBatch` / `Add` / `PublishBatch` end-to-end. Recommended whenever the SQL semantics matter (claim races, dedup, archival).
- **Unit test the closure directly** — your `Handler` and `Publisher` are plain `func(ctx, *message.Message) error`. Call them with a synthesized `*message.Message` and assert behavior; don't even involve the library. Faster, no DB needed.

```go
// Unit test pattern: the handler is the unit under test.
func TestChargeHandler(t *testing.T) {
    h := func(ctx context.Context, msg *message.Message) error {
        return chargeCustomer(ctx, parseOrderID(msg.Payload))
    }

    err := h(context.Background(), &message.Message{
        EventType: "order.created",
        Payload:   []byte(`{"order_id":42}`),
    })
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
}
```

---

## License

Released under the MIT License — see [LICENSE](LICENSE).
