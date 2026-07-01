// Package message provides a transactional inbox/outbox messaging library for PostgreSQL.
//
// Messages flow through three tables, scoped by Config.Name (e.g. Name="asset" -> message_asset_hot):
//   - message_<name>_dedup:   Receive()-side idempotency (message_id is the global dedup key)
//   - message_<name>_hot:     the live queue — PENDING/PROCESSING/RETRY, and FAILED rows
//     acting as a dead-letter queue until RetryFailed/RetryOne re-queues them
//     or ArchiveExhausted archives them after Config.FailedRetention
//   - message_<name>_archive: append-only history of PROCESSED and archived-FAILED rows
//
// Receive and Enqueue are the two entry points; both write into the same hot
// table and are drained the same way by ProcessBatch, routed to handlers by
// Message.Topic:
//   - Receive: idempotent via the dedup table (duplicate message_id -> isNew=false, no-op)
//   - Enqueue: idempotent via the hot table's message_id primary key
//
// Key features:
//   - Transactional Receive/Enqueue (participates in caller's *sql.Tx, or auto-tx when nil)
//   - Configurable retry backoff (fixed, linear, exponential)
//   - Worker scaling via FOR UPDATE SKIP LOCKED, with owner-guarded terminal
//     transitions so a stale worker can't overwrite a row reclaimed by RecoverStuck
//   - Optional at-rest encryption (WithCiphers) with versioned ciphertext for key
//     rotation without a schema change; a row that fails to decrypt is
//     dead-lettered individually instead of failing the whole batch
//   - Admin/query API: DeadLetter, RetryFailed, RetryAllFailed, RetryOne,
//     GetMetrics, ListFailed/ListPending, FindByMessageID, ListArchived,
//     IsDuplicate, RemoveDedup
//   - No required background goroutines — call ProcessBatch/RunAll yourself,
//     or opt into the built-in interval-mode Runner
//
// # Logging
//
// By default nothing is logged. Pass WithLogger to enable structured logging:
//
//	m, _ := message.New(db, cfg, message.WithLogger(message.LoggerFunc(
//	    func(ctx context.Context, level message.LogLevel, event string, msg *message.Message) {
//	        slog.InfoContext(ctx, event,
//	            "level",      level.String(),
//	            "message_id", msg.MessageID,
//	            "topic",      msg.Topic,
//	            "status",     string(msg.Status),
//	        )
//	    },
//	)))
//
// Events emitted (with their default level):
//
//	enqueue                   INFO   — message accepted
//	enqueue.error             ERROR  — insert failed
//	receive                   INFO   — message accepted (new)
//	receive.duplicate         DEBUG  — deduplicated, no-op
//	receive.error             ERROR  — insert failed
//	processed                 INFO   — handler succeeded, message archived
//	process.retry             WARN   — handler failed, scheduled for retry
//	process.failed            ERROR  — max retries exceeded, FAILED (stays in hot as DLQ)
//	process.unhandled         ERROR  — no handler registered for topic, dead-lettered
//	process.decrypt_failed    ERROR  — row could not be decrypted, dead-lettered
package message
