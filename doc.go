// Package message provides an Inbox/Outbox messaging library for PostgreSQL.
//
// Inbox handles idempotent message receiving with deduplication, retry, and archival.
// Outbox handles transactional event publishing with relay, retry, and archival.
//
// Both share common database tables (message_hot, message_archive) differentiated
// by a direction column. Table names are prefixed via Config.TablePrefix for
// multi-service isolation.
//
// Key features:
//   - Idempotent inbox with dedup table
//   - Transactional outbox (Add within caller's pgx.Tx)
//   - Configurable retry backoff (fixed, linear, exponential)
//   - Worker scaling via FOR UPDATE SKIP LOCKED
//   - No background goroutines — caller controls scheduling
package message
