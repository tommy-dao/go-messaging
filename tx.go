package message

import (
	"context"
	"database/sql"
)

// DBTX is the common interface between *sql.DB and *sql.Tx.
type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Tx is a type alias for *sql.Tx, used by Outbox.Add() to participate
// in the caller's business transaction.
type Tx = *sql.Tx
