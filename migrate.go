package message

import (
	"context"
	"database/sql"
	"fmt"
)

// Migrate applies all messaging table DDL statements idempotently for the
// given name (see Schema).
func Migrate(ctx context.Context, db *sql.DB, name string) error {
	stmts, err := Schema(name)
	if err != nil {
		return err
	}
	for i, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("message: migrate statement %d: %w", i, err)
		}
	}
	return nil
}

// DropSchema drops all messaging tables for the given name. Intended for
// test teardown / decommissioning — this is a destructive, irreversible
// operation.
func DropSchema(ctx context.Context, db *sql.DB, name string) error {
	stmts, err := dropSchema(name)
	if err != nil {
		return err
	}
	for i, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("message: drop schema statement %d: %w", i, err)
		}
	}
	return nil
}
