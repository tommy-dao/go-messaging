package message

import (
	"context"
	"database/sql"
	"fmt"
)

// Migrate applies all messaging table DDL statements idempotently.
func Migrate(ctx context.Context, db *sql.DB, prefix string) error {
	for i, stmt := range Schema(prefix) {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("message: migrate statement %d: %w", i, err)
		}
	}
	return nil
}
