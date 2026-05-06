package message

import (
	"strings"
	"testing"
)

func TestSchema_WithPrefix(t *testing.T) {
	stmts := Schema("rwa")

	if len(stmts) == 0 {
		t.Fatal("Schema returned no statements")
	}

	// Check that all statements contain the prefix
	for i, stmt := range stmts {
		if !strings.Contains(stmt, "rwa_message_") {
			t.Errorf("statement %d does not contain prefix 'rwa_message_': %s", i, stmt[:80])
		}
	}
}

func TestSchema_EmptyPrefix(t *testing.T) {
	stmts := Schema("")

	for i, stmt := range stmts {
		if strings.Contains(stmt, "_message_dedup") && !strings.Contains(stmt, "message_dedup") {
			t.Errorf("statement %d should use 'message_dedup' without prefix: %s", i, stmt[:80])
		}
	}

	// Verify dedup table exists
	found := false
	for _, stmt := range stmts {
		if strings.Contains(stmt, "CREATE TABLE IF NOT EXISTS message_dedup") {
			found = true
			break
		}
	}
	if !found {
		t.Error("Schema should contain message_dedup table creation")
	}
}

func TestSchema_ContainsAllTables(t *testing.T) {
	stmts := Schema("test")
	joined := strings.Join(stmts, "\n")

	tables := []string{"test_message_dedup", "test_message_hot", "test_message_archive"}
	for _, tbl := range tables {
		if !strings.Contains(joined, "CREATE TABLE IF NOT EXISTS "+tbl) {
			t.Errorf("Schema missing table: %s", tbl)
		}
	}
}

func TestSchema_ContainsIndexes(t *testing.T) {
	stmts := Schema("test")
	joined := strings.Join(stmts, "\n")

	indexes := []string{
		"uq_test_message_hot_inbox",
		"uq_test_message_hot_outbox",
		"idx_test_message_hot_claim",
	}
	for _, idx := range indexes {
		if !strings.Contains(joined, idx) {
			t.Errorf("Schema missing index: %s", idx)
		}
	}
}
