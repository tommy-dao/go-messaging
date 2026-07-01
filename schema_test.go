package message

import (
	"strings"
	"testing"
)

func TestSchema_WithName(t *testing.T) {
	stmts, err := Schema("rwa")
	if err != nil {
		t.Fatalf("Schema failed: %v", err)
	}
	if len(stmts) == 0 {
		t.Fatal("Schema returned no statements")
	}

	for i, stmt := range stmts {
		if !strings.Contains(stmt, "message_rwa_") {
			t.Errorf("statement %d does not contain 'message_rwa_': %s", i, stmt[:min(80, len(stmt))])
		}
	}
}

func TestSchema_EmptyName(t *testing.T) {
	stmts, err := Schema("")
	if err != nil {
		t.Fatalf("Schema failed: %v", err)
	}

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
	stmts, err := Schema("test")
	if err != nil {
		t.Fatalf("Schema failed: %v", err)
	}
	joined := strings.Join(stmts, "\n")

	tables := []string{"message_test_dedup", "message_test_hot", "message_test_archive"}
	for _, tbl := range tables {
		if !strings.Contains(joined, "CREATE TABLE IF NOT EXISTS "+tbl) {
			t.Errorf("Schema missing table: %s", tbl)
		}
	}
}

func TestSchema_ContainsIndexes(t *testing.T) {
	stmts, err := Schema("test")
	if err != nil {
		t.Fatalf("Schema failed: %v", err)
	}
	joined := strings.Join(stmts, "\n")

	indexes := []string{
		"idx_message_test_hot_claim",
		"idx_message_test_hot_failed",
		"idx_message_test_archive_message_id",
	}
	for _, idx := range indexes {
		if !strings.Contains(joined, idx) {
			t.Errorf("Schema missing index: %s", idx)
		}
	}
}

func TestAssertValidName(t *testing.T) {
	cases := []struct {
		name    string
		wantErr bool
	}{
		{"", false},
		{"asset", false},
		{"asset_v2", false},
		{"has space", true},
		{"has-dash", true},
		{strings.Repeat("a", 48), true},
		{strings.Repeat("a", 47), false},
	}
	for _, c := range cases {
		err := assertValidName(c.name)
		if c.wantErr && err == nil {
			t.Errorf("assertValidName(%q) = nil, want error", c.name)
		}
		if !c.wantErr && err != nil {
			t.Errorf("assertValidName(%q) = %v, want nil", c.name, err)
		}
	}
}
