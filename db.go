package message

import (
	"fmt"
	"regexp"
)

var nameRegex = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// maxNameLen keeps "message_<name>_archive" (the longest generated identifier,
// 16 fixed chars) under Postgres' 63-byte identifier limit.
const maxNameLen = 47

// assertValidName validates Config.Name before it is interpolated into DDL/DML.
// An empty name is valid and selects the unscoped "message_*" tables.
func assertValidName(name string) error {
	if name == "" {
		return nil
	}
	if !nameRegex.MatchString(name) {
		return fmt.Errorf("message: invalid name %q: must match %s", name, nameRegex.String())
	}
	if len(name) > maxNameLen {
		return fmt.Errorf("message: invalid name %q: exceeds %d chars", name, maxNameLen)
	}
	return nil
}

// tableName returns the name-scoped table identifier, e.g. tableName("asset", "hot") -> "message_asset_hot".
// An empty name yields the unscoped table, e.g. tableName("", "hot") -> "message_hot".
func tableName(name, suffix string) string {
	if name == "" {
		return "message_" + suffix
	}
	return "message_" + name + "_" + suffix
}
