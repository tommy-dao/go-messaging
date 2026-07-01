package message

import "errors"

var (
	// ErrDuplicate indicates a message with the same message_id already exists in dedup.
	ErrDuplicate = errors.New("message: duplicate")

	// ErrNotFound indicates the requested message was not found.
	ErrNotFound = errors.New("message: not found")

	// ErrNoCipher indicates an encrypted row was read but no cipher is configured to decrypt it.
	ErrNoCipher = errors.New("message: no cipher configured")
)
