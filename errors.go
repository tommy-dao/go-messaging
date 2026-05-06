package message

import "errors"

var (
	// ErrDuplicate indicates a message with the same key already exists.
	ErrDuplicate = errors.New("message: duplicate")

	// ErrNotFound indicates the requested message was not found.
	ErrNotFound = errors.New("message: not found")
)
