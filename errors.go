package message

import "errors"

var (
	// ErrDuplicate indicates a message with the same key already exists.
	ErrDuplicate = errors.New("message: duplicate")

	// ErrMaxRetry indicates the message has exceeded its maximum retry count.
	ErrMaxRetry = errors.New("message: max retries exceeded")

	// ErrNotFound indicates the requested message was not found.
	ErrNotFound = errors.New("message: not found")
)
