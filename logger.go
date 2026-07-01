package message

import "context"

// LogLevel represents the severity of a log event.
type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
)

func (l LogLevel) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// Logger receives structured log events from the messaging library.
// msg is the Message being operated on — may be partial for store-level events
// (e.g. only ID is set when archiving). Callers extract whatever fields they need.
//
// By default NoopLogger is used — nothing is logged.
// Override by passing WithLogger to New().
type Logger interface {
	Log(ctx context.Context, level LogLevel, event string, msg *Message)
}

// NoopLogger discards all log entries. This is the default.
type NoopLogger struct{}

func (NoopLogger) Log(_ context.Context, _ LogLevel, _ string, _ *Message) {}

// LoggerFunc adapts a plain function to the Logger interface.
//
//	message.WithLogger(message.LoggerFunc(func(ctx context.Context, level message.LogLevel, event string, msg *message.Message) {
//	    slog.InfoContext(ctx, event, "message_id", msg.MessageID, "topic", msg.Topic)
//	}))
type LoggerFunc func(ctx context.Context, level LogLevel, event string, msg *Message)

func (f LoggerFunc) Log(ctx context.Context, level LogLevel, event string, msg *Message) {
	f(ctx, level, event, msg)
}
