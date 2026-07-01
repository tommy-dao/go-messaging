package message

// maxErrorLen caps last_error length — protects against unbounded error
// bodies (and any PII they might carry) accumulating in the hot/archive tables.
const maxErrorLen = 1000

// toErrorMessage renders err as a bounded string suitable for last_error.
func toErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	return truncateError(err.Error())
}

func truncateError(s string) string {
	if len(s) <= maxErrorLen {
		return s
	}
	return s[:maxErrorLen]
}
