package pg

import (
	"errors"
	"net"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// IsConnectionError returns true if err indicates a connection-level problem
// (as opposed to a query-level error like a missing relation or syntax error).
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}

	// pgconn.ConnectError means the driver couldn't establish a connection.
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) {
		return true
	}

	// PgError with SQLSTATE class 08 (connection exception) or 57 (operator intervention).
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		code := pgErr.Code
		if strings.HasPrefix(code, "08") || strings.HasPrefix(code, "57") {
			return true
		}
		// Any other PgError is a query-level error.
		return false
	}

	// Network-level errors.
	var netOpErr *net.OpError
	if errors.As(err, &netOpErr) {
		return true
	}
	var syscallErr *os.SyscallError
	if errors.As(err, &syscallErr) {
		return true
	}

	// Fallback: check error message for common connection failure strings.
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "no such host") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "connection timed out")
}
