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

	// PgError with SQLSTATE class 08 (connection exception) or specific class 57
	// (operator intervention) codes that indicate the server is shutting down or
	// unavailable — NOT query-level cancellations like 57014.
	//
	// Class 08 — Connection Exception (all codes):
	//   https://www.postgresql.org/docs/current/errcodes-appendix.html#ERRCODES-TABLE
	//
	// Class 57 — Operator Intervention (selective):
	//   57000  operator_intervention (generic)
	//   57P01  admin_shutdown        — server is shutting down
	//   57P02  crash_shutdown        — server crashed
	//   57P03  cannot_connect_now    — server is starting up
	//   57P04  database_dropped      — database no longer exists
	//
	// Excluded from class 57:
	//   57014  query_canceled — fired by statement_timeout, pg_cancel_backend(),
	//          or client context cancellation. This is a normal query-level event,
	//          not a sign that the database is unreachable.
	//          https://www.postgresql.org/docs/current/errcodes-appendix.html
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		code := pgErr.Code
		if strings.HasPrefix(code, "08") {
			return true
		}
		switch code {
		case "57000", "57P01", "57P02", "57P03", "57P04":
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
