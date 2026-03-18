package pg

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsConnectionError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		// --- nil / generic ---
		{name: "nil", err: nil, want: false},
		{name: "generic error", err: errors.New("something failed"), want: false},
		{name: "context.Canceled", err: context.Canceled, want: false},
		{name: "context.DeadlineExceeded", err: context.DeadlineExceeded, want: false},

		// --- pgconn.ConnectError ---
		{
			name: "ConnectError",
			err:  &pgconn.ConnectError{Config: nil},
			want: true,
		},
		{
			name: "wrapped ConnectError",
			err:  fmt.Errorf("outer: %w", &pgconn.ConnectError{}),
			want: true,
		},
		{
			name: "deeply wrapped ConnectError",
			err:  fmt.Errorf("layer1: %w", fmt.Errorf("layer2: %w", &pgconn.ConnectError{})),
			want: true,
		},

		// --- PgError class 08 (connection exception) ---
		{name: "PgError 08000", err: &pgconn.PgError{Code: "08000"}, want: true},
		{name: "PgError 08001", err: &pgconn.PgError{Code: "08001"}, want: true},
		{name: "PgError 08003", err: &pgconn.PgError{Code: "08003"}, want: true},
		{name: "PgError 08004", err: &pgconn.PgError{Code: "08004"}, want: true},
		{name: "PgError 08006", err: &pgconn.PgError{Code: "08006", Message: "connection failure"}, want: true},
		{name: "PgError 08P01", err: &pgconn.PgError{Code: "08P01"}, want: true},

		// --- PgError class 57 (operator intervention) ---
		{name: "PgError 57000", err: &pgconn.PgError{Code: "57000"}, want: true},
		{name: "PgError 57014 query_canceled", err: &pgconn.PgError{Code: "57014"}, want: true},
		{name: "PgError 57P01 admin_shutdown", err: &pgconn.PgError{Code: "57P01", Message: "admin shutdown"}, want: true},
		{name: "PgError 57P02 crash_shutdown", err: &pgconn.PgError{Code: "57P02"}, want: true},
		{name: "PgError 57P03 cannot_connect_now", err: &pgconn.PgError{Code: "57P03"}, want: true},

		// --- PgError non-connection codes (query errors) ---
		{name: "PgError 42P01 relation does not exist", err: &pgconn.PgError{Code: "42P01"}, want: false},
		{name: "PgError 42601 syntax error", err: &pgconn.PgError{Code: "42601"}, want: false},
		{name: "PgError 23505 unique_violation", err: &pgconn.PgError{Code: "23505"}, want: false},
		{name: "PgError 42501 insufficient_privilege", err: &pgconn.PgError{Code: "42501"}, want: false},
		{name: "PgError empty code", err: &pgconn.PgError{Code: ""}, want: false},

		// --- Wrapped PgError ---
		{
			name: "wrapped PgError connection class",
			err:  fmt.Errorf("query: %w", &pgconn.PgError{Code: "08006"}),
			want: true,
		},
		{
			name: "wrapped PgError query class",
			err:  fmt.Errorf("query: %w", &pgconn.PgError{Code: "42P01"}),
			want: false,
		},

		// --- net.OpError ---
		{
			name: "net.OpError dial",
			err:  &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")},
			want: true,
		},
		{
			name: "net.OpError read",
			err:  &net.OpError{Op: "read", Net: "tcp", Err: errors.New("reset by peer")},
			want: true,
		},
		{
			name: "wrapped net.OpError",
			err:  fmt.Errorf("pool: %w", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("refused")}),
			want: true,
		},

		// --- os.SyscallError ---
		{
			name: "SyscallError connect",
			err:  &os.SyscallError{Syscall: "connect", Err: errors.New("refused")},
			want: true,
		},
		{
			name: "wrapped SyscallError",
			err:  fmt.Errorf("outer: %w", &os.SyscallError{Syscall: "write", Err: errors.New("broken pipe")}),
			want: true,
		},

		// --- String-based fallbacks ---
		{name: "connection refused string", err: errors.New("dial tcp: connection refused"), want: true},
		{name: "connection reset string", err: errors.New("read: connection reset by peer"), want: true},
		{name: "broken pipe string", err: errors.New("write: broken pipe"), want: true},
		{name: "no such host string", err: errors.New("dial tcp: lookup foo.invalid: no such host"), want: true},
		{name: "i/o timeout string", err: errors.New("i/o timeout"), want: true},
		{name: "connection timed out string", err: errors.New("dial tcp 10.0.0.1:5432: connection timed out"), want: true},
		{name: "mixed case Connection Refused", err: errors.New("Connection Refused"), want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsConnectionError(tt.err)
			if got != tt.want {
				t.Errorf("IsConnectionError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
