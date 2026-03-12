package pg

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

// ErrDatabaseDown is returned by HealthGate.Check when the database is unreachable.
var ErrDatabaseDown = errors.New("database health gate: connection unavailable")

// HealthGate tracks database connectivity and short-circuits catalog queries
// when the database is unreachable. Instead of ~30 collectors each timing out
// independently, a single background goroutine pings until recovery.
type HealthGate struct {
	pool   *pgxpool.Pool
	logger *logrus.Logger
	down   atomic.Bool
	// pinging is used as a CompareAndSwap guard so only one goroutine pings.
	pinging atomic.Bool
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewHealthGate creates a new HealthGate. Call Start before use.
func NewHealthGate(pool *pgxpool.Pool, logger *logrus.Logger) *HealthGate {
	return &HealthGate{
		pool:   pool,
		logger: logger,
	}
}

// Start stores the parent context used for the ping goroutine lifecycle.
func (h *HealthGate) Start(ctx context.Context) {
	h.ctx, h.cancel = context.WithCancel(ctx)
}

// Stop cancels any running ping goroutine.
func (h *HealthGate) Stop() {
	if h.cancel != nil {
		h.cancel()
	}
}

// Check returns ErrDatabaseDown if the gate is closed, nil otherwise.
func (h *HealthGate) Check() error {
	if h.down.Load() {
		return ErrDatabaseDown
	}
	return nil
}

// ReportError inspects err and, if it is a connection-level error, marks the
// gate as closed and spawns the ping-until-recovery goroutine (at most once).
func (h *HealthGate) ReportError(err error) {
	if err == nil || !IsConnectionError(err) {
		return
	}
	h.down.Store(true)
	// Only one goroutine should ping at a time.
	if h.pinging.CompareAndSwap(false, true) {
		h.logger.Warn("health gate: database connection lost, starting recovery pings")
		go h.pingUntilRecovery()
	}
}

func (h *HealthGate) pingUntilRecovery() {
	defer h.pinging.Store(false)

	backoff := 1 * time.Second
	const maxBackoff = 30 * time.Second

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-time.After(backoff):
		}

		err := h.pool.Ping(h.ctx)
		if err == nil {
			h.logger.Info("health gate: database connection recovered")
			h.down.Store(false)
			return
		}
		h.logger.Debugf("health gate: ping failed (%v), retrying in %s", err, backoff)

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

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
