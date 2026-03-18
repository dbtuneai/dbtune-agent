package agent

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

// ErrDatabaseDown is returned by HealthGate.Check when the database is unreachable.
var ErrDatabaseDown = errors.New("database health gate: connection unavailable")

// HealthGate tracks database connectivity and short-circuits catalog queries
// when the database is unreachable. Instead of ~30 collectors each timing out
// independently, a single background goroutine pings until recovery.
//
// All methods are nil-receiver safe — a nil *HealthGate is a no-op gate that
// never blocks. This lets callers skip nil checks entirely.
type HealthGate struct {
	pool              *pgxpool.Pool
	isConnectionError func(error) bool
	logger            *logrus.Logger
	down              atomic.Bool
	// pinging is used as a CompareAndSwap guard so only one goroutine pings.
	pinging atomic.Bool
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewHealthGate creates a ready-to-use HealthGate. The gate's internal context
// is derived from parent, so cancelling parent also stops any recovery pings.
func NewHealthGate(parent context.Context, pool *pgxpool.Pool, isConnErr func(error) bool, logger *logrus.Logger) *HealthGate {
	ctx, cancel := context.WithCancel(parent)
	return &HealthGate{
		pool:              pool,
		isConnectionError: isConnErr,
		logger:            logger,
		ctx:               ctx,
		cancel:            cancel,
	}
}

// Stop cancels any running ping goroutine. Only needed in tests for cleanup;
// production code lets the process exit naturally.
func (h *HealthGate) Stop() {
	if h == nil {
		return
	}
	h.cancel()
}

// Check returns ErrDatabaseDown if the gate is closed, nil otherwise.
func (h *HealthGate) Check() error {
	if h == nil || !h.down.Load() {
		return nil
	}
	return ErrDatabaseDown
}

// ReportError inspects err and, if it is a connection-level error, marks the
// gate as closed and spawns the ping-until-recovery goroutine (at most once).
func (h *HealthGate) ReportError(err error) {
	if h == nil || err == nil || !h.isConnectionError(err) {
		return
	}
	h.down.Store(true)
	if h.pool == nil {
		return // no pool to recover with
	}
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
		backoff = min(backoff*2, maxBackoff)
	}
}
