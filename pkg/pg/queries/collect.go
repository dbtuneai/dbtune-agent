package queries

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/dbtuneai/agent/pkg/internal/pgxutil"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CatalogCollector defines a periodic catalog collection task.
type CatalogCollector struct {
	Name          string
	Interval      time.Duration
	Collect       func(ctx context.Context) (*CollectResult, error)
	SkipUnchanged bool // When true, skip send if payload hash matches previous
}

// CollectResult holds both the typed payload and its pre-marshaled JSON bytes.
// JSON is marshaled once at collection time so the sending pipeline can reuse
// the bytes without re-serializing.
type CollectResult struct {
	JSON []byte // Pre-marshaled JSON of the payload
}

// PrepareCtx is a hook called before each catalog query to allow adapters
// (e.g. CNPG, Patroni) to perform failover checks or replace the context.
type PrepareCtx func(ctx context.Context) (context.Context, error)

// Payload wraps a slice of rows collected from a catalog view.
type Payload[T any] struct {
	CollectedAt time.Time `json:"collected_at"`
	Rows        []T       `json:"rows"`
}

// collectorOpts holds optional configuration for NewCollector.
type collectorOpts struct {
	skipUnchanged  bool
	pgMajorVersion int
	minPGVersion   int
}

// CollectorOption configures optional behavior for NewCollector.
type CollectorOption func(*collectorOpts)

// WithSkipUnchanged marks the collector to skip sending if the JSON payload
// hash matches the previous collection.
func WithSkipUnchanged() CollectorOption {
	return func(o *collectorOpts) { o.skipUnchanged = true }
}

// WithMinPGVersion skips collection when pgMajorVersion is below minVersion.
func WithMinPGVersion(pgMajorVersion, minVersion int) CollectorOption {
	return func(o *collectorOpts) {
		o.pgMajorVersion = pgMajorVersion
		o.minPGVersion = minVersion
	}
}

// skipUnchangedForceMultiplier controls how many collection intervals can
// pass before a hashed collector forces a send even if unchanged. This acts
// as a heartbeat so the backend knows the collector is still alive.
const skipUnchangedForceMultiplier = 30

// skipTracker tracks hash-based deduplication state for SkipUnchanged collectors.
type skipTracker struct {
	lastHash       uint64
	initialized    bool
	unchangedCount int
	forceAfter     int // number of unchanged collections before forcing a send
}

func newSkipTracker(forceAfter int) *skipTracker {
	return &skipTracker{forceAfter: forceAfter}
}

// shouldSkip returns true if data is unchanged and the force-send threshold
// hasn't been reached. When it returns false (either changed or forced), the
// caller should send the data.
func (s *skipTracker) shouldSkip(data []byte) bool {
	h := xxhash.Sum64(data)
	if s.initialized && h == s.lastHash {
		s.unchangedCount++
		if s.unchangedCount < s.forceAfter {
			return true
		}
		// Force send as heartbeat, reset counter
		s.unchangedCount = 0
		return false
	}
	s.lastHash = h
	s.initialized = true
	s.unchangedCount = 0
	return false
}

// NewCollector creates a CatalogCollector for a catalog view query.
// JSON is marshaled once per collection; when SkipUnchanged is set, an xxhash
// of the bytes is compared to the previous collection to avoid redundant sends.
// Even when unchanged, a send is forced every skipUnchangedForceMultiplier
// collection intervals to act as a heartbeat.
func NewCollector[T any](
	pool *pgxpool.Pool,
	prepareCtx PrepareCtx,
	name string,
	interval time.Duration,
	query string,
	opts ...CollectorOption,
) CatalogCollector {
	var cfg collectorOpts
	for _, o := range opts {
		o(&cfg)
	}
	scanner := pgxutil.NewScanner[T]()
	tracker := newSkipTracker(skipUnchangedForceMultiplier)
	return CatalogCollector{
		Name:          name,
		Interval:      interval,
		SkipUnchanged: cfg.skipUnchanged,
		Collect: func(ctx context.Context) (*CollectResult, error) {
			if cfg.minPGVersion > 0 && cfg.pgMajorVersion < cfg.minPGVersion {
				return nil, nil
			}
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			collectedAt := time.Now().UTC()
			querier := func() (pgx.Rows, error) {
				return utils.QueryWithPrefix(pool, ctx, query)
			}
			rows, err := CollectView(querier, name, scanner)
			if err != nil {
				return nil, err
			}
			if cfg.skipUnchanged {
				rowsData, err := json.Marshal(rows)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal %s rows for hash: %w", name, err)
				}
				if tracker.shouldSkip(rowsData) {
					return nil, nil
				}
			}
			data, err := json.Marshal(&Payload[T]{CollectedAt: collectedAt, Rows: rows})
			if err != nil {
				return nil, fmt.Errorf("failed to marshal %s: %w", name, err)
			}
			return &CollectResult{JSON: data}, nil
		},
	}
}

// Hash returns a hex string of the xxhash of the JSON bytes, suitable for
// sending to the backend as a change-detection token.
func (r *CollectResult) Hash() string {
	return strconv.FormatUint(xxhash.Sum64(r.JSON), 16)
}

// CollectView queries a pg catalog view and scans the results into a slice
// of structs using the provided scanner. The querier function should set a deadline, eg via a context.
func CollectView[T any](querier func() (pgx.Rows, error), viewName string, scanner *pgxutil.Scanner[T]) ([]T, error) {
	rows, err := querier()
	if err != nil {
		return nil, fmt.Errorf("failed to query %s: %w", viewName, err)
	}
	defer rows.Close()

	return pgx.CollectRows(rows, scanner.Scan)
}
