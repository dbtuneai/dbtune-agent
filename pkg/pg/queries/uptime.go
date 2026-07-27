package queries

// UptimeMinutes reports how long the PostgreSQL server has been running,
// in minutes, by computing the difference between the current timestamp and
// pg_postmaster_start_time(). The payload also carries cluster identity —
// system identifier, timeline ID and postmaster start time — so the backend
// can distinguish restarts, restores and failovers (DBT-2347). Identity
// fields are optional: they are omitted when unavailable.
//
// https://www.postgresql.org/docs/current/functions-info.html#FUNCTIONS-INFO-SESSION
// https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADMIN-CONTROLDATA

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"
)

type UptimeMinutesRow struct {
	UptimeMinutes       float64 `json:"uptime_minutes"`
	SystemIdentifier    *string `json:"system_identifier,omitempty"`
	TimelineID          *int64  `json:"timeline_id,omitempty"`
	PostmasterStartTime *string `json:"postmaster_start_time,omitempty"`
}

const (
	UptimeMinutesName     = "server_uptime"
	UptimeMinutesInterval = 5 * time.Second
)

const uptimeMinutesQuery = `
SELECT EXTRACT(EPOCH FROM (current_timestamp - pg_postmaster_start_time())) / 60 AS uptime_minutes,
       pg_postmaster_start_time() AS postmaster_start_time`

const clusterIdentityQuery = `
SELECT (pg_control_system()).system_identifier AS system_identifier,
       (pg_control_checkpoint()).timeline_id AS timeline_id`

// clusterIdentityRefreshInterval bounds how often the pg_control_* query
// runs. Identity only changes on restart/restore/failover, so a few minutes
// of staleness is fine and avoids an extra round-trip on every 5s tick.
const clusterIdentityRefreshInterval = 10 * time.Minute

type clusterIdentity struct {
	systemIdentifier string
	timelineID       int64
}

// clusterIdentityCache lazily queries and caches the cluster identity.
// pg_control_system()/pg_control_checkpoint() can be permission-restricted
// on managed services (RDS/Aurora/CloudSQL) or missing on old versions; on
// failure the identity is omitted and the failure logged once per streak.
// Not safe for concurrent use — each collector runs in a single goroutine.
type clusterIdentityCache struct {
	pool          *pgxpool.Pool
	cached        *clusterIdentity
	lastAttempt   time.Time
	loggedFailure bool
}

func (c *clusterIdentityCache) get(ctx context.Context) *clusterIdentity {
	if !c.lastAttempt.IsZero() && time.Since(c.lastAttempt) < clusterIdentityRefreshInterval {
		return c.cached
	}
	c.lastAttempt = time.Now()
	var systemIdentifier int64
	var timelineID int64
	err := utils.QueryRowWithPrefix(c.pool, ctx, clusterIdentityQuery).Scan(&systemIdentifier, &timelineID)
	if err != nil {
		if !c.loggedFailure {
			slog.Warn("cluster identity unavailable; omitting system_identifier/timeline_id from server_uptime",
				"error", err)
			c.loggedFailure = true
		}
		c.cached = nil
		return nil
	}
	c.loggedFailure = false
	c.cached = &clusterIdentity{
		// system_identifier is a uint64 exposed through bigint; reinterpret
		// the bits so values above MaxInt64 render as unsigned decimal.
		systemIdentifier: strconv.FormatUint(uint64(systemIdentifier), 10), //nolint:gosec // intentional bit reinterpretation
		timelineID:       timelineID,
	}
	return c.cached
}

func UptimeMinutesCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	identity := &clusterIdentityCache{pool: pool}
	return CatalogCollector{
		Name:     UptimeMinutesName,
		Interval: UptimeMinutesInterval,
		Collect: func(ctx context.Context) (*CollectResult, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			collectedAt := time.Now().UTC()
			var uptimeMinutes float64
			var postmasterStartTime time.Time
			err = utils.QueryRowWithPrefix(pool, ctx, uptimeMinutesQuery).
				Scan(&uptimeMinutes, &postmasterStartTime)
			if err != nil {
				return nil, fmt.Errorf("failed to query %s: %w", UptimeMinutesName, err)
			}

			row := UptimeMinutesRow{UptimeMinutes: uptimeMinutes}
			if !postmasterStartTime.IsZero() {
				started := postmasterStartTime.UTC().Format(time.RFC3339)
				row.PostmasterStartTime = &started
			}
			if id := identity.get(ctx); id != nil {
				row.SystemIdentifier = &id.systemIdentifier
				row.TimelineID = &id.timelineID
			}

			data, err := json.Marshal(&Payload[UptimeMinutesRow]{
				CollectedAt: collectedAt,
				Rows:        []UptimeMinutesRow{row},
			})
			if err != nil {
				return nil, fmt.Errorf("failed to marshal %s: %w", UptimeMinutesName, err)
			}
			return &CollectResult{JSON: data}, nil
		},
	}
}
