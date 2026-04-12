package queries

// https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-BGWRITER

import (
	"github.com/dbtuneai/agent/pkg/pg/collectorconfig"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStatBgwriterRow represents a row from pg_stat_bgwriter.
type PgStatBgwriterRow struct {
	CheckpointsTimed    *Bigint          `json:"checkpoints_timed" db:"checkpoints_timed"`
	CheckpointsReq      *Bigint          `json:"checkpoints_req" db:"checkpoints_req"`
	CheckpointWriteTime *DoublePrecision `json:"checkpoint_write_time" db:"checkpoint_write_time"`
	CheckpointSyncTime  *DoublePrecision `json:"checkpoint_sync_time" db:"checkpoint_sync_time"`
	BuffersCheckpoint   *Bigint          `json:"buffers_checkpoint" db:"buffers_checkpoint"`
	BuffersClean        *Bigint          `json:"buffers_clean" db:"buffers_clean"`
	MaxwrittenClean     *Bigint          `json:"maxwritten_clean" db:"maxwritten_clean"`
	BuffersBackend      *Bigint          `json:"buffers_backend" db:"buffers_backend"`
	BuffersBackendFsync *Bigint          `json:"buffers_backend_fsync" db:"buffers_backend_fsync"`
	BuffersAlloc        *Bigint          `json:"buffers_alloc" db:"buffers_alloc"`
	StatsReset          *TimestampTZ     `json:"stats_reset" db:"stats_reset"`
}

const (
	PgStatBgwriterName     = "pg_stat_bgwriter"
	PgStatBgwriterInterval = 1 * time.Minute
)

const pgStatBgwriterQuery = `SELECT * FROM pg_stat_bgwriter`

// PgStatBgwriterRegistration describes the pgstatbgwriter collector's configuration schema.
var PgStatBgwriterRegistration = collectorconfig.CollectorRegistration{
	Name: PgStatBgwriterName,
	Kind: collectorconfig.CatalogCollectorKind,
}

func PgStatBgwriterCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return NewCollector[PgStatBgwriterRow](pool, prepareCtx, PgStatBgwriterName, PgStatBgwriterInterval, pgStatBgwriterQuery)
}
