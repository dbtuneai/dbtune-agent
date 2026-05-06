package queries

// https://www.postgresql.org/docs/current/catalog-pg-index.html

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/dbtuneai/agent/pkg/internal/pgxutil"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgIndexRow carries the fast-moving fields per index: stats (reltuples,
// relpages) and the lifecycle bools that flip during REINDEX, CREATE INDEX
// CONCURRENTLY, validation, and clustering. Definitional fields (indexdef,
// indkey, indisunique, etc.) are emitted by the separate pg_index_inventory
// collector on a slower cadence.
type PgIndexRow struct {
	SchemaName     *Name    `json:"schemaname" db:"schemaname"`
	IndexName      *Name    `json:"indexname" db:"indexname"`
	RelTuples      *Real    `json:"reltuples" db:"reltuples"`
	RelPages       *Integer `json:"relpages" db:"relpages"`
	IndIsValid     *Boolean `json:"indisvalid" db:"indisvalid"`
	IndIsReady     *Boolean `json:"indisready" db:"indisready"`
	IndIsLive      *Boolean `json:"indislive" db:"indislive"`
	IndImmediate   *Boolean `json:"indimmediate" db:"indimmediate"`
	IndIsClustered *Boolean `json:"indisclustered" db:"indisclustered"`
	IndIsReplIdent *Boolean `json:"indisreplident" db:"indisreplident"`
	IndCheckXmin   *Boolean `json:"indcheckxmin" db:"indcheckxmin"`
}

const (
	PgIndexName     = "pg_index"
	PgIndexInterval = 5 * time.Minute
)

// pgIndexHeartbeatEvery is the number of collection intervals after which
// the per-row hash cache is cleared, forcing the next collection to re-emit
// every row as a safety net against dedup drift.
//
// At PgIndexInterval = 5min, 288 intervals = 24h between full re-sends.
const pgIndexHeartbeatEvery = 288

const pgIndexQuery = `
SELECT
    n.nspname AS schemaname,
    c.relname AS indexname,
    c.reltuples::float8 AS reltuples,
    c.relpages,
    i.indisvalid,
    i.indisready,
    i.indislive,
    i.indimmediate,
    i.indisclustered,
    i.indisreplident,
    i.indcheckxmin
FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
JOIN pg_class t ON t.oid = i.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
ORDER BY n.nspname, c.relname
`

type pgIndexKey struct {
	Schema string
	Name   string
}

// pgIndexState holds the per-row hash cache and the heartbeat counter. The
// cache is bounded by the number of indexes on the source database and is
// not persisted across agent restarts — a fresh agent starts empty and
// re-sends every row on its first collection, which is the same behaviour
// the heartbeat produces.
type pgIndexState struct {
	hashes         map[pgIndexKey]uint64
	sinceHeartbeat int
	heartbeatEvery int
}

func newPgIndexState(heartbeatEvery int) *pgIndexState {
	return &pgIndexState{
		hashes:         make(map[pgIndexKey]uint64),
		heartbeatEvery: heartbeatEvery,
	}
}

func hashPgIndexRow(r *PgIndexRow) (uint64, error) {
	b, err := json.Marshal(struct {
		T  *Real    `json:"t,omitempty"`
		P  *Integer `json:"p,omitempty"`
		V  *Boolean `json:"v,omitempty"`
		Re *Boolean `json:"re,omitempty"`
		L  *Boolean `json:"l,omitempty"`
		Im *Boolean `json:"im,omitempty"`
		C  *Boolean `json:"c,omitempty"`
		Ri *Boolean `json:"ri,omitempty"`
		Cx *Boolean `json:"cx,omitempty"`
	}{
		T: r.RelTuples, P: r.RelPages,
		V: r.IndIsValid, Re: r.IndIsReady, L: r.IndIsLive,
		Im: r.IndImmediate, C: r.IndIsClustered, Ri: r.IndIsReplIdent,
		Cx: r.IndCheckXmin,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to marshal pg_index volatile fields for hash: %w", err)
	}
	return xxhash.Sum64(b), nil
}

// filter returns the subset of rows whose volatile-column hash differs from
// the previous collection, updating the cache in place. Rows missing
// required identifying fields are dropped. After heartbeatEvery invocations
// the cache is cleared so the next call re-emits every row.
func (s *pgIndexState) filter(rows []PgIndexRow) ([]PgIndexRow, error) {
	s.sinceHeartbeat++
	if s.sinceHeartbeat >= s.heartbeatEvery {
		s.hashes = make(map[pgIndexKey]uint64)
		s.sinceHeartbeat = 0
	}

	seen := make(map[pgIndexKey]struct{}, len(rows))
	changed := make([]PgIndexRow, 0, len(rows))
	for _, r := range rows {
		if r.SchemaName == nil || r.IndexName == nil {
			continue
		}
		k := pgIndexKey{Schema: string(*r.SchemaName), Name: string(*r.IndexName)}
		seen[k] = struct{}{}
		h, err := hashPgIndexRow(&r)
		if err != nil {
			return nil, err
		}
		if prev, ok := s.hashes[k]; !ok || prev != h {
			changed = append(changed, r)
			s.hashes[k] = h
		}
	}
	for k := range s.hashes {
		if _, ok := seen[k]; !ok {
			delete(s.hashes, k)
		}
	}
	return changed, nil
}

// PgIndexCollector returns a CatalogCollector that emits only the rows
// whose volatile stats (reltuples, relpages) have changed since the
// previous collection, with a periodic heartbeat that re-emits everything.
// The generic WithSkipUnchanged machinery is deliberately not used here
// because it operates at whole-payload granularity; per-row dedup is the
// right shape for this catalog.
func PgIndexCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return pgIndexCollectorWithHeartbeat(pool, prepareCtx, pgIndexHeartbeatEvery)
}

// pgIndexCollectorWithHeartbeat is the heartbeat-injectable constructor used
// by tests; production code goes through PgIndexCollector.
func pgIndexCollectorWithHeartbeat(pool *pgxpool.Pool, prepareCtx PrepareCtx, heartbeatEvery int) CatalogCollector {
	scanner := pgxutil.NewScanner[PgIndexRow]()
	state := newPgIndexState(heartbeatEvery)

	return CatalogCollector{
		Name:     PgIndexName,
		Interval: PgIndexInterval,
		Collect: func(ctx context.Context) (*CollectResult, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			collectedAt := time.Now().UTC()

			querier := func() (pgx.Rows, error) {
				return utils.QueryWithPrefix(pool, ctx, pgIndexQuery)
			}
			rows, err := CollectView(querier, PgIndexName, scanner)
			if err != nil {
				return nil, err
			}

			changed, err := state.filter(rows)
			if err != nil {
				return nil, err
			}
			if len(changed) == 0 {
				return nil, nil
			}

			data, err := json.Marshal(&Payload[PgIndexRow]{
				CollectedAt: collectedAt,
				Rows:        changed,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to marshal %s: %w", PgIndexName, err)
			}
			return &CollectResult{JSON: data}, nil
		},
	}
}
