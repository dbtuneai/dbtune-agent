package pg

import (
	"testing"

	"github.com/dbtuneai/agent/pkg/pg/collectorconfig"
	"github.com/dbtuneai/agent/pkg/pg/queries"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countByName counts how many collectors have the given name.
func countByName(cs []queries.CatalogCollector, name string) int {
	n := 0
	for _, c := range cs {
		if c.Name == name {
			n++
		}
	}
	return n
}

// findByName returns the first collector with the given name, or nil.
func findByName(cs []queries.CatalogCollector, name string) *queries.CatalogCollector {
	for i := range cs {
		if cs[i].Name == name {
			return &cs[i]
		}
	}
	return nil
}

func newEmptyCfg() CollectorsConfig {
	return CollectorsConfig{Simple: map[string]collectorconfig.BaseConfig{}}
}

func TestBuildCatalogCollectors_DefaultIncludesAll(t *testing.T) {
	cfg := newEmptyCfg()
	// PG 17+ so all version-gated simple collectors stay in.
	cs, err := BuildCatalogCollectors(nil, 17, cfg, nil)
	require.NoError(t, err)

	// 35 simple entries + 7 typed entries in the reference build list
	// (pg_index hot + pg_index_inventory each counted separately).
	assert.Equal(t, 42, len(cs), "default build should include all collectors at PG 17")
	assert.NotNil(t, findByName(cs, queries.PgStatStatementsName))
	assert.NotNil(t, findByName(cs, queries.PgStatCheckpointerName))
	assert.NotNil(t, findByName(cs, queries.PgStatWalName))
}

func TestBuildCatalogCollectors_DisableOne(t *testing.T) {
	disabled := false
	cfg := newEmptyCfg()
	cfg.Simple[queries.PgStatWalName] = collectorconfig.BaseConfig{Enabled: &disabled}

	cs, err := BuildCatalogCollectors(nil, 17, cfg, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, countByName(cs, queries.PgStatWalName), "disabled collector should be dropped")
	// Other collectors unaffected.
	assert.NotNil(t, findByName(cs, queries.PgStatCheckpointerName))
}

func TestBuildCatalogCollectors_IntervalOverride(t *testing.T) {
	cfg := newEmptyCfg()
	// pg_stats default interval is much larger than 60s, so 600s must be allowed.
	secs := 600
	cfg.Simple[queries.PgStatArchiverName] = collectorconfig.BaseConfig{IntervalSeconds: &secs}

	cs, err := BuildCatalogCollectors(nil, 17, cfg, nil)
	require.NoError(t, err)
	archiver := findByName(cs, queries.PgStatArchiverName)
	if assert.NotNil(t, archiver, "archiver collector must be present") {
		assert.Equal(t, float64(600), archiver.Interval.Seconds())
	}
}

// A configured interval below the collector's default is a user error and
// must surface — we do not silently fall back to the default.
func TestBuildCatalogCollectors_IntervalBelowMinimumFailsFast(t *testing.T) {
	cfg := newEmptyCfg()
	// 1 second is below every default; use a collector whose default is > 1s.
	secs := 1
	cfg.Simple[queries.PgStatArchiverName] = collectorconfig.BaseConfig{IntervalSeconds: &secs}

	cs, err := BuildCatalogCollectors(nil, 17, cfg, nil)
	require.Error(t, err, "build must fail when interval is below the collector default")
	assert.Nil(t, cs)
	assert.Contains(t, err.Error(), queries.PgStatArchiverName, "error must name the offending collector")
}
