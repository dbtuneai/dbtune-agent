package catalog

import (
	"testing"

	"github.com/dbtuneai/agent/pkg/catalog/collectorconfig"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countByName counts how many collectors have the given name.
func countByName(cs []Collector, name string) int {
	n := 0
	for _, c := range cs {
		if c.Name == name {
			n++
		}
	}
	return n
}

// findByName returns the first collector with the given name, or nil.
func findByName(cs []Collector, name string) *Collector {
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

func TestBuildCollectors_DefaultIncludesAll(t *testing.T) {
	cfg := newEmptyCfg()
	// PG 17+ so all version-gated simple collectors stay in.
	cs, err := BuildCollectors(nil, 17, cfg, nil, discardLogger())
	require.NoError(t, err)

	// 36 simple entries + 7 typed entries in the reference build list
	// (pg_index hot + pg_index_inventory each counted separately).
	assert.Equal(t, 43, len(cs), "default build should include all collectors at PG 17")
	assert.NotNil(t, findByName(cs, PgStatStatementsName))
	assert.NotNil(t, findByName(cs, PgStatCheckpointerName))
	assert.NotNil(t, findByName(cs, PgStatWalName))
	assert.NotNil(t, findByName(cs, CapabilitiesName))
}

func TestBuildCollectors_DisableOne(t *testing.T) {
	disabled := false
	cfg := newEmptyCfg()
	cfg.Simple[PgStatWalName] = collectorconfig.BaseConfig{Enabled: &disabled}

	cs, err := BuildCollectors(nil, 17, cfg, nil, discardLogger())
	require.NoError(t, err)
	assert.Equal(t, 0, countByName(cs, PgStatWalName), "disabled collector should be dropped")
	// Other collectors unaffected.
	assert.NotNil(t, findByName(cs, PgStatCheckpointerName))
}

func TestBuildCollectors_IntervalOverride(t *testing.T) {
	cfg := newEmptyCfg()
	// pg_stats default interval is much larger than 60s, so 600s must be allowed.
	secs := 600
	cfg.Simple[PgStatArchiverName] = collectorconfig.BaseConfig{IntervalSeconds: &secs}

	cs, err := BuildCollectors(nil, 17, cfg, nil, discardLogger())
	require.NoError(t, err)
	archiver := findByName(cs, PgStatArchiverName)
	if assert.NotNil(t, archiver, "archiver collector must be present") {
		assert.Equal(t, float64(600), archiver.Interval.Seconds())
	}
}

// A configured interval below the collector's default is a user error and
// must surface — we do not silently fall back to the default.
func TestBuildCollectors_IntervalBelowMinimumFailsFast(t *testing.T) {
	cfg := newEmptyCfg()
	// 1 second is below every default; use a collector whose default is > 1s.
	secs := 1
	cfg.Simple[PgStatArchiverName] = collectorconfig.BaseConfig{IntervalSeconds: &secs}

	cs, err := BuildCollectors(nil, 17, cfg, nil, discardLogger())
	require.Error(t, err, "build must fail when interval is below the collector default")
	assert.Nil(t, cs)
	assert.Contains(t, err.Error(), PgStatArchiverName, "error must name the offending collector")
}

func TestCollectorsForVersion_Valid(t *testing.T) {
	cfg := newEmptyCfg()
	cs, err := CollectorsForVersion(nil, pg.Version{Major: 16, Minor: 4, HasMinor: true}, cfg, discardLogger())
	require.NoError(t, err)
	assert.Greater(t, len(cs), 0, "should return collectors for valid version")
}

// TestCollectorsForVersion_AuroraDisablesPgStatWal mirrors the pattern
// used by CreateAuroraRDSAdapter: load config, disable the WAL collectors, build.
func TestCollectorsForVersion_AuroraDisablesPgStatWal(t *testing.T) {
	cfg := newEmptyCfg()
	disabled := false
	cfg.Simple[PgStatWalName] = collectorconfig.BaseConfig{Enabled: &disabled}
	cfg.Simple[PgStatWalReceiverName] = collectorconfig.BaseConfig{Enabled: &disabled}

	cs, err := CollectorsForVersion(nil, pg.Version{Major: 16, Minor: 4, HasMinor: true}, cfg, discardLogger())
	require.NoError(t, err)
	assert.Nil(t, findByName(cs, PgStatWalName), "pg_stat_wal must be absent for Aurora")
	assert.Nil(t, findByName(cs, PgStatWalReceiverName), "pg_stat_wal_receiver must be absent for Aurora")
	// Other collectors still present.
	assert.NotNil(t, findByName(cs, PgStatDatabaseName))
}
