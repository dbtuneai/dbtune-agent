package pg

import (
	"testing"

	"github.com/dbtuneai/agent/pkg/pg/collectorconfig"
	"github.com/dbtuneai/agent/pkg/pg/queries"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCatalogCollectorsForVersion_Valid(t *testing.T) {
	cfg := newEmptyCfg()
	cs, err := CatalogCollectorsForVersion(nil, "16.4", cfg)
	require.NoError(t, err)
	assert.Greater(t, len(cs), 0, "should return collectors for valid version")
}

func TestCatalogCollectorsForVersion_InvalidVersion(t *testing.T) {
	cfg := newEmptyCfg()
	_, err := CatalogCollectorsForVersion(nil, "abc", cfg)
	require.Error(t, err)
}

func TestCatalogCollectorsForVersion_EmptyVersion(t *testing.T) {
	cfg := newEmptyCfg()
	_, err := CatalogCollectorsForVersion(nil, "", cfg)
	require.Error(t, err)
}

// TestCatalogCollectorsForVersion_AuroraDisablesPgStatWal mirrors the pattern
// used by CreateAuroraRDSAdapter: load config, disable pg_stat_wal, build.
func TestCatalogCollectorsForVersion_AuroraDisablesPgStatWal(t *testing.T) {
	cfg := newEmptyCfg()
	disabled := false
	cfg.Simple[queries.PgStatWalName] = collectorconfig.BaseConfig{Enabled: &disabled}

	cs, err := CatalogCollectorsForVersion(nil, "16.4", cfg)
	require.NoError(t, err)
	assert.Nil(t, findByName(cs, queries.PgStatWalName), "pg_stat_wal must be absent for Aurora")
	// Other collectors still present.
	assert.NotNil(t, findByName(cs, queries.PgStatDatabaseName))
}
