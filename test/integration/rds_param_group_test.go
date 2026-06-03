//go:build rds_integration

// Integration tests for the RDS adapter's parameter-group discovery and the
// default-PG refusal path introduced on fix-reject-default-parameter-group-for-rds.
//
// These hit real AWS APIs and a real Postgres connection, so they are gated
// behind the `rds_integration` build tag and excluded from the default
// `go test ./...` run.
//
// ─── HOW TO RUN ──────────────────────────────────────────────────────────────
//
// 1. AWS credentials available to the default chain (e.g. `aws sso login`).
// 2. Network reachability to the target DB (public IP, VPN, peering, etc.).
// 3. Export env. Example for a plain RDS instance:
//
//      export DBT_AWS_REGION=<region>
//      export DBT_RDS_DATABASE_IDENTIFIER=<db-instance-identifier>
//      export DBT_POSTGRESQL_CONNECTION_URL='postgres://user:pass@<host>:5432/postgres?sslmode=require'
//
//    For Aurora, point at a specific writer or reader instance (the agent
//    operates per-instance). The PG connection URL can target either the
//    instance endpoint or a cluster endpoint.
//
// 4. Run:
//
//      go test -tags=rds_integration -v -run TestRDSAdapter ./test/integration/...
//
// ─── OPTIONAL ASSERTIONS ─────────────────────────────────────────────────────
//
//   DBTUNE_TEST_EXPECTED_PG          assert exact instance PG name discovered
//   DBTUNE_TEST_EXPECTED_CLUSTER_PG  assert exact cluster PG name discovered
//
// ─── SWITCHING SCENARIOS (default vs custom) ─────────────────────────────────
//
// Refusal path:    leave the instance on `default.*` and run the apply test;
//                  expect `*agent.DefaultParameterGroupError`.
// Success path:    swap to a custom group, wait for in-sync, re-run:
//
//      aws rds modify-db-instance \
//        --db-instance-identifier $DBT_RDS_DATABASE_IDENTIFIER \
//        --db-parameter-group-name dbtune-test-custom-pg16 \
//        --apply-immediately
//      aws rds wait db-instance-available \
//        --db-instance-identifier $DBT_RDS_DATABASE_IDENTIFIER
//
// The success path calls `rds:ModifyDBParameterGroup` for real. It uses a
// dynamic, reload-only knob so no reboot is triggered.

package integration_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/rds"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupViper prepares package-global viper state for an adapter construction.
// The dbtune.* values are syntactically-valid stubs (they pass the UUID/URL
// validators in pkg/dbtune); no traffic is ever sent to them.
func setupViper(t *testing.T) {
	t.Helper()

	for _, key := range []string{
		"DBT_AWS_REGION",
		"DBT_RDS_DATABASE_IDENTIFIER",
		"DBT_POSTGRESQL_CONNECTION_URL",
	} {
		if os.Getenv(key) == "" {
			t.Skipf("required env var %s is not set", key)
		}
	}

	t.Setenv("DBT_DBTUNE_SERVER_URL", "http://localhost:9999")
	t.Setenv("DBT_DBTUNE_API_KEY", "00000000-0000-0000-0000-000000000001")
	t.Setenv("DBT_DBTUNE_DATABASE_ID", "00000000-0000-0000-0000-000000000002")

	viper.Reset()
}

func newAdapter(t *testing.T) *rds.RDSAdapter {
	t.Helper()
	setupViper(t)
	key := "rds"
	adapter, err := rds.CreateRDSAdapterWithoutCollectors(&key)
	require.NoError(t, err, "failed to construct RDS adapter")
	t.Cleanup(func() {
		if adapter.PGDriver != nil {
			adapter.PGDriver.Close()
		}
	})
	return adapter
}

func TestRDSAdapter_ParameterGroupDiscovery(t *testing.T) {
	adapter := newAdapter(t)

	info := adapter.State.DBInfo
	require.NotNil(t, info)
	t.Logf("discovered instance PG=%q  cluster PG=%q",
		info.ParameterGroupName, info.ClusterParameterGroupName)

	require.NotEmpty(t, info.ParameterGroupName,
		"every RDS/Aurora instance reports at least one DBParameterGroup")

	if want := os.Getenv("DBTUNE_TEST_EXPECTED_PG"); want != "" {
		assert.Equal(t, want, info.ParameterGroupName)
	}
	if want := os.Getenv("DBTUNE_TEST_EXPECTED_CLUSTER_PG"); want != "" {
		assert.Equal(t, want, info.ClusterParameterGroupName)
	}

	flats, err := adapter.GetSystemInfo(context.Background())
	require.NoError(t, err)

	keys := make(map[string]bool, len(flats))
	for _, f := range flats {
		keys[f.Key] = true
	}

	assert.True(t, keys[metrics.AWSRDSParameterGroup.Key],
		"system-info must include %q", metrics.AWSRDSParameterGroup.Key)

	if info.ClusterParameterGroupName != "" {
		assert.True(t, keys[metrics.AWSRDSClusterParameterGroup.Key],
			"system-info must include %q when cluster PG present",
			metrics.AWSRDSClusterParameterGroup.Key)
	} else {
		assert.False(t, keys[metrics.AWSRDSClusterParameterGroup.Key],
			"%q must be absent for non-clustered instances",
			metrics.AWSRDSClusterParameterGroup.Key)
	}
}

// TestRDSAdapter_ApplyConfig_BehaviourMatchesAttachedPG asserts the branch's
// contract: ApplyConfig refuses iff the attached PG starts with "default.".
// On a custom PG, it issues a real ModifyDBParameterGroup against AWS.
func TestRDSAdapter_ApplyConfig_BehaviourMatchesAttachedPG(t *testing.T) {
	viper.Set("postgresql.allow_restart", false)
	adapter := newAdapter(t)

	pgName := adapter.State.DBInfo.ParameterGroupName
	require.NotEmpty(t, pgName)

	// Dynamic, reload-only knob so the custom-PG branch does not reboot.
	proposed := &agent.ProposedConfigResponse{
		KnobApplication: agent.KnobApplicationReload,
		KnobsOverrides:  []string{"log_min_duration_statement"},
		Config: []agent.PGConfigRow{
			{
				Name:    "log_min_duration_statement",
				Setting: 1000,
				Unit:    "ms",
				Vartype: "integer",
				Context: "superuser",
			},
		},
	}

	err := adapter.ApplyConfig(context.Background(), proposed)

	if strings.HasPrefix(pgName, "default.") {
		var typed *agent.DefaultParameterGroupError
		require.Error(t, err)
		require.True(t, errors.As(err, &typed),
			"expected *agent.DefaultParameterGroupError, got %T: %v", err, err)
		assert.Equal(t, pgName, typed.ParameterGroupName)
		assert.Equal(t, "default_parameter_group", err.ErrorType())
		return
	}

	require.NoError(t, err,
		"apply against custom PG %q should succeed; got %v", pgName, err)
}
