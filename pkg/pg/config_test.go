package pg

import (
	"strings"
	"testing"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testConnectionURL = "postgres://user:pass@localhost:5432/db"

// TestConfigFromViper_AllowRestartFromEnv locks in that the env var
// DBT_POSTGRESQL_ALLOW_RESTART populates pgConfig.AllowRestart via the
// sub-viper unmarshal path.
func TestConfigFromViper_AllowRestartFromEnv(t *testing.T) {
	viper.Reset()
	defer viper.Reset()

	t.Setenv("DBT_POSTGRESQL_CONNECTION_URL", testConnectionURL)
	t.Setenv("DBT_POSTGRESQL_ALLOW_RESTART", "true")

	cfg, err := ConfigFromViper(nil)
	require.NoError(t, err)
	assert.True(t, cfg.AllowRestart, "pgConfig.AllowRestart should reflect DBT_POSTGRESQL_ALLOW_RESTART")
}

// TestConfigFromViper_GlobalViperSeesAllowRestartEnv is the regression
// test for the bug where viper.Sub("postgresql").BindEnv(...) did not
// propagate to the global viper, so viper.GetBool("postgresql.allow_restart")
// returned false even when DBT_POSTGRESQL_ALLOW_RESTART=true was set.
//
// The fix in ConfigFromViper adds matching BindEnv calls on the global
// viper. If a future refactor removes those calls, this test fails.
func TestConfigFromViper_GlobalViperSeesAllowRestartEnv(t *testing.T) {
	t.Run("env var only", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		t.Setenv("DBT_POSTGRESQL_CONNECTION_URL", testConnectionURL)
		t.Setenv("DBT_POSTGRESQL_ALLOW_RESTART", "true")

		_, err := ConfigFromViper(nil)
		require.NoError(t, err)

		assert.True(t,
			viper.GetBool("postgresql.allow_restart"),
			"global viper should resolve DBT_POSTGRESQL_ALLOW_RESTART at postgresql.allow_restart",
		)
	})

	t.Run("env var with config file present", func(t *testing.T) {
		viper.Reset()
		defer viper.Reset()

		// A config file with a postgresql: section causes ConfigFromViper to
		// take the viper.Sub() branch — this is the path that previously
		// dropped the env var on the global viper.
		viper.SetConfigType("yaml")
		require.NoError(t, viper.ReadConfig(strings.NewReader(
			"postgresql:\n  connection_url: "+testConnectionURL+"\n  allow_restart: false\n",
		)))

		t.Setenv("DBT_POSTGRESQL_ALLOW_RESTART", "true")

		cfg, err := ConfigFromViper(nil)
		require.NoError(t, err)

		assert.True(t, cfg.AllowRestart, "env var should override config file in pgConfig")
		assert.True(t,
			viper.GetBool("postgresql.allow_restart"),
			"global viper should also see env var override (regression: sub-viper bind did not propagate)",
		)
	})
}

// TestIsRestartAllowed_ResolvesEnvVar exercises the actual symptom the
// user reported: agent.IsRestartAllowed() returned false despite
// DBT_POSTGRESQL_ALLOW_RESTART=true. It must return true after
// ConfigFromViper has run.
func TestIsRestartAllowed_ResolvesEnvVar(t *testing.T) {
	viper.Reset()
	defer viper.Reset()

	t.Setenv("DBT_POSTGRESQL_CONNECTION_URL", testConnectionURL)
	t.Setenv("DBT_POSTGRESQL_ALLOW_RESTART", "true")

	_, err := ConfigFromViper(nil)
	require.NoError(t, err)

	assert.True(t, agent.IsRestartAllowed(),
		"agent.IsRestartAllowed() must resolve DBT_POSTGRESQL_ALLOW_RESTART through the global viper")
}

// TestIsRestartAllowed_DefaultsFalse confirms the safe default holds when
// neither env var nor config file sets allow_restart.
func TestIsRestartAllowed_DefaultsFalse(t *testing.T) {
	viper.Reset()
	defer viper.Reset()

	t.Setenv("DBT_POSTGRESQL_CONNECTION_URL", testConnectionURL)

	_, err := ConfigFromViper(nil)
	require.NoError(t, err)

	assert.False(t, agent.IsRestartAllowed(),
		"IsRestartAllowed() must default to false when nothing sets allow_restart")
}
