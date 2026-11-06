package config

import (
	"fmt"
	"runtime"

	"github.com/spf13/viper"
)

const (
	PostgresConfigKey = "postgresql"
)

type Postgres struct {
	ConnectionURL     string `mapstructure:"connection_url" validate:"required"`
	ServiceName       string `mapstructure:"service_name"`        // TODO(eddie): Should be moved under pgprem, as it doesn't apply to all other PG providers
	RestartScriptPath string `mapstructure:"restart_script_path"` // When set, restarts execute this script directly (no shell) instead of `systemctl restart`. Must be an executable path with no arguments. Takes precedence over ServiceName.
	AllowRestart      bool   `mapstructure:"allow_restart"`
}

func PostgresFromViper(key *string) (Postgres, error) {
	var keyValue string
	if key == nil {
		keyValue = PostgresConfigKey
	} else {
		keyValue = *key
	}

	// Try both "postgres" and "postgresql" keys for backward compatibility
	dbtuneConfig := viper.Sub(keyValue)
	if dbtuneConfig == nil && keyValue == PostgresConfigKey {
		// Try alternate key "postgres" for backward compatibility
		dbtuneConfig = viper.Sub("postgres")
	}

	if dbtuneConfig == nil {
		// If no sub-config found, use the global viper instance
		// This allows environment variables to work properly
		dbtuneConfig = viper.GetViper()
	}

	_ = dbtuneConfig.BindEnv("connection_url", "DBT_POSTGRESQL_CONNECTION_URL")
	_ = dbtuneConfig.BindEnv("service_name", "DBT_POSTGRESQL_SERVICE_NAME")
	_ = dbtuneConfig.BindEnv("restart_script_path", "DBT_POSTGRESQL_RESTART_SCRIPT_PATH")
	_ = dbtuneConfig.BindEnv("allow_restart", "DBT_POSTGRESQL_ALLOW_RESTART")

	// Also bind on the global viper so dotted lookups like
	// viper.GetBool("postgresql.allow_restart") (used by IsRestartAllowed)
	// resolve the env var. BindEnv on a sub-viper does not propagate to the parent.
	_ = viper.BindEnv("postgresql.connection_url", "DBT_POSTGRESQL_CONNECTION_URL")
	_ = viper.BindEnv("postgresql.service_name", "DBT_POSTGRESQL_SERVICE_NAME")
	_ = viper.BindEnv("postgresql.restart_script_path", "DBT_POSTGRESQL_RESTART_SCRIPT_PATH")
	_ = viper.BindEnv("postgresql.allow_restart", "DBT_POSTGRESQL_ALLOW_RESTART")

	dbtuneConfig.SetDefault("allow_restart", false)

	var pgConfig Postgres

	// If using global viper, manually extract the nested config
	err := dbtuneConfig.Unmarshal(&pgConfig)
	if err != nil {
		return Postgres{}, fmt.Errorf("unable to decode into struct, %w", err)
	}

	err = ValidateStruct(&pgConfig)
	if err != nil {
		return Postgres{}, err
	}

	if pgConfig.AllowRestart && pgConfig.RestartScriptPath == "" && runtime.GOOS == "windows" {
		return Postgres{}, fmt.Errorf("postgresql.allow_restart is not supported on Windows: the agent restarts PostgreSQL via systemctl, which has no Windows equivalent yet. Set postgresql.restart_script_path (env: DBT_POSTGRESQL_RESTART_SCRIPT_PATH) to a custom restart script, or set postgresql.allow_restart=false (or unset DBT_POSTGRESQL_ALLOW_RESTART) to run the agent in monitoring/reload-only mode")
	}

	return pgConfig, nil
}

// IsRestartAllowed reports whether postgresql.allow_restart is set. It reads
// the global viper, so it sees DBT_POSTGRESQL_ALLOW_RESTART only after
// PostgresFromViper has bound it.
func IsRestartAllowed() bool {
	return viper.GetBool("postgresql.allow_restart")
}
