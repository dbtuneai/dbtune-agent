package pg

import (
	"fmt"
	"os"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/spf13/viper"
)

const (
	DEFAULT_CONFIG_KEY = "postgresql"
)

type Config struct {
	ConnectionURL          string `mapstructure:"connection_url" validate:"required"`
	ServiceName            string `mapstructure:"service_name"` // TODO(eddie): Should be moved under pgprem, as it doesn't apply to all other PG providers
	IncludeQueries         bool   `mapstructure:"include_queries"`
	MaximumQueryTextLength int    `mapstructure:"maximum_query_text_length"`
	AllowRestart           bool   `mapstructure:"allow_restart"`
}

func ConfigFromViper(key *string) (Config, error) {
	var keyValue string
	if key == nil {
		keyValue = DEFAULT_CONFIG_KEY
	} else {
		keyValue = *key
	}

	// Try both "postgres" and "postgresql" keys for backward compatibility
	dbtuneConfig := viper.Sub(keyValue)
	if dbtuneConfig == nil && keyValue == DEFAULT_CONFIG_KEY {
		// Try alternate key "postgres" for backward compatibility
		dbtuneConfig = viper.Sub("postgres")
	}

	if dbtuneConfig == nil {
		// If no sub-config found, use the global viper instance
		// This allows environment variables to work properly
		dbtuneConfig = viper.GetViper()
	}

	dbtuneConfig.BindEnv("connection_url", "DBT_POSTGRESQL_CONNECTION_URL")
	dbtuneConfig.BindEnv("service_name", "DBT_POSTGRESQL_SERVICE_NAME")
	dbtuneConfig.BindEnv("include_queries", "DBT_POSTGRESQL_INCLUDE_QUERIES")
	dbtuneConfig.BindEnv("maximum_query_text_length", "DBT_POSTGRESQL_MAXIMUM_QUERY_TEXT_LENGTH")
	dbtuneConfig.BindEnv("allow_restart", "DBT_POSTGRESQL_ALLOW_RESTART")

	// Set defaults
	dbtuneConfig.SetDefault("include_queries", true)
	dbtuneConfig.SetDefault("maximum_query_text_length", 50_000)
	dbtuneConfig.SetDefault("allow_restart", false)

	var pgConfig Config

	// If using global viper, manually extract the nested config
	if viper.Sub(keyValue) == nil && viper.Sub("postgres") == nil {
		// No nested config found, try to unmarshal from environment variables via global viper
		err := dbtuneConfig.Unmarshal(&pgConfig)
		if err != nil {
			return Config{}, fmt.Errorf("unable to decode into struct, %v", err)
		}
	} else {
		err := dbtuneConfig.Unmarshal(&pgConfig)
		if err != nil {
			return Config{}, fmt.Errorf("unable to decode into struct, %v", err)
		}
	}

	err := utils.ValidateStruct(&pgConfig)
	if err != nil {
		return Config{}, err
	}
	return pgConfig, nil
}

func DetectConfigFromConfigFile() bool {
	config := viper.Sub(DEFAULT_CONFIG_KEY)
	if config != nil {
		return true
	}
	// Check alternate key "postgres" for backward compatibility
	config = viper.Sub("postgres")
	return config != nil
}

func DetectConfigFromEnv() bool {
	envKeysToDetect := []string{
		"DBT_POSTGRESQL_CONNECTION_URL",
		// NOTE: We don't require the service name to be set,
		"DBT_POSTGRESQL_SERVICE_NAME",
	}
	for _, envKey := range envKeysToDetect {
		if os.Getenv(envKey) != "" {
			return true
		}
	}

	return false
}
