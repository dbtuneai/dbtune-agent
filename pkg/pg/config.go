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
	ConnectionURL  string `mapstructure:"connection_url" validate:"required"`
	ServiceName    string `mapstructure:"service_name"`
	IncludeQueries bool   `mapstructure:"include_queries"`
}

func ConfigFromViper(key *string) (Config, error) {
	var keyValue string
	if key == nil {
		keyValue = DEFAULT_CONFIG_KEY
	} else {
		keyValue = *key
	}

	dbtuneConfig := viper.Sub(keyValue)
	if dbtuneConfig == nil {
		dbtuneConfig = viper.New()
	}

	dbtuneConfig.BindEnv("connection_url", "DBT_POSTGRESQL_CONNECTION_URL")
	dbtuneConfig.BindEnv("service_name", "DBT_POSTGRESQL_SERVICE_NAME")
	dbtuneConfig.BindEnv("include_queries", "DBT_POSTGRESQL_INCLUDE_QUERIES")

	var pgConfig Config
	err := dbtuneConfig.Unmarshal(&pgConfig)
	if err != nil {
		return Config{}, fmt.Errorf("unable to decode into struct, %v", err)
	}

	err = utils.ValidateStruct(&pgConfig)
	if err != nil {
		return Config{}, err
	}
	return pgConfig, nil
}

func DetectConfigFromConfigFile() bool {
	config := viper.Sub(DEFAULT_CONFIG_KEY)
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
