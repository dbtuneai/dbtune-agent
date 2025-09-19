package cloudsql

import (
	"os"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/spf13/viper"
)

const DEFAULT_CONFIG_KEY = "cloudsql"

type Config struct {
	ProjectID    string `mapstructure:"project_id" validate:"required"`
	DatabaseName string `mapstructure:"database_name" validate:"required"`
}

func ConfigFromViper() (Config, error) {
	dbtuneConfig := viper.Sub(DEFAULT_CONFIG_KEY)

	if dbtuneConfig == nil {
		dbtuneConfig = viper.New()
	}

	dbtuneConfig.BindEnv("project_id", "DBT_GCP_PROJECT_ID")
	dbtuneConfig.BindEnv("database_name", "DBT_GCP_DATABASE_NAME")

	var config Config

	err := dbtuneConfig.Unmarshal(&config)
	if err != nil {
		return Config{}, err
	}

	err = utils.ValidateStruct(&config)
	if err != nil {
		return Config{}, err
	}

	return config, nil
}

func DetectConfigFromEnv() bool {
	envKeysToDetect := []string{
		"DBT_GCP_PROJECT_ID",
		"DBT_GCP_DATABSE_NAME",
	}
	for _, envKey := range envKeysToDetect {
		if os.Getenv(envKey) != "" {
			return true
		}
	}
	return false
}

func DetectConfigFromConfigFile() bool {
	return viper.Sub(DEFAULT_CONFIG_KEY) != nil
}
