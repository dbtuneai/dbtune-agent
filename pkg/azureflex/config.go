package azureflex

import (
	"os"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/spf13/viper"
)

const DEFAULT_CONFIG_KEY = "azure_flex"

type Config struct {
	SubscriptionID    string `mapstructure:"subscription_id" validate:"required"`
	ResourceGroupName string `mapstructure:"resource_group_name" validate:"required"`
	ServerName        string `mapstructure:"server_name" validate:"required"`
}

func ConfigFromViper() (Config, error) {
	dbtuneConfig := viper.Sub(DEFAULT_CONFIG_KEY)

	if dbtuneConfig == nil {
		dbtuneConfig = viper.New()
	}

	dbtuneConfig.BindEnv("subscription_id", "DBT_AZURE_FLEX_SUBSCRIPTION_ID")
	dbtuneConfig.BindEnv("resource_group_name", "DBT_AZURE_FLEX_RESOURCE_GROUP_NAME")
	dbtuneConfig.BindEnv("server_name", "DBT_AZURE_FLEX_SERVER_NAME")

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
		"DBT_AZURE_FLEX_SUBSCRIPTION_ID",
		"DBT_AZURE_FLEX_RESOURCE_GROUP_NAME",
		"DBT_AZURE_FLEX_SERVER_NAME",
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
