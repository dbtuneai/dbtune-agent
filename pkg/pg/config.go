package pg

import (
	"fmt"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/spf13/viper"
)

const (
	DEFAULT_CONFIG_KEY = "postgresql"
)

type Config struct {
	ConnectionURL string `mapstructure:"connection_url" validate:"required"`
	ServiceName   string `mapstructure:"service_name"`
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
