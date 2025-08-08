package cloudsql

import (
	"github.com/spf13/viper"
)

type Config struct {
	ProjectID    string `mapstructure:"project_id"`
	DatabaseName string `mapstructure:"database_name"`
}

func ConfigFromViper() (Config, error) {
	dbtuneConfig := viper.Sub("cloudsql")

	var config Config

	err := dbtuneConfig.Unmarshal(&config)
	if err != nil {
		return Config{}, err
	}

	return config, nil
}
