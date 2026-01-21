package docker

import (
	"fmt"
	"os"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/spf13/viper"
)

const (
	DEFAULT_CONFIG_KEY = "docker"
)

type Config struct {
	ContainerName string `mapstructure:"container_name" validate:"required"`
	SwarmMode     bool   `mapstructure:"swarm_mode"`
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

	dbtuneConfig.BindEnv("container_name", "DBT_DOCKER_CONTAINER_NAME")
	dbtuneConfig.BindEnv("swarm_mode", "DBT_DOCKER_SWARM_MODE")

	var dockerConfig Config
	err := dbtuneConfig.Unmarshal(&dockerConfig)
	if err != nil {
		return Config{}, fmt.Errorf("unable to decode into struct, %v", err)
	}

	err = utils.ValidateStruct(&dockerConfig)
	if err != nil {
		return Config{}, err
	}
	return dockerConfig, nil
}

func DetectConfigFromConfigFile() bool {
	return viper.Sub(DEFAULT_CONFIG_KEY) != nil
}

func DetectConfigFromEnv() bool {
	envKeysToDetect := []string{
		"DBT_DOCKER_CONTAINER_NAME",
		"DBT_DOCKER_SWARM_MODE",
	}

	for _, envKey := range envKeysToDetect {
		if os.Getenv(envKey) != "" {
			return true
		}
	}
	return false
}
