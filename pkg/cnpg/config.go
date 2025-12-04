package cnpg

import (
	"os"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/spf13/viper"
)

const DEFAULT_CONFIG_KEY = "cnpg"
const DEFAULT_CONTAINER_NAME = "postgres"

type Config struct {
	Namespace      string `mapstructure:"namespace" validate:"required"`
	KubeconfigPath string `mapstructure:"kubeconfig_path"`
	ClusterName    string `mapstructure:"cluster_name" validate:"required"` // Required: CNPG cluster name for dynamic pod discovery
	PodName        string `mapstructure:"pod_name"`                         // Optional: if not specified, auto-discovers primary pod
	ContainerName  string `mapstructure:"container_name"`                   // Optional: defaults to "postgres" if not specified
}

func ConfigFromViper() (Config, error) {
	dbtuneConfig := viper.Sub(DEFAULT_CONFIG_KEY)

	if dbtuneConfig == nil {
		dbtuneConfig = viper.New()
	}

	dbtuneConfig.BindEnv("namespace", "DBT_CNPG_NAMESPACE")
	dbtuneConfig.BindEnv("kubeconfig_path", "DBT_CNPG_KUBECONFIG_PATH")
	dbtuneConfig.BindEnv("cluster_name", "DBT_CNPG_CLUSTER_NAME")
	dbtuneConfig.BindEnv("pod_name", "DBT_CNPG_POD_NAME")
	dbtuneConfig.BindEnv("container_name", "DBT_CNPG_CONTAINER_NAME")
	// Default to "postgres" which is the standard container name in CNPG pods
	dbtuneConfig.SetDefault("container_name", "postgres")

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
		"DBT_CNPG_KUBECONFIG_PATH",
		"DBT_CNPG_NAMESPACE",
		"DBT_CNPG_POD_NAME",
		"DBT_CNPG_CONTAINER_NAME",
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
