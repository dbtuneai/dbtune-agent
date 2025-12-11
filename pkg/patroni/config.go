package patroni

import (
	"os"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/spf13/viper"
)

const DEFAULT_CONFIG_KEY = "patroni"
const DEFAULT_API_PORT = 8008

// Config represents the configuration for a Patroni adapter
// Important: Each agent instance monitors ONE specific Patroni node, not the entire cluster
// Even though Patroni operates as an HA cluster, the agent connects to individual nodes
type Config struct {
	NodeName      string `mapstructure:"node_name" validate:"required"`       // Required: one agent monitors one specific Patroni node
	PatroniAPIURL string `mapstructure:"patroni_api_url" validate:"required"` // Required: Patroni REST API URL for this specific node (e.g., http://10.0.1.5:8008)
	ClusterName   string `mapstructure:"cluster_name"`                        // Optional: name of the Patroni cluster this node belongs to (informational)

}

func ConfigFromViper() (Config, error) {
	dbtuneConfig := viper.Sub(DEFAULT_CONFIG_KEY)

	if dbtuneConfig == nil {
		dbtuneConfig = viper.New()
	}

	// Bind environment variables
	dbtuneConfig.BindEnv("node_name", "DBT_PATRONI_NODE_NAME")
	dbtuneConfig.BindEnv("patroni_api_url", "DBT_PATRONI_API_URL")
	dbtuneConfig.BindEnv("cluster_name", "DBT_PATRONI_CLUSTER_NAME")
	dbtuneConfig.BindEnv("node_host", "DBT_PATRONI_NODE_HOST")
	dbtuneConfig.BindEnv("node_port", "DBT_PATRONI_NODE_PORT")

	// Set defaults
	dbtuneConfig.SetDefault("node_port", DEFAULT_API_PORT)

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
		"DBT_PATRONI_NODE_NAME",
		"DBT_PATRONI_API_URL",
		"DBT_PATRONI_NODE_HOST",
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
