package rds

import (
	"fmt"
	"os"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/spf13/viper"
)

const (
	RDS_CONFIG_KEY    = "rds"
	AURORA_CONFIG_KEY = "rds-aurora"
)

// For now, this suffices for both RDS and AuroraRDS.
type Config struct {
	AWSAccessKey          string `mapstructure:"AWS_ACCESS_KEY_ID"`
	AWSSecretAccessKey    string `mapstructure:"AWS_SECRET_ACCESS_KEY"`
	AWSRegion             string `mapstructure:"AWS_REGION" validate:"required"`
	RDSDatabaseIdentifier string `mapstructure:"RDS_DATABASE_IDENTIFIER" validate:"required"`
	RDSParameterGroupName string `mapstructure:"RDS_PARAMETER_GROUP_NAME" validate:"required"`
}

func ConfigFromViper(keyValue string) (Config, error) {
	// Create a new Viper instance for Aurora configuration if the sub-config doesn't exist
	dbtuneConfig := viper.Sub(keyValue)
	if dbtuneConfig == nil {
		// If the section doesn't exist in the config file, create a new Viper instance
		dbtuneConfig = viper.New()
	}

	// Bind environment variables directly to keys (not nested paths)
	dbtuneConfig.BindEnv("AWS_ACCESS_KEY_ID", "DBT_AWS_ACCESS_KEY_ID")
	dbtuneConfig.BindEnv("AWS_SECRET_ACCESS_KEY", "DBT_AWS_SECRET_ACCESS_KEY")
	dbtuneConfig.BindEnv("AWS_REGION", "DBT_AWS_REGION")
	dbtuneConfig.BindEnv("RDS_PARAMETER_GROUP_NAME", "DBT_RDS_PARAMETER_GROUP_NAME")
	dbtuneConfig.BindEnv("RDS_DATABASE_IDENTIFIER", "DBT_RDS_DATABASE_IDENTIFIER")

	// Bind also global environment variables as fallback for AWS credentials
	dbtuneConfig.BindEnv("AWS_ACCESS_KEY_ID")
	dbtuneConfig.BindEnv("AWS_SECRET_ACCESS_KEY")
	dbtuneConfig.BindEnv("AWS_REGION")

	var rdsConfig Config
	err := dbtuneConfig.Unmarshal(&rdsConfig)
	if err != nil {
		return Config{}, fmt.Errorf("unable to decode into struct: %v", err)
	}

	// Validate required configuration
	err = utils.ValidateStruct(&rdsConfig)
	if err != nil {
		return Config{}, err
	}

	// Validate non-credential required configuration
	if rdsConfig.AWSRegion == "" {
		return Config{}, fmt.Errorf("AWS_REGION is required")
	}
	if rdsConfig.RDSDatabaseIdentifier == "" {
		return Config{}, fmt.Errorf("RDS_DATABASE_IDENTIFIER is required")
	}
	if rdsConfig.RDSParameterGroupName == "" {
		return Config{}, fmt.Errorf("RDS_PARAMETER_GROUP_NAME is required")
	}

	return rdsConfig, nil
}

type DetectedConfig string

const (
	// Aurora was detected from the configuration.
	Aurora DetectedConfig = "aurora"
	// RDS was detected from the configuration.
	RDS DetectedConfig = "rds"
	// If we detect the environment variables are set, these are re-used for both
	// RDS and Aurora, hence it's ambiguous and we require the user to specify.
	// This does not happen for the config file as the sub-heading in the config file is
	// different.
	Ambiguous DetectedConfig = "ambiguous"
	// Neither was detected, it's something else.
	None DetectedConfig = "none"
)

func DetectConfigFromConfigFile() DetectedConfig {
	if viper.Sub(RDS_CONFIG_KEY) != nil {
		return RDS
	}
	if viper.Sub(AURORA_CONFIG_KEY) != nil {
		return Aurora
	}
	return None
}

func DetectConfigFromEnv() bool {
	envKeysToDetect := []string{
		"DBT_RDS_PARAMETER_GROUP_NAME",
		"DBT_RDS_DATABASE_IDENTIFIER",
		"DBT_AWS_ACCESS_KEY_ID",
		"DBT_AWS_SECRET_ACCESS_KEY",
		"DBT_AWS_REGION",
	}
	for _, envKey := range envKeysToDetect {
		if os.Getenv(envKey) != "" {
			return true
		}
	}
	return false
}
