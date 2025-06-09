package aiven

import (
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/spf13/viper"
)

const (
	DEFAULT_METRIC_RESOLUTION_SECONDS = 30
	DEFAULT_CONFIG_KEY                = "aiven"
)

type Config struct {
	APIToken         string        `mapstructure:"AIVEN_API_TOKEN" validate:"required"`
	ProjectName      string        `mapstructure:"AIVEN_PROJECT_NAME" validate:"required"`
	ServiceName      string        `mapstructure:"AIVEN_SERVICE_NAME" validate:"required"`
	MetricResolution time.Duration `mapstructure:"metric_resolution_seconds" validate:"required"`
	// NOTE: If specified, we are able to use the
	// session refresh hack. Not documented.
	DatabaseName string `mapstructure:"database_name"`
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

	// Bind requirement variables
	dbtuneConfig.BindEnv("AIVEN_API_TOKEN", "DBT_AIVEN_API_TOKEN")
	dbtuneConfig.BindEnv("AIVEN_PROJECT_NAME", "DBT_AIVEN_PROJECT_NAME")
	dbtuneConfig.BindEnv("AIVEN_SERVICE_NAME", "DBT_AIVEN_SERVICE_NAME")

	// This parameter is required to activate our hack to
	// force session restarts. We do not document this.
	// Ideally we can remove this once we get more support from
	// Aiven's API for `random_page_cost`, `seq_page_cost` and
	// `effective_io_concurrency`.
	dbtuneConfig.BindEnv("AIVEN_DATABASE_NAME", "DBT_AIVEN_DATABASE_NAME")

	// These are some lower level configuration variables we do not document.
	// They control some behaviours of the agent w.r.t guardrails and define
	// the resolution at which Aiven will give us metrics. This resolution
	// prevents us from spamming their API where we do not get any new information.
	dbtuneConfig.SetDefault("metric_resolution_seconds", DEFAULT_METRIC_RESOLUTION_SECONDS)
	dbtuneConfig.BindEnv("AIVEN_METRIC_RESOLUTION_SECONDS", "DBT_AIVEN_METRIC_RESOLUTION_SECONDS")
	var config Config
	err := dbtuneConfig.Unmarshal(&config)
	if err != nil {
		return Config{}, fmt.Errorf("unable to decode into struct: %v", err)
	}

	err = utils.ValidateStruct(&config)
	if err != nil {
		return Config{}, err
	}

	// Since we are specifying in units of seconds, but a raw int such as 30
	// is interpreted as nanoseconds, we need to convert it
	config.MetricResolution = time.Duration(config.MetricResolution) * time.Second
	return config, nil
}
