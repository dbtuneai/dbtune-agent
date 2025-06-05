package guardrails

import (
	"fmt"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/spf13/viper"
)

type Config struct {
	MemoryThreshold float64 `mapstructure:"memory_threshold" validate:"gte=1,lte=100"`
}

const (
	DEFAULT_CONFIG_KEY       = "guardrail_settings"
	MEMORY_THRESHOLD_DEFAULT = 90
)

func ConfigFromViper(key *string) (Config, error) {
	var keyValue string
	if key == nil {
		keyValue = DEFAULT_CONFIG_KEY
	} else {
		keyValue = *key
	}

	settingConfig := viper.Sub(keyValue)
	if settingConfig == nil {
		settingConfig = viper.New()
	}

	settingConfig.BindEnv("memory_threshold", "DBT_MEMORY_THRESHOLD")
	settingConfig.SetDefault("memory_threshold", MEMORY_THRESHOLD_DEFAULT)

	var settings Config
	err := settingConfig.Unmarshal(&settings)
	if err != nil {
		return Config{}, fmt.Errorf("unable to decode into struct, %v", err)
	}

	err = utils.ValidateStruct(&settings)
	return settings, err
}
