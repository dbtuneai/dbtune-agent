package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type Guardrails struct {
	MemoryThreshold float64 `mapstructure:"memory_threshold" validate:"gte=1,lte=100"`
}

const (
	GuardrailsConfigKey    = "guardrail_settings"
	MemoryThresholdDefault = 90
)

func GuardrailsFromViper(key *string) (Guardrails, error) {
	var keyValue string
	if key == nil {
		keyValue = GuardrailsConfigKey
	} else {
		keyValue = *key
	}

	settingConfig := viper.Sub(keyValue)
	if settingConfig == nil {
		settingConfig = viper.New()
	}

	_ = settingConfig.BindEnv("memory_threshold", "DBT_MEMORY_THRESHOLD")
	settingConfig.SetDefault("memory_threshold", MemoryThresholdDefault)

	var settings Guardrails
	err := settingConfig.Unmarshal(&settings)
	if err != nil {
		return Guardrails{}, fmt.Errorf("unable to decode into struct, %w", err)
	}

	err = ValidateStruct(&settings)
	return settings, err
}
