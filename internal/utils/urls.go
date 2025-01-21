package utils

import (
	"fmt"

	"github.com/spf13/viper"
)

const (
	DefaultServerURL = "https://api.dbtune.com"
)

type ServerURLs struct {
	ServerUrl string `mapstructure:"server_url"` // Optional
	ApiKey    string `mapstructure:"api_key"`
	DbID      string `mapstructure:"database_id"`
}

func CreateServerURLs() (ServerURLs, error) {
	dbtuneConfig := viper.Sub("dbtune")
	dbtuneConfig.BindEnv("server_url", "DBT_DBTUNE_SERVER_URL")
	dbtuneConfig.SetDefault("server_url", DefaultServerURL)
	dbtuneConfig.BindEnv("api_key", "DBT_DBTUNE_API_KEY")
	dbtuneConfig.BindEnv("database_id", "DBT_DBTUNE_DATABASE_ID")

	if dbtuneConfig == nil {
		return ServerURLs{}, fmt.Errorf("dbtune configuration not found")
	}

	var servTest ServerURLs
	err := dbtuneConfig.Unmarshal(&servTest)
	if err != nil {
		return ServerURLs{}, fmt.Errorf("unable to decode into struct, %v", err)
	}

	if err := servTest.validate(); err != nil {
		return ServerURLs{}, fmt.Errorf("error validating configuration: %s", err)
	}

	return ServerURLs{
		ServerUrl: servTest.ServerUrl,
		ApiKey:    servTest.ApiKey,
		DbID:      servTest.DbID,
	}, nil
}

func (s ServerURLs) validate() error {
	if s.ServerUrl == "" {
		return fmt.Errorf("server_url is required")
	}
	if s.ApiKey == "" {
		return fmt.Errorf("api_key is required")
	}
	if s.DbID == "" {
		return fmt.Errorf("database_id is required")
	}
	return nil
}

// PostHeartbeat generates the URL for posting a heartbeat.
func (s ServerURLs) PostHeartbeat() string {
	return fmt.Sprintf("%s/api/v1/databases/%s/agents/heartbeat", s.ServerUrl, s.DbID)
}

// PostSystemInfo generates the URL for posting system info.
func (s ServerURLs) PostSystemInfo() string {
	return fmt.Sprintf("%s/api/v1/databases/%s/system-info", s.ServerUrl, s.DbID)
}

// PostMetrics generates the URL for posting metrics.
func (s ServerURLs) PostMetrics() string {
	return fmt.Sprintf("%s/api/v1/databases/%s/post-metrics", s.ServerUrl, s.DbID)
}

// PostActiveConfig generates the URL for posting active configurations.
func (s ServerURLs) PostActiveConfig() string {
	return fmt.Sprintf("%s/api/v1/databases/%s/configurations", s.ServerUrl, s.DbID)
}

// GetKnobRecommendations generates the URL for getting knob recommendations.
func (s ServerURLs) GetKnobRecommendations() string {
	return fmt.Sprintf("%s/api/v1/databases/%s/configurations?status=recommended", s.ServerUrl, s.DbID)
}

// PostGuardrailSignal generates the URL for posting a guardrail signal.
func (s ServerURLs) PostGuardrailSignal() string {
	return fmt.Sprintf("%s/api/v1/databases/%s/guardrail", s.ServerUrl, s.DbID)
}
