package dbtune

import (
	"fmt"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/spf13/viper"
)

const (
	DefaultServerURL = "https://app.dbtune.com"
)

type ServerURLs struct {
	ServerUrl string `mapstructure:"server_url" validate:"required,url"`
	ApiKey    string `mapstructure:"api_key" validate:"required,uuid"`
	DbID      string `mapstructure:"database_id" validate:"required,uuid"`
}

func CreateServerURLs() (ServerURLs, error) {
	dbtuneConfig := viper.Sub("dbtune")
	if dbtuneConfig == nil {
		// If the section doesn't exist in the config file, create a new Viper instance
		dbtuneConfig = viper.New()
	}

	_ = dbtuneConfig.BindEnv("server_url", "DBT_DBTUNE_SERVER_URL")
	dbtuneConfig.SetDefault("server_url", DefaultServerURL)
	_ = dbtuneConfig.BindEnv("api_key", "DBT_DBTUNE_API_KEY")
	_ = dbtuneConfig.BindEnv("database_id", "DBT_DBTUNE_DATABASE_ID")

	var servTest ServerURLs
	err := dbtuneConfig.Unmarshal(&servTest)
	if err != nil {
		return ServerURLs{}, fmt.Errorf("unable to decode into struct, %w", err)
	}

	err = utils.ValidateStruct(&servTest)
	if err != nil {
		return ServerURLs{}, err
	}

	return ServerURLs{
		ServerUrl: servTest.ServerUrl,
		ApiKey:    servTest.ApiKey,
		DbID:      servTest.DbID,
	}, nil
}

// AgentURL builds a URL for the given agent API path segment.
// e.g. AgentURL("heartbeat") => "https://app.dbtune.com/api/v1/agent/heartbeat?uuid=<db-id>"
func (s ServerURLs) AgentURL(path string) string {
	return fmt.Sprintf("%s/api/v1/agent/%s?uuid=%s", s.ServerUrl, path, s.DbID)
}

// GetKnobRecommendations generates the URL for getting knob recommendations.
func (s ServerURLs) GetKnobRecommendations() string {
	return fmt.Sprintf("%s/api/v1/agent/configurations?uuid=%s&status=recommended", s.ServerUrl, s.DbID)
}
