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

	if dbtuneConfig == nil {
		return ServerURLs{}, fmt.Errorf("dbtune configuration not found")
	}

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

// PostHeartbeat generates the URL for posting a heartbeat.
func (s ServerURLs) PostHeartbeat() string {
	return fmt.Sprintf("%s/api/v1/agent/heartbeat?uuid=%s", s.ServerUrl, s.DbID)
}

// PostSystemInfo generates the URL for posting system info.
func (s ServerURLs) PostSystemInfo() string {
	return fmt.Sprintf("%s/api/v1/agent/system-info?uuid=%s", s.ServerUrl, s.DbID)
}

// PostMetrics generates the URL for posting metrics.
func (s ServerURLs) PostMetrics() string {
	return fmt.Sprintf("%s/api/v1/agent/metrics?uuid=%s", s.ServerUrl, s.DbID)
}

// PostActiveConfig generates the URL for posting active configurations.
func (s ServerURLs) PostActiveConfig() string {
	return fmt.Sprintf("%s/api/v1/agent/configurations?uuid=%s", s.ServerUrl, s.DbID)
}

// PostDDL generates the URL for posting DDL schema data.
func (s ServerURLs) PostDDL() string {
	return fmt.Sprintf("%s/api/v1/agent/ddl?uuid=%s", s.ServerUrl, s.DbID)
}

// PostPgStatistic generates the URL for posting pg_statistic data.
func (s ServerURLs) PostPgStatistic() string {
	return fmt.Sprintf("%s/api/v1/agent/pg_statistic?uuid=%s", s.ServerUrl, s.DbID)
}

// PostPgStatUserTables generates the URL for posting pg_stat_user_tables data.
func (s ServerURLs) PostPgStatUserTables() string {
	return fmt.Sprintf("%s/api/v1/agent/pg_stat_user_tables?uuid=%s", s.ServerUrl, s.DbID)
}

// PostPgClass generates the URL for posting pg_class data.
func (s ServerURLs) PostPgClass() string {
	return fmt.Sprintf("%s/api/v1/agent/pg_class?uuid=%s", s.ServerUrl, s.DbID)
}

func (s ServerURLs) PostPgStatActivity() string {
	return fmt.Sprintf("%s/api/v1/agent/pg_stat_activity?uuid=%s", s.ServerUrl, s.DbID)
}

func (s ServerURLs) PostPgStatDatabaseAll() string {
	return fmt.Sprintf("%s/api/v1/agent/pg_stat_database?uuid=%s", s.ServerUrl, s.DbID)
}

func (s ServerURLs) PostPgStatDatabaseConflicts() string {
	return fmt.Sprintf("%s/api/v1/agent/pg_stat_database_conflicts?uuid=%s", s.ServerUrl, s.DbID)
}

func (s ServerURLs) PostPgStatArchiver() string {
	return fmt.Sprintf("%s/api/v1/agent/pg_stat_archiver?uuid=%s", s.ServerUrl, s.DbID)
}

func (s ServerURLs) PostPgStatBgwriterAll() string {
	return fmt.Sprintf("%s/api/v1/agent/pg_stat_bgwriter?uuid=%s", s.ServerUrl, s.DbID)
}

func (s ServerURLs) PostPgStatCheckpointerAll() string {
	return fmt.Sprintf("%s/api/v1/agent/pg_stat_checkpointer?uuid=%s", s.ServerUrl, s.DbID)
}

func (s ServerURLs) PostPgStatWalAll() string {
	return fmt.Sprintf("%s/api/v1/agent/pg_stat_wal?uuid=%s", s.ServerUrl, s.DbID)
}

func (s ServerURLs) PostPgStatIO() string {
	return fmt.Sprintf("%s/api/v1/agent/pg_stat_io?uuid=%s", s.ServerUrl, s.DbID)
}

func (s ServerURLs) PostPgStatReplication() string {
	return fmt.Sprintf("%s/api/v1/agent/pg_stat_replication?uuid=%s", s.ServerUrl, s.DbID)
}

func (s ServerURLs) PostPgStatReplicationSlots() string {
	return fmt.Sprintf("%s/api/v1/agent/pg_stat_replication_slots?uuid=%s", s.ServerUrl, s.DbID)
}

func (s ServerURLs) PostPgStatSlru() string {
	return fmt.Sprintf("%s/api/v1/agent/pg_stat_slru?uuid=%s", s.ServerUrl, s.DbID)
}

func (s ServerURLs) PostPgStatUserIndexes() string {
	return fmt.Sprintf("%s/api/v1/agent/pg_stat_user_indexes?uuid=%s", s.ServerUrl, s.DbID)
}

func (s ServerURLs) PostPgStatioUserTables() string {
	return fmt.Sprintf("%s/api/v1/agent/pg_statio_user_tables?uuid=%s", s.ServerUrl, s.DbID)
}

func (s ServerURLs) PostPgStatioUserIndexes() string {
	return fmt.Sprintf("%s/api/v1/agent/pg_statio_user_indexes?uuid=%s", s.ServerUrl, s.DbID)
}

func (s ServerURLs) PostPgStatUserFunctions() string {
	return fmt.Sprintf("%s/api/v1/agent/pg_stat_user_functions?uuid=%s", s.ServerUrl, s.DbID)
}

// GetKnobRecommendations generates the URL for getting knob recommendations.
func (s ServerURLs) GetKnobRecommendations() string {
	return fmt.Sprintf("%s/api/v1/agent/configurations?uuid=%s&status=recommended", s.ServerUrl, s.DbID)
}

// PostGuardrailSignal generates the URL for posting a guardrail signal.
func (s ServerURLs) PostGuardrailSignal() string {
	return fmt.Sprintf("%s/api/v1/agent/guardrails?uuid=%s", s.ServerUrl, s.DbID)
}

// PostError generates the URL for posting errors.
func (s ServerURLs) PostError() string {
	return fmt.Sprintf("%s/api/v1/agent/log-entries?uuid=%s", s.ServerUrl, s.DbID)
}
