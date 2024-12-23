package adapters

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"

	"example.com/dbtune-agent/internal/collectors"
	"example.com/dbtune-agent/internal/parameters"
	"example.com/dbtune-agent/internal/utils"
	"github.com/hashicorp/go-retryablehttp"
	pgPool "github.com/jackc/pgx/v5/pgxpool"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/spf13/viper"
)

type DefaultPostgreSQLAdapter struct {
	utils.CommonAgent
	pgDriver *pgPool.Pool
}

func CreateDefaultPostgreSQLAdapter(url string, agentID string, instanceID string) *DefaultPostgreSQLAdapter {
	dbtuneConfig := viper.Sub("postgresql")
	dbtuneConfig.BindEnv("connection_url", "DBT_POSTGRESQL_CONNECTION_URL")
	dbURL := dbtuneConfig.GetString("connection_url")
	if dbURL == "" {
		panic("DBT_POSTGRESQL_CONNECTION_URL is not set")
	}

	dbpool, err := pgPool.New(context.Background(), dbURL)
	if err != nil {
		fmt.Println("Could not create PG driver")
		panic(err)
	}

	commonAgent := utils.CreateCommonAgent(url, agentID, instanceID)

	commonAgent.MetricsState = utils.MetricsState{
		Collectors: DefaultCollectors(dbpool),
		Cache:      utils.Caches{},
	}

	return &DefaultPostgreSQLAdapter{
		CommonAgent: *commonAgent,
		pgDriver:    dbpool,
	}
}

// Example on how to override one of the shared methods
//func (adapter *DefaultPostgreSQLAdapter) SendHeartbeat() error {
//	adapter.logger.Infof("Overriding shared SendHeartbeat logic")
//	return nil
//}

func DefaultCollectors(driver *pgPool.Pool) []utils.MetricCollector {
	return []utils.MetricCollector{
		{
			Key:        "query_runtime",
			MetricType: "float",
			Collector:  collectors.QueryRuntime(driver),
		},
		{
			Key:        "transactions_per_second",
			MetricType: "int",
			Collector:  collectors.TransactionsPerSecond(driver),
		},
		{
			Key:        "active_connections",
			MetricType: "int",
			Collector:  collectors.ActiveConnections(driver),
		},
	}
}

func (adapter *DefaultPostgreSQLAdapter) GetMetrics() ([]utils.FlatValue, error) {
	adapter.Logger.Println("Collecting metrics")
	// Cleanup metrics from the previous heartbeat
	adapter.MetricsState.Metrics = []utils.FlatValue{}

	for _, collector := range adapter.MetricsState.Collectors {
		adapter.Logger.Println("Collector", collector.Key)
		err := collector.Collector(&adapter.MetricsState)
		if err != nil {
			adapter.Logger.Error("Error in collector", collector.Key)
			adapter.Logger.Error(err)
		}
	}

	adapter.Logger.Debug("Metrics collected", adapter.MetricsState.Metrics)

	return adapter.MetricsState.Metrics, nil
}

func (adapter *DefaultPostgreSQLAdapter) SendMetrics(metrics []utils.FlatValue) error {
	adapter.Logger.Println("Sending metrics to server")

	formattedMetrics := utils.FormatMetrics(metrics)

	jsonData, err := json.Marshal(formattedMetrics)
	if err != nil {
		return err
	}

	adapter.Logger.Debug("Metrics body payload")
	adapter.Logger.Debug(string(jsonData))

	resp, err := adapter.APIClient.Post(adapter.ServerURLs.PostMetrics(), "application/json", jsonData)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 {
		return errors.New(fmt.Sprintf("Failed to send metrics, code: %d", resp.StatusCode))
	}

	return nil
}

func (adapter *DefaultPostgreSQLAdapter) GetSystemInfo() ([]utils.FlatValue, error) {
	adapter.Logger.Println("Collecting system info")

	var systemInfo []utils.FlatValue

	pgVersion, err := collectors.PGVersion(adapter.pgDriver)
	if err != nil {
		return nil, err
	}

	memoryInfo, err := mem.VirtualMemory()
	if err != nil {
		return nil, err
	}

	totalMemory, err := utils.NewMetric("hardware_info_total_memory", memoryInfo.Total, utils.Int)
	if err != nil {
		return nil, err
	}

	hostInfo, err := host.Info()
	if err != nil {
		return nil, err
	}

	cpuInfo, err := cpu.Info()
	if err != nil {
		return nil, err
	}

	version, _ := utils.NewMetric("system_info_pg_version", pgVersion, utils.String)
	hostOS, _ := utils.NewMetric("system_info_OS", hostInfo.OS, utils.String)
	platform, _ := utils.NewMetric("system_info_platform", hostInfo.Platform, utils.String)
	platformVersion, _ := utils.NewMetric("system_info_platform_version", hostInfo.PlatformVersion, utils.String)
	systemInfo = append(systemInfo, version, totalMemory, hostOS, platformVersion, platform)

	if len(cpuInfo) > 0 {
		noCPUs, _ := utils.NewMetric("hardware_info_no_cpu", cpuInfo[0].Cores, utils.Int)
		systemInfo = append(systemInfo, noCPUs)
	}

	return systemInfo, nil
}

func (adapter *DefaultPostgreSQLAdapter) SendSystemInfo(systemInfo []utils.FlatValue) error {
	adapter.Logger.Println("Sending system info to server")

	formattedMetrics := utils.FormatSystemInfo(systemInfo)

	adapter.Logger.Info(formattedMetrics)
	jsonData, err := json.Marshal(formattedMetrics)
	if err != nil {
		return err
	}

	adapter.Logger.Debug("System info body payload")
	adapter.Logger.Debug(string(jsonData))

	req, _ := retryablehttp.NewRequest("PUT", adapter.ServerURLs.PostSystemInfo(), bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")

	resp, err := adapter.APIClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 && resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		adapter.Logger.Error("Failed to send system info. Response body: ", string(body))
		return errors.New(fmt.Sprintf("Failed to send syste info, code: %d", resp.StatusCode))
	}

	return nil
}

func (adapter *DefaultPostgreSQLAdapter) GetActiveConfig() (utils.ConfigArraySchema, error) {

	var configRows utils.ConfigArraySchema
	rows, err := adapter.pgDriver.Query(context.Background(), "SELECT name, setting, unit, vartype, context from pg_settings;")
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var row utils.PGConfigRow
		err := rows.Scan(&row.Name, &row.Setting, &row.Unit, &row.Vartype, &row.Context)
		if err != nil {
			adapter.Logger.Error("Error scanning row", err)
			continue
		}
		configRows = append(configRows, row)
	}

	return configRows, nil
}

func (adapter *DefaultPostgreSQLAdapter) SendActiveConfig(config utils.ConfigArraySchema) error {
	adapter.Logger.Println("Sending active configuration to server")

	type Payload struct {
		Config utils.ConfigArraySchema `json:"config"`
	}

	jsonData, err := json.Marshal(Payload{Config: config})
	if err != nil {
		return err
	}

	adapter.Logger.Debug("Configuration info body payload")
	//adapter.Logger.Debug(string(jsonData))

	resp, err := adapter.APIClient.Post(adapter.ServerURLs.PostActiveConfig(), "application/json", jsonData)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 && resp.StatusCode != 201 {
		body, _ := io.ReadAll(resp.Body)
		adapter.Logger.Error("Failed to send configuration info. Response body: ", string(body))
		return errors.New(fmt.Sprintf("Failed to send config info, code: %d", resp.StatusCode))
	}

	return nil
}

func (adapter *DefaultPostgreSQLAdapter) GetProposedConfig() (*utils.ProposedConfigResponse, error) {
	adapter.Logger.Println("Fetching proposed configurations")

	resp, err := adapter.APIClient.Get(adapter.ServerURLs.GetKnobRecommendations())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var proposedConfig []utils.ProposedConfigResponse

	if err := json.Unmarshal(body, &proposedConfig); err != nil {
		return nil, err
	}

	if len(proposedConfig) > 0 {
		return &proposedConfig[0], nil
	} else {
		return nil, nil
	}
}

func (adapter *DefaultPostgreSQLAdapter) ApplyConfig(proposedConfig *utils.ProposedConfigResponse) error {
	adapter.Logger.Infof("Applying Config: %s", proposedConfig.KnobApplication)

	if proposedConfig.KnobApplication == "restart" {
		panic("Restart application not implemented")
	}

	// Apply the configuration with ALTER
	for _, knob := range proposedConfig.KnobsOverrides {
		knobConfig, err := parameters.FindRecommendedKnob(proposedConfig.Config, knob)
		if err != nil {
			return err
		}

		// We make the assumption every setting is a number parsed as float
		query := fmt.Sprintf(`ALTER SYSTEM SET "%s" = %s;`, knobConfig.Name, strconv.FormatFloat(knobConfig.Setting.(float64), 'f', -1, 64))
		adapter.Logger.Debugf(`Executing: %s`, query)

		_, err = adapter.pgDriver.Exec(context.Background(), query)
		if err != nil {
			return err
		}
	}

	// Reload database when everything is applied
	_, err := adapter.pgDriver.Exec(context.Background(), "SELECT pg_reload_conf();")
	if err != nil {
		return err
	}

	return nil
}
