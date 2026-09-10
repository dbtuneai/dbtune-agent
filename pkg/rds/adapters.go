package rds

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	guardrails "github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/dbtuneai/agent/pkg/pg/collectorconfig"
	"github.com/dbtuneai/agent/pkg/pg/queries"
	"github.com/jackc/pgx/v5/pgxpool"
)

type RDSAdapter struct {
	agent.CommonAgent
	agent.CatalogGetter
	Config            Config
	GuardrailSettings guardrails.Config
	pgConfig          pg.Config
	State             State
	AWSClients        AWSClients
	PGDriver          *pgxpool.Pool
	PGVersion         string
}

func CreateRDSAdapterWithoutCollectors(configKey *string) (*RDSAdapter, error) {
	var keyValue string
	if configKey == nil {
		keyValue = RDS_CONFIG_KEY
	} else {
		keyValue = *configKey
	}

	var err error
	var config Config
	config, err = ConfigFromViper(keyValue)
	if err != nil {
		return nil, fmt.Errorf("failed to bind config from key %s: %w", keyValue, err)
	}

	guardrailSettings, err := guardrails.ConfigFromViper(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to validate settings for guardrails %w", err)
	}

	ctx := context.Background()

	// Create AWS config
	cfg, err := FetchAWSConfig(
		config.AWSAccessKey,
		config.AWSSecretAccessKey,
		config.AWSRegion,
		ctx,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to load AWS config: %w", err)
	}
	clients := NewAWSClients(cfg)

	// Check if RDS client can fetch the database instance correctly and the tokens work
	dbInfo, err := FetchDBInfo(config.RDSDatabaseIdentifier, &clients, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to describe database instance: %w", err)
	}

	pgConfig, err := pg.ConfigFromViper(nil)
	if err != nil {
		return nil, err
	}

	dbpool, err := pgxpool.New(context.Background(), pgConfig.ConnectionURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create PG driver: %w", err)
	}

	commonAgent := agent.CreateCommonAgent()
	// PGVersion
	PGVersion, err := pg.PGVersion(dbpool)
	if err != nil {
		return nil, err
	}
	commonAgent.DBPool = dbpool
	adapter := &RDSAdapter{
		CommonAgent: *commonAgent,
		Config:      config,
		pgConfig:    pgConfig,
		State: State{
			DBInfo: &dbInfo,
		},
		AWSClients:        clients,
		GuardrailSettings: guardrailSettings,
		PGDriver:          dbpool,
		PGVersion:         PGVersion,
	}
	adapter.Logger().Infof("detected parameter group %q for instance %q", dbInfo.ParameterGroupName, config.RDSDatabaseIdentifier)
	if dbInfo.ClusterParameterGroupName != "" {
		adapter.Logger().Infof("detected cluster parameter group %q", dbInfo.ClusterParameterGroupName)
	}
	return adapter, nil
}

func CreateRDSAdapter(configKey *string) (*RDSAdapter, error) {
	rdsAdapter, err := CreateRDSAdapterWithoutCollectors(configKey)
	if err != nil {
		return nil, err
	}
	catalog, err := pg.StandardCatalogCollectors(rdsAdapter.PGDriver, rdsAdapter.PGVersion)
	if err != nil {
		return nil, err
	}
	rdsAdapter.SetCatalogCollectors(catalog)
	collectors := rdsAdapter.Collectors()
	rdsAdapter.InitCollectors(collectors)
	return rdsAdapter, nil
}

func (adapter *RDSAdapter) refreshDBInfo(ctx context.Context) error {
	dbInfo, err := FetchDBInfo(
		adapter.Config.RDSDatabaseIdentifier,
		&adapter.AWSClients,
		ctx,
	)
	if err != nil {
		return err
	}
	adapter.State.DBInfo = &dbInfo
	adapter.State.LastDBInfoCheck = time.Now()
	return nil
}

func (adapter *RDSAdapter) GetSystemInfo(ctx context.Context) ([]metrics.FlatValue, error) {
	adapter.Logger().Info("Collecting system info")

	if err := adapter.refreshDBInfo(ctx); err != nil {
		return nil, err
	}

	// Get the RDSDB specific info
	info, err := adapter.State.DBInfo.TryIntoFlatValuesSlice()
	if err != nil {
		return nil, err
	}

	// PGVersion
	pgVersion, err := pg.PGVersion(adapter.PGDriver)
	if err != nil {
		return nil, err
	}

	version, err := metrics.PGVersion.AsFlatValue(pgVersion)
	if err != nil {
		adapter.Logger().Errorf("Failed to create PostgreSQL version metric: %v", err)
		return nil, err
	}
	info = append(info, version)

	// MaxConnections
	maxConnections, err := pg.MaxConnections(adapter.PGDriver)
	if err != nil {
		return nil, err
	}
	maxConnectionsMetric, err := metrics.PGMaxConnections.AsFlatValue(maxConnections)

	if err != nil {
		adapter.Logger().Errorf("Failed to create PostgreSQL max connections metric: %v", err)
		return nil, err
	}
	info = append(info, maxConnectionsMetric)

	// Current database and user-database count
	databaseInfo, err := pg.DatabaseSystemInfo(adapter.PGDriver)
	if err != nil {
		adapter.Logger().Errorf("Failed to create database system info metrics: %v", err)
		return nil, err
	}
	info = append(info, databaseInfo...)

	return info, nil
}

func (adapter *RDSAdapter) GetActiveConfig(ctx context.Context) (agent.ConfigArraySchema, error) {
	return pg.GetActiveConfig(adapter.PGDriver, ctx)
}

func (adapter *RDSAdapter) ApplyConfig(ctx context.Context, proposedConfig *agent.ProposedConfigResponse) agent.ApplyConfigError {
	if adapter.State.ApplyDebounced(applyDebounce) {
		adapter.Logger().Infof("Config was applied less than %s ago, skipping", applyDebounce)
		return nil
	}

	// Stamped on return, so a refused or failed apply is debounced too.
	defer func() { adapter.State.LastApplyAttempt = time.Now() }()

	// Fail fast before any AWS call. The check after the refresh catches a
	// group that rotated to a default since the last system-info tick.
	if err := defaultParameterGroupError(adapter.State.DBInfo); err != nil {
		return err
	}

	// Apply against the group attached now, not the one seen at the last tick.
	if err := adapter.refreshDBInfo(ctx); err != nil {
		return &agent.ConfigApplyError{Err: fmt.Errorf("failed to refresh DB info before apply: %w", err)}
	}
	if err := defaultParameterGroupError(adapter.State.DBInfo); err != nil {
		return err
	}

	adapter.Logger().Infof(
		"Parameter group %q apply status before write: %s",
		adapter.State.DBInfo.ParameterGroupName,
		adapter.State.DBInfo.ParameterGroupStatus,
	)

	targetConfig, targetErr := extractConfigValues(proposedConfig)
	if targetErr != nil {
		return &agent.ConfigApplyError{Err: fmt.Errorf("failed to resolve knobs to apply: %w", targetErr)}
	}

	err := ApplyConfig(
		targetConfig,
		proposedConfig.KnobApplication,
		&adapter.AWSClients,
		adapter.State.DBInfo.ParameterGroupName,
		adapter.Config.RDSDatabaseIdentifier,
		adapter.Logger(),
		ctx,
	)
	if err != nil {
		return asApplyConfigError(err)
	}

	// Instance is online, we validate that PostgreSQL is back online also
	adapter.Logger().Info("Waiting for PostgreSQL to come back online...")
	err = pg.WaitPostgresReady(adapter.PGDriver)
	if err != nil {
		return &agent.ConfigApplyError{Err: fmt.Errorf("error waiting for PostgreSQL to come back online: %w", err)}
	}

	// The group holding the values is not proof the server loaded them.
	if applyErr := adapter.verifyAppliedSettings(ctx, targetConfig); applyErr != nil {
		return applyErr
	}

	return nil
}

// Timing for the pg_settings read-back. RDS propagates an immediate change
// within about a minute; past that it is not coming.
const (
	pgVerifyTimeout  = 90 * time.Second
	pgVerifyInterval = 5 * time.Second
	// For the one-off group read on the failure path.
	paramGroupReadTimeout = 15 * time.Second
)

// verifyAppliedSettings polls pg_settings until the server reports every value
// written, and classifies the failure otherwise.
func (adapter *RDSAdapter) verifyAppliedSettings(
	ctx context.Context,
	targets []configValue,
) agent.ApplyConfigError {
	if len(targets) == 0 {
		return nil
	}

	waitCtx, cancel := context.WithTimeout(ctx, pgVerifyTimeout)
	defer cancel()

	adapter.Logger().Info("Verifying the new configuration is live in PostgreSQL...")

	diff := settingsDiff{Missing: configNames(targets)}
	for {
		rows, queryErr := queries.QueryPgSettings(adapter.PGDriver, waitCtx)
		if queryErr != nil {
			adapter.Logger().Warnf("Could not read pg_settings while verifying the apply: %v", queryErr)
		} else {
			diff = diffPGSettings(targets, rows)
			if diff.applied() {
				adapter.Logger().Infof("Configuration verified live for %s", strings.Join(configNames(targets), ", "))
				return nil
			}
			if len(diff.Missing) > 0 {
				return &agent.ConfigApplyError{Err: fmt.Errorf(
					"cannot verify apply: %s unknown to this PostgreSQL server",
					strings.Join(diff.Missing, ", "),
				)}
			}
			adapter.Logger().Infof("Waiting for PostgreSQL to report the new configuration: %s", diff)
		}

		select {
		case <-waitCtx.Done():
			return &agent.ConfigApplyError{Err: fmt.Errorf(
				"timed out after %s waiting for PostgreSQL to report the new configuration (%s); %s",
				pgVerifyTimeout, diff, adapter.parameterGroupDiagnosis(ctx, targets),
			)}
		case <-time.After(pgVerifyInterval):
		}
	}
}

// parameterGroupDiagnosis says whether the group holds the requested values,
// separating a write that never stuck from one the engine never loaded.
//
// Failure path only: it costs an API call and cannot judge an apply by itself.
func (adapter *RDSAdapter) parameterGroupDiagnosis(ctx context.Context, targets []configValue) string {
	name := adapter.State.DBInfo.ParameterGroupName

	ctx, cancel := context.WithTimeout(ctx, paramGroupReadTimeout)
	defer cancel()

	actual, err := getRDSParameterInfo(&adapter.AWSClients, name, configNames(targets), ctx)
	if err != nil {
		return fmt.Sprintf("could not read parameter group %q back to narrow it down: %v", name, err)
	}
	if mismatches := groupValueMismatches(targets, actual); len(mismatches) > 0 {
		return fmt.Sprintf(
			"parameter group %q does not hold %s, so the write did not stick",
			name, strings.Join(mismatches, ", "),
		)
	}
	return fmt.Sprintf(
		"parameter group %q does hold the requested values, so the engine never loaded them",
		name,
	)
}

// asApplyConfigError keeps a typed ApplyConfigError from anywhere in the chain,
// so the platform gets its wire type. pg.ValidateRestartPolicy returns
// *agent.RestartNotAllowedError, which wrapping would downgrade.
func asApplyConfigError(err error) agent.ApplyConfigError {
	var typed agent.ApplyConfigError
	if errors.As(err, &typed) {
		return typed
	}
	return &agent.ConfigApplyError{Err: fmt.Errorf("failed to apply config: %w", err)}
}

func (adapter *RDSAdapter) Collectors() []agent.MetricCollector {
	return []agent.MetricCollector{
		{
			Key: pg.MetricHardware,
			Collector: RDSHardwareInfo(
				adapter.Config.RDSDatabaseIdentifier,
				&adapter.State,
				&adapter.AWSClients,
				adapter.Logger(),
			),
		},
	}
}

// Guardrails checks memory utilization and returns Critical if thresholds are exceeded
func (adapter *RDSAdapter) Guardrails(_ context.Context) *guardrails.Signal {
	if time.Since(adapter.State.LastGuardrailCheck) < 5*time.Second {
		return nil
	}
	adapter.Logger().Info("Checking guardrails")
	adapter.State.LastGuardrailCheck = time.Now()

	totalMemoryBytes, err := adapter.State.DBInfo.TotalMemoryBytes()
	if err != nil {
		adapter.Logger().Errorf("Failed to get total memory bytes: %v", err)
		return nil
	}

	if adapter.State.DBInfo.PerformanceInsightsEnabled() {
		resourceID, err := adapter.State.DBInfo.ResourceID()
		if err != nil {
			adapter.Logger().Errorf("Failed to get resource ID: %v", err)
			return nil
		}

		memoryUsageBytes, err := GetMemoryUsageFromPI(
			&adapter.AWSClients,
			resourceID,
			adapter.Logger(),
		)
		if err != nil {
			adapter.Logger().Errorf("Failed to get memory usage from PI: %v", err)
			return nil
		}

		memoryUsagePercent := (float64(memoryUsageBytes) / float64(totalMemoryBytes)) * 100

		adapter.Logger().Debugf("Memory usage: %.2f%%", memoryUsagePercent)
		if memoryUsagePercent > adapter.GuardrailSettings.MemoryThreshold {
			return &guardrails.Signal{
				Level: guardrails.Critical,
				Type:  guardrails.Memory,
			}
		}
	} else {
		freeableMemoryBytes, err := GetFreeableMemoryFromCW(
			adapter.Config.RDSDatabaseIdentifier,
			&adapter.AWSClients,
		)
		if err != nil {
			adapter.Logger().Errorf("Failed to get memory usage from CloudWatch: %v", err)
			return nil
		}
		freeableMemoryPercent := (float64(freeableMemoryBytes) / float64(totalMemoryBytes)) * 100

		adapter.Logger().Debugf("Freeable memory: %.2f%%", freeableMemoryPercent)
		if freeableMemoryPercent < (100 - adapter.GuardrailSettings.MemoryThreshold) {
			return &guardrails.Signal{
				Level: guardrails.Critical,
				Type:  guardrails.FreeableMemory,
			}
		}
	}

	return nil
}

// NOTE: For now, Aurora doesn't deviate from RDS in any functional way via API or how we
// query things. If it were to change, we can expand upon this.
type AuroraRDSAdapter struct {
	RDSAdapter
}

func CreateAuroraRDSAdapter() (*AuroraRDSAdapter, error) {
	configKey := AURORA_CONFIG_KEY
	rdsAdapter, err := CreateRDSAdapterWithoutCollectors(&configKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create AuroraRDS adapter: %w", err)
	}
	cc, err := pg.CollectorsConfigFromViper()
	if err != nil {
		return nil, fmt.Errorf("failed to parse collectors config: %w", err)
	}
	// Aurora does not support pg_stat_wal: the underlying pg_stat_get_wal()
	// function errors with "not supported in Aurora". AWS does not document
	// this publicly; see https://github.com/DataDog/integrations-core/issues/15890.
	// pg_stat_wal_receiver is unsupported for the same reason: its backing
	// function pg_stat_get_wal_receiver() also errors with "not supported in Aurora".
	rdsAdapter.Logger().Infof("Aurora: disabling %s and %s (not supported)", queries.PgStatWalName, queries.PgStatWalReceiverName)
	disabled := false
	cc.Simple[queries.PgStatWalName] = collectorconfig.BaseConfig{Enabled: &disabled}
	cc.Simple[queries.PgStatWalReceiverName] = collectorconfig.BaseConfig{Enabled: &disabled}
	catalog, err := pg.CatalogCollectorsForVersion(rdsAdapter.PGDriver, rdsAdapter.PGVersion, cc)
	if err != nil {
		return nil, err
	}
	rdsAdapter.SetCatalogCollectors(catalog)
	collectors := rdsAdapter.Collectors()
	rdsAdapter.InitCollectors(collectors)
	return &AuroraRDSAdapter{*rdsAdapter}, nil
}
