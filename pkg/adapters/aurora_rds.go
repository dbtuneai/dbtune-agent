package adapters

import (
	"fmt"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/hashicorp/go-retryablehttp"
	pgPool "github.com/jackc/pgx/v5/pgxpool"
)

type AuroraRDSAdapter struct {
	rdsAdapter RDSAdapter
}

func CreateAuroraRDSAdapter() (*AuroraRDSAdapter, error) {
	rdsConfig, err := BindConfigRDS("rds-aurora")
	if err != nil {
		return nil, fmt.Errorf("failed to bind RDS config: %w", err)
	}

	rdsAdapter, err := CreateRDSAdapter(rdsConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create RDS adapter: %w", err)
	}
	return &AuroraRDSAdapter{rdsAdapter: *rdsAdapter}, nil
}

func (adapter *AuroraRDSAdapter) PGDriver() *pgPool.Pool {
	return adapter.rdsAdapter.pgDriver
}

func (adapter *AuroraRDSAdapter) APIClient() *retryablehttp.Client {
	return adapter.rdsAdapter.APIClient()
}

func (adapter *AuroraRDSAdapter) GetSystemInfo() ([]utils.FlatValue, error) {
	return adapter.rdsAdapter.GetSystemInfo()
}

func (adapter *AuroraRDSAdapter) ApplyConfig(proposedConfig *agent.ProposedConfigResponse) error {
	return adapter.rdsAdapter.ApplyConfig(proposedConfig)
}

func (adapter *AuroraRDSAdapter) Collectors() []agent.MetricCollector {
	return adapter.rdsAdapter.Collectors()
}

// Guardrails checks memory utilization and returns Critical if thresholds are exceeded
func (adapter *AuroraRDSAdapter) Guardrails() *agent.GuardrailSignal {
	return adapter.rdsAdapter.Guardrails()
}
