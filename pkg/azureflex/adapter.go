package azureflex

import (
	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/metrics"
)

type AzureFlexAdapter struct {
	agent.CommonAgent
}

func (*AzureFlexAdapter) ApplyConfig(proposedConfig *agent.ProposedConfigResponse) error {
	return nil
}

func (*AzureFlexAdapter) GetActiveConfig() (agent.ConfigArraySchema, error) {
	return nil, nil
}

func (*AzureFlexAdapter) GetProposedConfig() (*agent.ProposedConfigResponse, error) {
	return nil, nil
}

func (*AzureFlexAdapter) GetMetrics() ([]metrics.FlatValue, error) {
	return []metrics.FlatValue{}, nil
}

func (*AzureFlexAdapter) GetSystemInfo() ([]metrics.FlatValue, error) {
	return []metrics.FlatValue{}, nil
}

func (*AzureFlexAdapter) Guardrails() *guardrails.Signal {
	return nil
}

func CreateAzureFlexAdapter() (*AzureFlexAdapter, error) {
	return &AzureFlexAdapter{}, nil
}
