package azureflex

import (
	"context"
	"fmt"
	"regexp"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/postgresql/armpostgresqlflexibleservers/v5"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/pg"
)

var parseSKURegex = regexp.MustCompile("([B,D,E])([0-9]+)(ds|ads|s|ms)(?:_v([0-9]+))?")

func (adapter *AzureFlexAdapter) GetSystemInfo(ctx context.Context) ([]metrics.FlatValue, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}

	clientFactory, err := armpostgresqlflexibleservers.NewClientFactory(adapter.AzureFlexConfig.SubscriptionID, cred, nil)
	if err != nil {
		return nil, err
	}

	serverInfo, err := clientFactory.NewServersClient().Get(ctx, adapter.AzureFlexConfig.ResourceGroupName, adapter.AzureFlexConfig.ServerName, nil)
	if err != nil {
		return nil, err
	}

	// TODO: nil pointer checks!
	sku := *serverInfo.SKU.Name

	// TODO: separate out SKU handling and test
	matches := parseSKURegex.FindStringSubmatch(sku)

	// first elements of matches will be the whole string, so we want that plus 4 more
	if len(matches) != 5 {
		return nil, fmt.Errorf("Could not parse Azure SKU for System Info")
	}
	machineSeries := matches[1]
	nCores, _ := strconv.Atoi(matches[2])
	machineSubSeries := matches[3]

	var memGb int
	switch machineSeries {
	case "B":
		switch nCores {
		case 1:
			memGb = 2
		case 2:
			if machineSubSeries == "s" {
				memGb = 4
			} else {
				memGb = 8
			}
		default:
			memGb = 4 * nCores
		}
	case "D":
		memGb = 4 * nCores
	case "E":
		switch nCores {
		case 96:
			memGb = 672
		case 64:
			memGb = 432
		default:
			memGb = 8 * nCores
		}
	}

	maxConnections, err := pg.MaxConnections(adapter.PGDriver)
	if err != nil {
		return nil, err
	}

	pgVersion, err := pg.PGVersion(adapter.PGDriver)
	if err != nil {
		return nil, err
	}

	version, err := metrics.PGVersion.AsFlatValue(pgVersion.String())
	if err != nil {
		return nil, fmt.Errorf("Failed to convert version: %w", err)
	}
	maxConnectionsMetric, err := metrics.PGMaxConnections.AsFlatValue(maxConnections)
	if err != nil {
		return nil, fmt.Errorf("Failed to convert connections: %w", err)
	}
	cpuCountMetric, err := metrics.NodeCPUCount.AsFlatValue(nCores)
	if err != nil {
		return nil, fmt.Errorf("Failed to convert cpu count: %w", err)
	}
	memoryTotalMetric, err := metrics.NodeMemoryTotal.AsFlatValue(memGb * 1024 * 1024 * 1024)
	if err != nil {
		return nil, fmt.Errorf("Failed to convert memory total: %w", err)
	}

	databaseInfo, err := pg.DatabaseSystemInfo(adapter.PGDriver)
	if err != nil {
		return nil, err
	}

	systemInfo := make([]metrics.FlatValue, 0, 4+len(databaseInfo))
	systemInfo = append(systemInfo,
		version,
		maxConnectionsMetric,
		cpuCountMetric,
		memoryTotalMetric,
	)
	systemInfo = append(systemInfo, databaseInfo...)
	return systemInfo, nil
}
