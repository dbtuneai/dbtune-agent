# Extending DBtune Agent

This guide explains how to build DBtune agent from source and extend it with custom functionality.

## Building from Source

1. Prerequisites:

   - Go 1.23.1 or later
   - Git

2. Clone and build:

```bash
git clone <repository-url>
cd dbtune-agent
go build -o dbtune-agent cmd/agent.go
```

## Project Structure

```
dbtune-agent/
├── cmd/                    # Main application entry points
└── internal/
    ├── adapters/          # Database deployment type adapters
    ├── collectors/        # Metric collection implementations
    ├── parameters/        # Parameter related utilities
    └── utils/             # Shared utilities and interfaces
```

## Extension Points

The DBtune agent is designed to be extensible through its adapter architecture. You can extend it in several ways:

1. Custom Database Adapters (PostgreSQL On-prem, RDS, Aurora, Tembo etc.)
2. New Metric Collectors (Custom metrics)
3. Custom Parameter Management
4. Additional Data Sources (sources like Prometheus, Datadog, etc.)

### Creating a Custom Adapter

1. Create a new adapter struct that implements the PostgreSQLAdapter interface:

```go
type CustomAdapter struct {
    adapters.DefaultPostgreSQLAdapter
    // Add custom fields
}

// Implement required interface methods that you want to extend
func (a *CustomAdapter) GetMetrics() ([]utils.FlatValue, error) {
    metrics := []utils.FlatValue{}

    // Include your metrics fetched from datasources like Prometheus, Datadog, etc.
    prometheusMetrics, err := GetPrometheusMetrics()
    if err != nil {
        return nil, err
    }

    // Add custom metrics
    metrics = append(metrics, utils.FlatValue{
        Key: "custom.metric",
        Value: prometheusMetrics.CustomMetric,
        Type: utils.GaugeType,
    })

    return metrics, nil
}
```

2. Modify the `agent.go` to include your adapter and run it:

```go
    case *useCustomAdapter:
		adapter, err = adapters.CreateCustomAdapter()
		if err != nil {
			log.Fatalf("Failed to create Custom adapter: %v", err)
		}
```

### Adding New Metric Collectors

1. Create a new collector in the `collectors` package:

```go
func NewCustomCollector(adapter utils.PostgreSQLAdapter) func(ctx context.Context, state *utils.MetricsState) error {
    return func(ctx context.Context, state *utils.MetricsState) error {
        // Implement collection logic
        value, err := collectCustomMetric()
        if err != nil {
            return err
        }

        metric, err := utils.NewMetric("custom.metric", value, utils.GaugeType)
        if err != nil {
            return err
        }

        state.AddMetric(metric)
        return nil
    }
}
```

2. Register the collector with your adapter:

```go
func (a *CustomAdapter) Initialize() error {
    err := a.DefaultPostgreSQLAdapter.Initialize()
    if err != nil {
        return err
    }

    a.MetricsState.Collectors = append(a.MetricsState.Collectors, utils.MetricCollector{
        Key:        "custom_metric",
        MetricType: utils.GaugeType,
        Collector:  collectors.NewCustomCollector(a),
    })

    return nil
}
```

## Development Guidelines

### Testing

1. Unit Tests:

```bash
go test ./...
```

2. Integration Tests:

```bash
task integration-tests
```

3. Local Development Environment:

```bash
# Start a local PostgreSQL instance
task start-demo-postgres

# Run the agent in debug mode
go run cmd/agent.go
```

## Debugging

To debug with more logging granularity enable debug logging in `dbtune.yaml`:

```yaml
debug: true
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Write tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## Support

For development support:

- GitHub Issues for bugs
