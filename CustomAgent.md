# Extending DBtune agent

This guide explains how to build DBtune agent from source and extend it with custom functionality.

## Building from source

1. Prerequisites:

   - Go 1.23.1 or later
   - Git

2. Clone and build:

```bash
git clone <repository-url>
cd dbtune-agent
go build -o dbtune-agent cmd/agent.go
```

## Project structure

```
dbtune-agent/
├── cmd/                    # Main application entry points
├── pkg/                    # Core packages
│   ├── adapters/           # Database deployment type adapters (PostgreSQL, Docker)
│   ├── adeptersinterfaces/ # Common interfaces for adapters
│   ├── agent/              # Core agent functionality
│   ├── collectors/         # Metric collection implementations
│   ├── internal/           # Internal utilities and parameters
│   └── runner/             # Agent runtime execution
```

## Extension points

The DBtune agent is designed to be extensible through its adapter architecture. You can extend it in several ways:

1. Custom database adapters (PostgreSQL On-prem, RDS, Aurora, Tembo, etc.)
2. New metric collectors (custom metrics)
3. Custom parameter management
4. Additional data sources (sources like Prometheus, Datadog, etc.)

### Creating a custom adapter

1. Create a new adapter struct that implements the PostgreSQLAdapter interface from `pkg/adeptersinterfaces`:

```go
type CustomAdapter struct {
    adapters.DefaultPostgreSQLAdapter
    // Add custom fields
}

// Required interface methods to implement:
func (a *CustomAdapter) Logger() *logrus.Logger
func (a *CustomAdapter) PGDriver() *pgPool.Pool
func (a *CustomAdapter) APIClient() *retryablehttp.Client

// Optional: Implement additional methods to extend/override functionality
func (a *CustomAdapter) GetSystemInfo() ([]utils.FlatValue, error) {
    // Custom system info implementation
}

func (a *CustomAdapter) Guardrails() *agent.GuardrailType {
    // Custom guardrails implementation
}
```

2. Modify the `cmd/agent.go` to include your adapter and run it:

```go
    case *useCustomAdapter:
        adapter, err = adapters.CreateCustomAdapter()
        if err != nil {
            log.Fatalf("Failed to create Custom adapter: %v", err)
        }
```

### Adding new metric collectors

1. Create a new collector in the `pkg/collectors` package:

```go
func NewCustomCollector(pgAdapter adeptersinterfaces.PostgreSQLAdapter) func(ctx context.Context, state *agent.MetricsState) error {
    return func(ctx context.Context, state *agent.MetricsState) error {
        // Implement collection logic
        value, err := collectCustomMetric()
        if err != nil {
            return err
        }

        metric, err := utils.NewMetric("custom.metric", value, utils.Int)
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
func CreateCustomAdapter() (*CustomAdapter, error) {
    // Initialize base adapter...

    customAdapter := &CustomAdapter{
        DefaultPostgreSQLAdapter: *defaultAdapter,
        // Initialize custom fields...
    }

    // Add custom collectors
    customAdapter.MetricsState.Collectors = append(
        defaultAdapter.MetricsState.Collectors,
        agent.MetricCollector{
            Key:        "custom_metric",
            MetricType: utils.Int,
            Collector:  collectors.NewCustomCollector(customAdapter),
        },
    )

    return customAdapter, nil
}
```

## Development guidelines

### Testing

1. Unit tests:

```bash
go test ./...
```

2. Integration tests:

```bash
task integration-tests
```

3. Local development environment:

```bash
# Start a local PostgreSQL instance
task start-dev-postgres

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
