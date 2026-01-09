# dbtune-agent - Development Guide

## Overview

The dbtune-agent is a lightweight, extensible monitoring and configuration management tool written in Go. It collects metrics from PostgreSQL databases, applies configuration changes, and monitors guardrails. The agent communicates with the dbtune-platform via REST API to enable database performance tuning.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         dbtune-agent                                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   Entry Point           Core Logic              Communication       │
│   ┌─────────┐          ┌─────────┐             ┌─────────┐         │
│   │cmd/     │─────────►│runner/  │────────────►│dbtune/  │         │
│   │agent.go │          │         │             │API      │         │
│   └─────────┘          └────┬────┘             └─────────┘         │
│                             │                        │              │
│                             ▼                        ▼              │
│                        ┌─────────┐          REST API to Platform   │
│                        │agent/   │                                 │
│                        │Common   │                                 │
│                        │Agent    │                                 │
│                        └────┬────┘                                 │
│                             │                                       │
│              ┌──────────────┼──────────────┐                       │
│              │              │              │                        │
│              ▼              ▼              ▼                        │
│         ┌─────────┐    ┌─────────┐   ┌─────────┐                  │
│         │Adapter  │    │Adapter  │   │Adapter  │                  │
│         │pgprem/  │    │rds/     │   │cloudsql/│                  │
│         └────┬────┘    └────┬────┘   └────┬────┘                  │
│              │              │              │                        │
│              └──────────────┼──────────────┘                        │
│                             │                                       │
│                             ▼                                       │
│                        ┌─────────┐                                 │
│                        │pg/      │                                 │
│                        │Metrics  │                                 │
│                        │Queries  │                                 │
│                        └────┬────┘                                 │
│                             │                                       │
│                             ▼                                       │
│                     PostgreSQL Database                             │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
dbtune-agent/
├── cmd/
│   └── agent.go              # Entry point - CLI flag handling and adapter selection
├── pkg/
│   ├── agent/                # Core agent implementation
│   │   ├── agent.go          # CommonAgent and AgentLooper interface
│   │   └── agent_test.go     # Agent tests
│   ├── runner/               # Main execution loop
│   ├── pg/                   # PostgreSQL utilities and metric collectors
│   ├── dbtune/               # REST API client for platform communication
│   ├── metrics/              # Metric type definitions and formatting
│   ├── guardrails/           # Performance safety mechanisms
│   ├── internal/             # Internal utilities
│   │   ├── parameters/       # Configuration parameter parsing
│   │   └── utils/            # Helper functions
│   ├── pgprem/               # On-premises PostgreSQL adapter
│   │   ├── adapter.go        # DefaultPostgreSQLAdapter implementation
│   │   └── collectors.go     # Collector registration
│   ├── rds/                  # AWS RDS/Aurora adapter
│   │   ├── adapters.go       # RDSAdapter/AuroraAdapter
│   │   ├── clients.go        # AWS SDK clients
│   │   └── collectors.go     # AWS-specific collectors
│   ├── cloudsql/             # Google Cloud SQL adapter
│   ├── aiven/                # Aiven PostgreSQL adapter
│   ├── azureflex/            # Azure Flexible Server adapter
│   ├── cnpg/                 # CloudNativePG (Kubernetes) adapter
│   ├── patroni/              # Patroni HA clusters adapter
│   └── docker/               # Docker container adapter
├── go.mod                    # Go module definition
├── go.sum                    # Dependency checksums
├── Taskfile.yml              # Task commands for development
├── README.md                 # User documentation
├── CustomAgent.md            # Extension guide
└── dbtune.yaml               # Configuration file example
```

## Key Interfaces

### AgentLooper Interface

All adapters must implement the `AgentLooper` interface defined in `pkg/agent/agent.go`:

```go
type AgentLooper interface {
    // Heartbeat and health
    SendHeartbeat() error

    // Metrics collection and transmission
    GetMetrics() ([]metrics.FlatValue, error)
    SendMetrics([]metrics.FlatValue) error

    // System information
    GetSystemInfo() ([]metrics.FlatValue, error)
    SendSystemInfo([]metrics.FlatValue) error

    // Configuration management
    GetActiveConfig() (ConfigArraySchema, error)
    SendActiveConfig(ConfigArraySchema) error
    GetProposedConfig() (*ProposedConfigResponse, error)
    ApplyConfig(knobs *ProposedConfigResponse) error

    // Safety mechanisms
    Guardrails() *guardrails.Signal
    SendGuardrailSignal(signal guardrails.Signal) error

    // Error reporting
    SendError(payload ErrorPayload) error

    // Logging
    Logger() *log.Logger
}
```

### CommonAgent

The `CommonAgent` struct provides default implementations for most `AgentLooper` methods. Adapters typically:

1. Embed `CommonAgent`
2. Override specific methods as needed (e.g., `GetSystemInfo`, `ApplyConfig`, `Guardrails`)
3. Register their metric collectors

Example from `pkg/pgprem/adapter.go`:

```go
type DefaultPostgreSQLAdapter struct {
    agent.CommonAgent
    pgDriver        *pgPool.Pool
    pgConfig        pg.Config
    GuardrailConfig guardrails.Config
    PGVersion       string
}
```

## Adapter Pattern

### Supported Adapters

| Adapter | Package | Use Case | Key Differences |
|---------|---------|----------|-----------------|
| `DefaultPostgreSQLAdapter` | `pkg/pgprem` | On-premises PostgreSQL | Uses systemctl for restarts, local hardware metrics |
| `RDSAdapter` | `pkg/rds` | AWS RDS PostgreSQL | Uses AWS SDK for config changes, CloudWatch metrics |
| `AuroraAdapter` | `pkg/rds` | AWS Aurora PostgreSQL | Aurora-specific parameter groups and storage |
| `CloudSQLAdapter` | `pkg/cloudsql` | Google Cloud SQL | GCP APIs for management |
| `AivenAdapter` | `pkg/aiven` | Aiven managed PostgreSQL | Aiven API integration |
| `AzureFlexAdapter` | `pkg/azureflex` | Azure Flexible Server | Azure SDK integration |
| `CNPGAdapter` | `pkg/cnpg` | CloudNativePG (Kubernetes) | Kubernetes CRD operations |
| `PatroniAdapter` | `pkg/patroni` | Patroni HA clusters | Patroni REST API |
| `DockerContainerAdapter` | `pkg/docker` | Docker containers | Docker SDK for restarts |

### Creating a New Adapter

1. Create a package in `pkg/your-adapter/`
2. Define your adapter struct embedding `CommonAgent`
3. Implement required methods:
   - `GetSystemInfo()` - Gather system hardware/version info
   - `ApplyConfig()` - Apply configuration changes (restart/reload logic)
   - `Guardrails()` - Safety checks (memory, performance thresholds)
4. Register collectors in a `DefaultCollectors()` function
5. Add a constructor function (e.g., `CreateYourAdapter()`)
6. Add CLI flag in `cmd/agent.go`

Example skeleton:

```go
package youradapter

import "github.com/dbtuneai/agent/pkg/agent"

type YourAdapter struct {
    agent.CommonAgent
    // Add custom fields
}

func CreateYourAdapter() (*YourAdapter, error) {
    commonAgent := agent.CreateCommonAgent()
    adapter := &YourAdapter{
        CommonAgent: *commonAgent,
    }

    // Register collectors
    collectors := DefaultCollectors(adapter)
    adapter.InitCollectors(collectors)

    return adapter, nil
}

func DefaultCollectors(adapter *YourAdapter) []agent.MetricCollector {
    return []agent.MetricCollector{
        // Add your collectors here
    }
}

func (adapter *YourAdapter) GetSystemInfo() ([]metrics.FlatValue, error) {
    // Implement system info collection
}

func (adapter *YourAdapter) ApplyConfig(config *agent.ProposedConfigResponse) error {
    // Implement configuration application
}

func (adapter *YourAdapter) Guardrails() *guardrails.Signal {
    // Implement safety checks
}
```

## Metric Collection

### MetricCollector Pattern

Metrics are collected using a concurrent, timeout-aware pattern. Each collector is a function that:

1. Accepts a `context.Context` and `*MetricsState`
2. Collects metrics from PostgreSQL or system
3. Adds metrics to state using `state.AddMetric(metric)`
4. Returns an error if collection fails

Example from `pkg/pg/`:

```go
func TransactionsPerSecond(dbpool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
    return func(ctx context.Context, state *agent.MetricsState) error {
        // Query PostgreSQL
        var xactCommit int64
        err := dbpool.QueryRow(ctx, "SELECT xact_commit FROM pg_stat_database WHERE datname = current_database()").Scan(&xactCommit)
        if err != nil {
            return err
        }

        // Calculate TPS from cached value
        cachedXact := state.Cache.XactCommit
        if !cachedXact.Timestamp.IsZero() {
            timeDiff := time.Since(cachedXact.Timestamp).Seconds()
            tps := float64(xactCommit - cachedXact.Count) / timeDiff

            metric, _ := metrics.PGTPS.AsFlatValue(tps)
            state.AddMetric(metric)
        }

        // Update cache
        state.Cache.XactCommit = agent.XactStat{
            Count:     xactCommit,
            Timestamp: time.Now(),
        }

        return nil
    }
}
```

### Standard Collectors

Located in `pkg/pg/`:

- `PGStatStatements()` - Query runtime metrics from pg_stat_statements
- `TransactionsPerSecond()` - TPS calculation
- `Connections()` - Active connection count
- `DatabaseSize()` - Database size in bytes
- `Autovacuum()` - Autovacuum activity
- `UptimeMinutes()` - Server uptime
- `PGStatDatabase()` - Database-level statistics
- `PGStatUserTables()` - Table-level statistics
- `PGStatBGwriter()` - Background writer stats
- `PGStatWAL()` - WAL statistics
- `WaitEvents()` - Wait event analysis

### Adding a Custom Collector

1. Create collector function in appropriate package
2. Register it in adapter's `DefaultCollectors()`
3. Use `metrics.FlatValue` for type safety
4. Handle context cancellation for timeouts

## Configuration Management

### Applying Configuration Changes

Configuration changes follow this flow:

1. Platform sends `ProposedConfigResponse` via REST API
2. Agent calls `ApplyConfig(config)`
3. Agent uses `pg.AlterSystem()` to apply each parameter
4. Agent performs reload or restart based on `config.KnobApplication`
5. Agent waits for PostgreSQL to be ready

Example from `pkg/pgprem/adapter.go`:

```go
func (adapter *DefaultPostgreSQLAdapter) ApplyConfig(proposedConfig *agent.ProposedConfigResponse) error {
    // Parse knobs
    parsedKnobs, err := parameters.ParseKnobConfigurations(proposedConfig)
    if err != nil {
        return err
    }

    // Apply each knob
    for _, knob := range parsedKnobs {
        err = pg.AlterSystem(adapter.pgDriver, knob.Name, knob.SettingValue)
        if err != nil {
            return fmt.Errorf("failed to alter system for %s: %w", knob.Name, err)
        }
    }

    // Reload or restart based on knob_application
    switch proposedConfig.KnobApplication {
    case "restart":
        // Restart service
        cmd := exec.Command("systemctl", "restart", adapter.pgConfig.ServiceName)
        if err := cmd.Run(); err != nil {
            return err
        }
        err := pg.WaitPostgresReady(adapter.pgDriver)
        if err != nil {
            return err
        }
    case "reload":
        err := pg.ReloadConfig(adapter.pgDriver)
        if err != nil {
            return err
        }
    }

    return nil
}
```

### Configuration Types

- `KnobApplication`: `"restart"` (requires restart) or `"reload"` (reload only)
- Parameters are typed: `integer`, `real`, `bool`, `string`, `enum`
- Units are tracked (e.g., `8kB`, `10min`)

## Guardrails

Guardrails prevent performance regressions during tuning. The agent checks:

1. Memory usage thresholds (default 90%)
2. Performance baseline comparisons
3. Hardware resource limits

Example from `pkg/pgprem/adapter.go`:

```go
func (adapter *DefaultPostgreSQLAdapter) Guardrails() *guardrails.Signal {
    memoryInfo, err := mem.VirtualMemory()
    if err != nil {
        return nil
    }

    memoryUsagePercent := float64(memoryInfo.Total-memoryInfo.Available) / float64(memoryInfo.Total) * 100

    if memoryUsagePercent > adapter.GuardrailConfig.MemoryThreshold {
        return &guardrails.Signal{
            Level: guardrails.Critical,
            Type:  guardrails.Memory,
        }
    }

    return nil
}
```

When a guardrail is triggered, the agent sends a signal to the platform, which can pause or rollback tuning.

## Development Workflow

### Prerequisites

- Go 1.23.1 or later
- PostgreSQL instance with pg_stat_statements enabled
- Task runner (`go install github.com/go-task/task/v3/cmd/task@latest`)

### Building

```bash
# Build the agent
go build -o dbtune-agent ./cmd/agent.go

# Or use the platform-specific build
GOOS=linux GOARCH=amd64 go build -o dbtune-agent-linux ./cmd/agent.go
```

### Running Tests

```bash
# Run all tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run tests for specific package
go test -v ./pkg/agent/

# Run specific test
go test -v ./pkg/agent/ -run TestGetMetrics

# Run tests with coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### Local Development

```bash
# Start a development PostgreSQL instance
task start-dev-postgres

# Run the agent in debug mode
# Create a dbtune.yaml with your config first
./dbtune-agent --local

# Or use environment variables
export DBT_POSTGRESQL_CONNECTION_URL="postgresql://dbtune:password@localhost:1994/postgres"
export DBT_DBTUNE_SERVER_URL="https://app.dbtune.com"
export DBT_DBTUNE_API_KEY="your-api-key"
export DBT_DBTUNE_DATABASE_ID="your-database-id"
export DBT_POSTGRESQL_INCLUDE_QUERIES=true
./dbtune-agent --local
```

### Debugging

Enable debug logging in `dbtune.yaml`:

```yaml
debug: true
```

Or via environment variable:

```bash
export DBT_DEBUG=true
```

This will output detailed logs for:
- Collector execution
- API communication
- Configuration changes
- Error details

## Configuration

### dbtune.yaml

```yaml
postgresql:
  connection_url: postgresql://user:password@localhost:5432/database
  include_queries: true
  maximum_query_text_length: 1000
  service_name: postgresql  # For restart operations (pgprem only)

guardrail_settings:
  memory_threshold: 90  # Percentage (0-100)

dbtune:
  server_url: https://app.dbtune.com
  api_key: your-api-key
  database_id: your-database-id

debug: false
```

### Environment Variables

All config options can be set via environment variables with `DBT_` prefix:

```bash
DBT_POSTGRESQL_CONNECTION_URL=postgresql://...
DBT_POSTGRESQL_INCLUDE_QUERIES=true
DBT_DBTUNE_SERVER_URL=https://app.dbtune.com
DBT_DBTUNE_API_KEY=your-key
DBT_DBTUNE_DATABASE_ID=your-id
DBT_GUARDRAIL_SETTINGS_MEMORY_THRESHOLD=90
DBT_DEBUG=true
```

## Common Development Tasks

### Adding a New Metric

1. Define metric in `pkg/metrics/metrics.go`:
   ```go
   var MyNewMetric = MetricDef{Key: "my_new_metric", Type: Int}
   ```

2. Create collector in `pkg/pg/` or adapter package:
   ```go
   func MyNewMetricCollector(dbpool *pgxpool.Pool) func(ctx context.Context, state *agent.MetricsState) error {
       return func(ctx context.Context, state *agent.MetricsState) error {
           var value int64
           err := dbpool.QueryRow(ctx, "SELECT my_value FROM my_table").Scan(&value)
           if err != nil {
               return err
           }

           metric, _ := metrics.MyNewMetric.AsFlatValue(value)
           state.AddMetric(metric)
           return nil
       }
   }
   ```

3. Register in adapter's `DefaultCollectors()`:
   ```go
   collectors = append(collectors, agent.MetricCollector{
       Key:       "my_new_metric",
       Collector: pg.MyNewMetricCollector(pgDriver),
   })
   ```

### Modifying Configuration Application

Most changes happen in `pkg/pg/config.go`:

- `AlterSystem()` - Sends ALTER SYSTEM SET commands
- `ReloadConfig()` - Calls pg_reload_conf()
- `WaitPostgresReady()` - Polls until PostgreSQL accepts connections

For adapter-specific restart logic, override `ApplyConfig()` in your adapter.

### Testing Configuration Changes

1. Use the development PostgreSQL instance
2. Create a test configuration
3. Apply via `ApplyConfig()`
4. Verify with `GetActiveConfig()`

Example test:

```go
func TestApplyConfig(t *testing.T) {
    adapter, _ := pgprem.CreateDefaultPostgreSQLAdapter()

    config := &agent.ProposedConfigResponse{
        Config: []agent.PGConfigRow{
            {Name: "work_mem", Setting: "8MB"},
        },
        KnobApplication: "reload",
    }

    err := adapter.ApplyConfig(config)
    assert.NoError(t, err)

    activeConfig, _ := adapter.GetActiveConfig()
    // Verify work_mem was set
}
```

## REST API Communication

The agent communicates with the platform via REST API endpoints defined in `pkg/dbtune/`:

- `POST /api/v1/agent/heartbeat` - Send heartbeat
- `POST /api/v1/agent/metrics` - Send metrics
- `PUT /api/v1/agent/systeminfo` - Send system info
- `POST /api/v1/agent/activeconfig` - Send active configuration
- `GET /api/v1/agent/knobs` - Get proposed configuration
- `POST /api/v1/agent/guardrail` - Send guardrail signal
- `POST /api/v1/agent/error` - Report errors

All requests include the `DBTUNE-API-KEY` header for authentication.

## Code Style

- **Go version**: 1.23.1+
- **Linting**: Use `golangci-lint`
- **Formatting**: Use `gofmt` or `goimports`
- **Testing**: Use `testing` package and `testify` for assertions
- **Error handling**: Always wrap errors with context using `fmt.Errorf("context: %w", err)`
- **Logging**: Use structured logging via `logrus`
- **Concurrency**: Use context.Context for cancellation, timeouts

## Common Pitfalls

1. **Context cancellation**: Always respect context cancellation in collectors
2. **Metric caching**: Use `state.Cache` for metrics that need previous values (TPS, I/O rates)
3. **Error handling**: Collectors should return errors, not panic
4. **Resource cleanup**: Always close database connections, defer response body closes
5. **Timeout handling**: Individual collectors have 10s timeout, total collection has 20s
6. **PostgreSQL version compatibility**: Check version before using version-specific features (e.g., pg_stat_checkpointer in PG17+)

## References

- **README.md** - User-facing documentation and quick start
- **CustomAgent.md** - Detailed extension guide for custom adapters
- **pkg/agent/agent.go** - Core AgentLooper interface definition
- **pkg/pg/** - PostgreSQL utility functions and metric collectors
- **Main CLAUDE.md** (parent directory) - Full platform architecture overview

## Tips for Development

1. **Start simple**: Use `DefaultPostgreSQLAdapter` as a reference when creating new adapters
2. **Test incrementally**: Test each collector individually before integrating
3. **Use debug mode**: Enable debug logging to see detailed execution flow
4. **Check logs**: The agent logs all API calls, errors, and collector execution
5. **Monitor heartbeats**: If heartbeats fail, check network connectivity and API key
6. **Version compatibility**: Test with multiple PostgreSQL versions (12, 13, 14, 15, 16, 17)
7. **Read integration tests**: See `dbtune-integration-tests/` for end-to-end examples

## Getting Help

- Check existing adapters for patterns (pgprem, rds, cloudsql)
- Read test files for usage examples
- Review GitHub issues for known problems
- Consult platform documentation at https://docs.dbtune.com
- Email support at support@dbtune.com
