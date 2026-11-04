# Extending DBtune agent

This guide explains how to build DBtune agent from source and extend it with custom functionality.

## Building from source

1. Prerequisites:

   - Go 1.25 or later
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
├── cmd/agent.go            # Entry point: picks an adapter, runs startup checks, starts the runner
└── pkg/
    ├── adapters/           # One package per deployment type, each with a CONFIG.md
    │   ├── aiven/
    │   ├── azureflex/
    │   ├── cloudsql/
    │   ├── cnpg/
    │   ├── docker/
    │   ├── patroni/
    │   ├── pgprem/         # Self-managed PostgreSQL (--local)
    │   └── rds/            # RDS and Aurora
    ├── agent/              # AgentLooper interface, CommonAgent, MetricCollector
    ├── backend/            # DBtune backend URLs; backendtest fakes the backend in tests
    ├── capabilities/       # Capability and CapabilityChecker types
    ├── checks/             # Startup checks for config and connectivity
    ├── guardrails/         # Guardrail signals and settings
    ├── metrics/            # Metric definitions and payload formatting
    ├── pg/                 # PostgreSQL config, helpers and collector configuration
    │   └── queries/        # Catalog collectors, one per PostgreSQL view
    ├── runner/             # Runs the collection, sending, config and guardrail loops
    ├── version/            # Build version information
    └── internal/           # kubernetes (used by cnpg), parameters, pgxutil, utils
```

## How the agent runs

`cmd/agent.go` creates one adapter, chosen by a flag (`--local`, `--docker`, `--rds`, `--aurora`, `--aiven`, `--cloudsql`, `--cnpg`, `--patroni`, `--azure-flex`) or, without a flag, detected from environment variables and the config file. It then hands the adapter to `runner.Runner`, which runs these loops:

| Loop | Interval | What it does |
|---|---|---|
| Heartbeat | 15s | Tells the backend the agent is alive |
| Metrics | 5s | Runs the adapter's metric collectors and sends their values together |
| System info | 1 min | Sends hardware and PostgreSQL facts (memory, CPU count, version, ...) |
| Config | 5s | Sends the active configuration, fetches a recommended one and applies it |
| Guardrails | 1s, at most every 15s | Checks for problems such as memory pressure and signals the backend |
| Catalog collectors | Each its own | Queries one PostgreSQL view and sends it as its own payload |

The agent collects two kinds of data:

- **Metric collectors** (`agent.MetricCollector`) gather hardware and infrastructure metrics from outside PostgreSQL: the OS, Docker, Kubernetes or a cloud provider's monitoring API. Each adapter supplies its own.
- **Catalog collectors** (`queries.CatalogCollector`) query PostgreSQL views such as `pg_stat_statements`, `pg_class` or `pg_stats`. Every adapter gets the same set from `pg.StandardCatalogCollectors`. Users can enable, disable and tune them in the `collectors:` section of the config file or with `DBT_COLLECTOR_*` environment variables.

## Extension points

### Adding an adapter

An adapter implements `agent.AgentLooper`. Embed `agent.CommonAgent`, which provides the backend client, the heartbeat, the metric loop and every `Send*` method, and `agent.CatalogGetter`, which provides `CatalogCollectors()`. You then only write the parts that depend on the deployment type:

```go
package myadapter

type MyAdapter struct {
	agent.CommonAgent
	agent.CatalogGetter
	pool *pgxpool.Pool
}

func CreateMyAdapter() (*MyAdapter, error) {
	pgConfig, err := pg.ConfigFromViper(nil)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.New(context.Background(), pgConfig.ConnectionURL)
	if err != nil {
		return nil, err
	}
	pgVersion, err := pg.PGVersion(pool)
	if err != nil {
		return nil, err
	}

	common := agent.CreateCommonAgent()
	common.DBPool = pool
	a := &MyAdapter{CommonAgent: *common, pool: pool}

	catalog, err := pg.StandardCatalogCollectors(pool, pgVersion, common.Logger())
	if err != nil {
		return nil, err
	}
	a.SetCatalogCollectors(catalog)
	a.InitCollectors([]agent.MetricCollector{
		{Key: pg.MetricHardware, Collector: myHardwareCollector()},
	})
	return a, nil
}

// The deployment-specific part of agent.AgentLooper:
func (a *MyAdapter) GetSystemInfo(ctx context.Context) ([]metrics.FlatValue, error)
func (a *MyAdapter) GetActiveConfig(ctx context.Context) (agent.ConfigArraySchema, error)
func (a *MyAdapter) ApplyConfig(ctx context.Context, cfg *agent.ProposedConfigResponse) agent.ApplyConfigError
func (a *MyAdapter) Guardrails(ctx context.Context) *guardrails.Signal
```

`pkg/adapters/pgprem` is the simplest complete example. To finish the adapter:

1. Put it in `pkg/adapters/<name>/` with a `CONFIG.md` describing its settings.
2. Add a flag and a `case` for it in `cmd/agent.go`. To support detection without a flag, add `DetectConfigFromEnv` and `DetectConfigFromConfigFile` functions like the other adapters have.

### Adding a hardware metric

A metric collector adds values to the shared `MetricsState`. Use a definition from `pkg/metrics` where one fits:

```go
func myHardwareCollector() func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		usage, err := readCPUUsage()
		if err != nil {
			return err
		}
		metric, err := metrics.NodeCPUUsage.AsFlatValue(usage)
		if err != nil {
			return err
		}
		state.AddMetric(metric)
		return nil
	}
}
```

For a new metric, define it with `metrics.MetricDef{Key: "my_metric", Type: metrics.Int}`. The backend also needs to know a new key before it can use it.

### Adding a catalog collector

A catalog collector is a row type, a name, an interval and a query. `pg_stat_archiver` is a minimal example:

```go
type PgStatArchiverRow struct {
	ArchivedCount *Bigint `json:"archived_count" db:"archived_count"`
	// ...
}

const (
	PgStatArchiverName     = "pg_stat_archiver"
	PgStatArchiverInterval = 1 * time.Minute
)

func PgStatArchiverCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) CatalogCollector {
	return NewCollector[PgStatArchiverRow](pool, prepareCtx, PgStatArchiverName, PgStatArchiverInterval,
		`SELECT * FROM pg_stat_archiver`, WithSkipUnchanged())
}
```

Then register it in two places:

1. The `entries` list in `pg.BuildCatalogCollectors` (`pkg/pg/build_catalog_collectors.go`), so every adapter runs it.
2. The `collectors` registry in `pkg/pg/collector_config.go`, so users can configure it.

Each catalog collector posts to `/api/v1/agent/<name>`, so the backend needs that endpoint before the collector ships.

### Reporting missing permissions

When the agent can run without a permission but loses data or features, report it as a capability instead of failing startup. Write a `capabilities.CapabilityChecker` and pass it to `pg.StandardCatalogCollectors` as an extra checker. The capabilities collector runs every checker, logs what is missing and reports it to the backend.

## Development guidelines

### Testing

1. Unit tests:

```bash
task test   # or: go test ./...
```

2. Integration tests (run against PostgreSQL 13 to 18; requires Docker):

```bash
task test-integration
```

3. Local development environment:

```bash
# Start a local PostgreSQL instance
task start-dev-postgres

# Run the agent against it
go run cmd/agent.go --local
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
