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
├── cmd/agent.go            # Entry point: loads config, picks an adapter, runs startup checks, starts the runner
└── pkg/
    ├── runner/             # Runs the loops below; HealthGate pauses database work while PostgreSQL is down
    ├── agent/              # What an adapter is: AgentLooper, CommonAgent, CatalogGetter, PanicGuard
    ├── adapters/           # One package per deployment type, each with a CONFIG.md
    │   ├── aiven/
    │   ├── azureflex/
    │   ├── cloudsql/
    │   ├── cnpg/
    │   ├── docker/
    │   ├── patroni/
    │   ├── pgprem/         # Self-managed PostgreSQL (--local)
    │   └── rds/            # RDS and Aurora
    ├── backend/            # DBtune API client: one method per endpoint, payload formats, log shipping
    │   └── backendtest/    # Fakes the backend in tests
    ├── config/             # Loads dbtune.yaml and DBT_* variables; settings several packages share
    ├── tuning/             # The config loop: active config, recommendations, ALTER SYSTEM, reload, restart policy
    ├── catalog/            # Catalog collectors: framework, registry, user config, one file per PostgreSQL view
    │   └── collectorconfig/ # Parser for the collectors: config section
    ├── capabilities/       # Capability types and checkers, including the PostgreSQL role checker
    ├── metrics/            # Metric definitions and the metric collector types
    ├── guardrails/         # Guardrail signals
    ├── pg/                 # PostgreSQL helpers: prefixed queries, version, server info, connection errors
    ├── checks/             # Startup checks for config and connectivity
    ├── version/            # Build version information
    └── internal/           # kubernetes (used by cnpg), pgxutil (row scanner)
```

A setting that several packages read lives in `pkg/config`. A setting that only one package reads is parsed next to the code that uses it: each adapter's section in its `config.go`, the `collectors:` section in `pkg/catalog/config.go`, and the `dbtune:` section in `pkg/backend/urls.go`.

Every adapter package uses the same file names:

| File | Holds |
|---|---|
| `adapter.go` | The adapter struct and its constructor |
| `config.go` | The adapter's own config section, documented in `CONFIG.md` |
| `system_info.go` | `GetSystemInfo` |
| `apply.go` | `GetActiveConfig`, `ApplyConfig` and their helpers |
| `guardrails.go` | `Guardrails` |
| `hardware.go` | The adapter's metric collectors |
| Other files | Named after the platform API or concern they wrap, such as `cloudwatch.go`, `docker_stats.go` or `failover.go` |

## How the agent runs

`cmd/agent.go` creates one adapter, chosen by a flag (`--local`, `--docker`, `--rds`, `--aurora`, `--aiven`, `--cloudsql`, `--cnpg`, `--patroni`, `--azure-flex`) or, without a flag, detected from environment variables and the config file. It then hands the adapter to `runner.Runner`, which runs these loops:

| Loop | Interval | What it does | Code |
|---|---|---|---|
| Heartbeat | 15s | Tells the backend the agent is alive | `backend` |
| Metrics | 5s | Runs the adapter's metric collectors and sends their values together | `agent` runs them, the adapter's `hardware.go` defines them |
| System info | 1 min | Sends hardware and PostgreSQL facts (memory, CPU count, version, ...) | The adapter's `system_info.go` |
| Config | 5s | Sends the active configuration, fetches a recommended one and applies it | `tuning`, the adapter's `apply.go` |
| Guardrails | 1s, at most every 15s | Checks for problems such as memory pressure and signals the backend | The adapter's `guardrails.go` |
| Catalog collectors | Each its own | Queries one PostgreSQL view and sends it as its own payload | `catalog` |

Every loop sends its results through `backend`.

The agent collects two kinds of data:

- **Metric collectors** (`metrics.Collector`) gather hardware and infrastructure metrics from outside PostgreSQL: the OS, Docker, Kubernetes or a cloud provider's monitoring API. Each adapter supplies its own.
- **Catalog collectors** (`catalog.Collector`) query PostgreSQL views such as `pg_stat_statements`, `pg_class` or `pg_stats`. Every adapter gets the same set from `catalog.StandardCollectors`. Users can enable, disable and tune them in the `collectors:` section of the config file or with `DBT_COLLECTOR_*` environment variables.

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
	pgConfig, err := config.PostgresFromViper(nil)
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

	collectors, err := catalog.StandardCollectors(pool, pgVersion, common.Logger())
	if err != nil {
		return nil, err
	}
	a.SetCatalogCollectors(collectors)
	a.InitCollectors([]metrics.Collector{
		{Key: catalog.MetricHardware, Collector: myHardwareCollector()},
	})
	return a, nil
}

// The deployment-specific part of agent.AgentLooper:
func (a *MyAdapter) GetSystemInfo(ctx context.Context) ([]metrics.FlatValue, error)
func (a *MyAdapter) GetActiveConfig(ctx context.Context) (tuning.ConfigArraySchema, error)
func (a *MyAdapter) ApplyConfig(ctx context.Context, cfg *tuning.ProposedConfigResponse) tuning.ApplyConfigError
func (a *MyAdapter) Guardrails(ctx context.Context) *guardrails.Signal
```

`pkg/adapters/pgprem` is the simplest complete example. `pkg/tuning` has the building blocks for `ApplyConfig` on a database the agent can reach directly: `ParseKnobConfigurations`, `ValidateRestartPolicy`, `AlterSystem`, `ReloadConfig` and `WaitPostgresReady`. To finish the adapter:

1. Put it in `pkg/adapters/<name>/` with a `CONFIG.md` describing its settings, using the file names above.
2. Add a flag and a `case` for it in `cmd/agent.go`. To support detection without a flag, add `DetectConfigFromEnv` and `DetectConfigFromConfigFile` functions like the other adapters have.

### Adding a hardware metric

A metric collector adds values to the shared `metrics.State`. Put it in the adapter's `hardware.go`, and use a definition from `pkg/metrics` where one fits:

```go
func myHardwareCollector() func(ctx context.Context, state *metrics.State) error {
	return func(ctx context.Context, state *metrics.State) error {
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

A catalog collector is a row type, a name, an interval and a query, in its own file in `pkg/catalog`. `pg_stat_archiver.go` is a minimal example:

```go
type PgStatArchiverRow struct {
	ArchivedCount *Bigint `json:"archived_count" db:"archived_count"`
	// ...
}

const (
	PgStatArchiverName     = "pg_stat_archiver"
	PgStatArchiverInterval = 1 * time.Minute
)

func PgStatArchiverCollector(pool *pgxpool.Pool, prepareCtx PrepareCtx) Collector {
	return NewCollector[PgStatArchiverRow](pool, prepareCtx, PgStatArchiverName, PgStatArchiverInterval,
		`SELECT * FROM pg_stat_archiver`, WithSkipUnchanged())
}
```

Then register it in two places, both in `pkg/catalog`:

1. The `entries` list in `BuildCollectors` (`registry.go`), so every adapter runs it.
2. The `collectors` registry in `config.go`, so users can configure it.

Each catalog collector posts to `/api/v1/agent/<name>`, so the backend needs that endpoint before the collector ships.

### Reporting missing permissions

When the agent can run without a permission but loses data or features, report it as a capability instead of failing startup. Write a `capabilities.CapabilityChecker` and pass it to `catalog.StandardCollectors` as an extra checker. `pkg/capabilities/postgres.go` is an example. The capabilities collector runs every checker, logs what is missing and reports it to the backend.

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
