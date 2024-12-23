<h1 align="center">DBtune Agent</h1>

This repository includes the open-source DBtune agent.

There are batteries includes agent implementations for many PostgreSQL deployments/services, but the agent can be extended to incorporate new environments and data sources.

The agent currently collects information about (not limited to the following):

- System metrics
  a. CPU
  b. Memory
  c. Disk
  d. Network
- PostgreSQL metrics
  a. Query runtime
  b. TPS
  c. Active connections
- Server metrics
  a. Configuration running
  b. Postgresql version

## Templates

1. VM server running PostgreSQL
2. Docker hosted PostgreSQL
3. RDS PostgreSQL
4. Aurora PostgreSQL

## How to run

We provide docker images for the templates (see above).

If you want to customise the agent, you can ....

## Configs

The agent uses Viper for configuration. You can either set a `dbtune.yaml` file in the location that you run the binary run or set the environment variables.

The environment variables are prefixed with `DBT_DBTUNE_`, with all the keys screaming snake case, and flat structure.

For example the following `dbtune.yaml` file:

```
postgresql:
  connection_url: postgresql://user:password@localhost:5432/database
```

Will be translated to the following environment variables:

```
DBT_DBTUNE_POSTGRESQL_CONNECTION_URL=postgresql://user:password@localhost:5432/database
```

## Default config values

Below are the default values for the config. You can override them in the `dbtune.yaml` file or the environment variables.

```yaml
# Shared settings
debug: bool # Enable debug mode - default: False
```

### TODOs:

1. Why I implemented this with a flat array and not a dict from first place? cannot even remember:
   This snippet may be easier to work with, and this append may not be safe for concurrency if multiple threads want to use the same array (for get metrics)

```golang
	totalMemory, err := utils.NewMetric("system_info_total_memory", memoryInfo.Total, utils.Int)
	systemInfo = append(systemInfo, totalMemory)
```

2. Create a runner functionality that will execute the main loop based on the adapter

3. Create a config with Viper

4. Add required metrics to operate:

```
database_avg_query_runtime: float
database_tps: int

Optional - Implemented in template agents
database_active_connections: int
```
