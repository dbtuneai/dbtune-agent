<h1 align="center">DBtune Agent</h1>

![Go](https://img.shields.io/badge/golang-00ADD8?&style=plastic&logo=go&logoColor=white)

The DBtune agent is a lightweight, extensible monitoring and configuration management tool for PostgreSQL databases. It collects system metrics, database performance data, and manages database configurations through a centralized DBtune service.

## Features

- Real-time metric collection and monitoring
- Automatic configuration management and optimization
- Support for multiple PostgreSQL deployment types:
  - Standalone PostgreSQL servers
  - Docker containers
  - Amazon RDS (beta)
  - Amazon Aurora (beta)
- Extensible adapter architecture for custom deployments
- Concurrent metric collection with error handling
- Configuration through YAML or environment variables

## Quick Start

### Prerequisites

There are multiple ways to run the DBtune agent:

- Download and run the pre-built binary from the releases page
- Use our official Docker image
- Build from source (requires Go 1.23.1 or later)

### Installation

1. Download the latest release for your platform from our releases page or pull our Docker image:

```bash
docker pull dbtune/agent:latest
```

2. Configure your environment by creating a `dbtune.yaml` file:

```yaml
debug: false
postgresql:
  connection_url: postgresql://user:password@localhost:5432/database
dbtune:
  server_url: http://your-dbtune-server:8000
  api_key: your-api-key
  database_id: your-database-id
```

3. Run the agent:

```bash
./dbtune-agent  # If using binary
# OR with Docker:
docker run -v $(pwd)/dbtune.yaml:/etc/dbtune/dbtune.yaml dbtune/agent:latest
```

## Configuration

### Required YAML Configuration

```yaml
# Basic configuration
postgresql:
  connection_url: postgresql://user:password@localhost:5432/database # Required: Database connection string

dbtune:
  server_url: http://localhost:8000 # Required: DBtune server endpoint
  api_key: your-api-key # Required: Your DBtune API key
  database_id: your-database-id # Required: Unique identifier for your database

# Optional settings
debug: false # Enable debug logging
collection_interval: 60 # Metrics collection interval in seconds

# Docker-specific settings (only needed for Docker deployments)
docker:
  container_name: "postgres" # Name of your PostgreSQL container
  network: "docker_default" # Docker network name
```

### Environment Variables

All configuration options can be set through environment variables, prefixed with `DBT_`:

```bash
export DBT_POSTGRESQL_CONNECTION_URL=postgresql://user:password@localhost:5432/database
export DBT_DBTUNE_SERVER_URL=http://localhost:8000
export DBT_DBTUNE_API_KEY=your-api-key
export DBT_DBTUNE_DATABASE_ID=your-database-id
```

## Core Metrics

The agent collects essential metrics required for DBtune's optimization engine:

### Required System Metrics

- CPU usage (user, system, idle)
- Memory utilization (total, used, cached)
- Disk I/O (reads/writes per second)
- Network statistics (bytes in/out)

### Essential PostgreSQL Metrics

- Query performance statistics
  - Transactions per second
  - Query runtime distribution
  - Cache hit ratios
- Resource utilization
  - Active connections
  - Connection states
  - Database size
- Background processes
  - Autovacuum statistics
  - Background writer statistics
- Wait events and locks

### Configuration Information

- PostgreSQL version and settings
- System configuration
- Hardware specifications

## Troubleshooting

### Common Issues

1. Connection Issues

```
Error: could not create PG driver
```

- Verify PostgreSQL connection URL
- Check network connectivity
- Ensure PostgreSQL is running

2. Metric Collection Errors

```
Error: collector failed: context deadline exceeded
```

- Increase collection timeout in configuration
- Check database performance
- Verify network stability

### Getting Help

- Check our [documentation](https://docs.dbtune.com)
- Join our [community Discord](https://discord.gg/dbtune)
- Email support: support@dbtune.com

## License

[Add License Information]
