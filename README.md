<h1 align="center">DBtune Agent</h1>

![Go](https://img.shields.io/badge/golang-00ADD8?&style=plastic&logo=go&logoColor=white)

The DBtune agent is a lightweight, extensible monitoring and configuration management tool for PostgreSQL databases. It collects system metrics, database performance data, and manages database configurations through a centralized DBtune service.

## Features

- Real-time metric collection and monitoring
- Automatic configuration management and optimization
- Support for multiple PostgreSQL deployment types:
  - Standalone PostgreSQL servers
  - Docker containers
  - Amazon RDS (coming soon)
  - Amazon Aurora (coming soon)
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

2. Configure your environment by creating a `dbtune.yaml` file (check the [configuration section](#configuration) for more details):

```yaml
debug: false
postgresql:
  connection_url: postgresql://user:password@localhost:5432/database
dbtune:
  server_url: https://app.dbtune.com
  api_key: your-api-key
  database_id: your-database-id
```

3. Run the agent:

```bash
./dbtune-agent  # If using binary
# OR with Docker:
# Example with mounted config file
docker run -v $(pwd)/dbtune.yaml:/app/dbtune.yaml \
    --name dbtune-agent \
     public.ecr.aws/dbtune/dbtune/agent:latest

# Example with environment variables
docker run \
  -e DBT_POSTGRESQL_CONNECTION_URL=postgresql://user:password@localhost:5432/database \
  -e DBT_DBTUNE_SERVER_URL=https://app.dbtune.com \
  -e DBT_DBTUNE_API_KEY=your-api-key \
  -e DBT_DBTUNE_DATABASE_ID=your-database-id \
  public.ecr.aws/dbtune/dbtune/agent:latest
```

## Configuration

### Configuration via YAML

```yaml
# Basic configuration
postgresql:
  connection_url: postgresql://user:password@localhost:5432/database # Required: Database connection string
  # Restart specific variable
  # This variable is about restarting postgresql service with the default PostgreSQL adapter.
  # It might need to run as root depending on your system and service setup.
  # Check the postgres.go adapter to see which commands are getting executed.
  service_name: postgresql-17 # (required) name of the service in case of restarts

dbtune:
  server_url: https://app.dbtune.com # Optional: DBtune server endpoint (change for self-hosted)
  api_key: your-api-key # Required: Your DBtune API key
  database_id: your-database-id # Required: Unique identifier for your database

# Optional settings
debug: false # Enable debug logging

# Docker-specific settings (only needed for Docker deployments)
docker:
  container_name: "postgres" # Name of your PostgreSQL container
```

### Environment Variables

All configuration options can be set through environment variables, prefixed with `DBT_` and following a screaming snake case format:

```bash
export DBT_POSTGRESQL_CONNECTION_URL=postgresql://user:password@localhost:5432/database
export DBT_DBTUNE_SERVER_URL=http://localhost:8000
export DBT_DBTUNE_API_KEY=your-api-key
export DBT_DBTUNE_DATABASE_ID=your-database-id
```

## Core Metrics

The agent collects essential metrics required for DBtune's optimization engine, you can find more information about the metrics in our [Notion page](https://dbtune.notion.site/DBtune-Collected-Metrics-17b1ecfc272180cc9f2cd7faeab2c503?pvs=4).

### Required System Metrics

- CPU usage
- Memory utilization
- Disk I/O (reads/writes per second)

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

## How to contribute

- Fork the repo and create a new branch
- Then make a PR to the main branch

### Getting Help

- Check our [documentation](https://docs.dbtune.com)
- Email support: support at dbtune.com

## License

See github.com/dbtuneai/agent//blob/main/LICENSE
