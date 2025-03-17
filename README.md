<h1 align="center">DBtune agent</h1>

![Go](https://img.shields.io/badge/golang-00ADD8?&style=plastic&logo=go&logoColor=white)

The DBtune agent is a lightweight, extensible monitoring and configuration management tool for PostgreSQL databases. It collects system metrics, database performance data, and manages database configurations through a centralized DBtune software as a service.

## Features

- Real-time metric collection and monitoring
- Automatic configuration management and optimization
- Support for multiple PostgreSQL deployment types:
  - Standalone PostgreSQL servers
  - Docker containers
  - Amazon Aurora
  - Amazon RDS (closed Beta)
  - Azure Flexible Server (coming soon)
- Extensible adapter architecture for custom deployments
- Concurrent metric collection with error handling
- Configuration through YAML or environment variables

## Quick start

### Prerequisites

There are multiple ways to run the DBtune agent:

- Download and run the pre-built binary from the releases page
- Use our official Docker image
- Build from source (requires Go 1.23.1 or later)

### Installation

1. Download the latest release for your platform from our releases page or pull our Docker image:

```bash
docker pull public.ecr.aws/dbtune/dbtune/agent:latest
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

# Aurora-specific settings
rds-aurora:
  AWS_REGION: "<your-aws-region>"
  # AWS credentials are optional
  # If not provided, the agent will use the default AWS credential chain, which includes WebIdentity tokens
  AWS_ACCESS_KEY_ID: "<your-aws-access-key-id>"
  AWS_SECRET_ACCESS_KEY: "<your-aws-secret-access-key>"
  RDS_DATABASE_IDENTIFIER: "<your-rds-database-identifier>" # The Writer instance of the Aurora cluster
  RDS_PARAMETER_GROUP_NAME: "<your-rds-parameter-group-name>" # Be sure to define a custom one and not to use the default.postgresXX one
```

### Environment variables

All configuration options can be set through environment variables, prefixed with `DBT_` and following a screaming snake case format:

```bash
export DBT_POSTGRESQL_CONNECTION_URL=postgresql://user:password@localhost:5432/database
export DBT_DBTUNE_SERVER_URL=http://localhost:8000
export DBT_DBTUNE_API_KEY=your-api-key
export DBT_DBTUNE_DATABASE_ID=your-database-id
```

## Core metrics

The agent collects essential metrics required for DBtune's optimization engine, you can find more information about the metrics in our [Notion page](https://dbtune.notion.site/DBtune-Collected-Metrics-17b1ecfc272180cc9f2cd7faeab2c503?pvs=4).

### Required system metrics

- CPU usage
- Memory utilization
- Disk I/O (reads/writes per second)

### Essential PostgreSQL metrics

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

### Configuration information

- PostgreSQL version and settings
- System configuration
- Hardware specifications

## How to contribute

- Fork the repo and create a new branch
- Then make a PR to the main branch

### Getting help

- Check our [documentation](https://docs.dbtune.com)
- Email support: support at dbtune.com

## License

See github.com/dbtuneai/agent//blob/main/LICENSE
