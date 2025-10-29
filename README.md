<h1 align="center">DBtune agent</h1>

![Go](https://img.shields.io/badge/golang-00ADD8?&style=plastic&logo=go&logoColor=white)

The DBtune agent is a lightweight, extensible monitoring and configuration management tool for PostgreSQL databases. It collects system metrics, database performance data, and manages database configurations through a centralized DBtune software as a service.

## Features

- Real-time metric collection and monitoring
- Automatic configuration management and optimization
- Support for multiple PostgreSQL deployment types:
  - Standalone PostgreSQL servers
  - Docker containers
  - CloudNativePG (CNPG) Kubernetes operator
  - Amazon Aurora
  - Amazon RDS (closed Beta)
  - Google Cloud Platform Cloud SQL for PostgreSQL
  - Azure Flexible Server (coming soon)
- Extensible adapter architecture for custom deployments
- Concurrent metric collection with error handling
- Configuration through YAML or environment variables

## Quick start

There are multiple ways to run the DBtune agent:

- Download and run the pre-built binary
- Use our official Docker image
- Build from source (requires Go 1.23.1 or later)
- AWS Fargate / ECS

### Installation

Create a configuration file called `dbtune.yaml` file with the following contents (check the [configuration section](#configuration) for more details):

```yaml
debug: false
postgresql:
  connection_url: postgresql://user:password@localhost:5432/database
  service_name: postgresql
  include_queries: true
dbtune:
  server_url: https://app.dbtune.com
  api_key: your-api-key
  database_id: your-database-id
```

This file will be used/referenced for the pre-built binary, docker and build from source installation methods.


#### Pre-built Binary
1. Download the latest release for your platform from our [releases page](https://github.com/dbtuneai/dbtune-agent/releases)
2. Run the agent via `./dbtune-agent` in a terminal where the binary was downloaded/expanded and your `dbtune.yaml` file exists.
3. (Optional) Install a systemd service for dbtune-agent. Create a systemd service with the following config and save it as `/etc/systemd/system/dbtune-agent.service`.

	```
	[Unit]
	Description=DBtune agent
	After=network.target

	[Service]
	User=dbtune   # User that runs the dbtune-agent, can be root
	Group=dbtune  # Group to run the dbtune-agent with
	WorkingDirectory=/usr/local/bin
	ExecStart=/usr/local/bin/dbtune-agent
	Restart=always
	RestartSec=5s
	StandardOutput=journal
	StandardError=journal

	[Install]
	WantedBy=multi-user.target
	```

Make sure the user and group that you specify in the systemd service has rights to access postgres data files and also is allowed to use sudo without a password to do a systemd restart of the postgresql service e.g. `sudo systemctl restart postgres`.

Reload the unit files and enable the dbtune-agent.service:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now dbtune-agent
```

Once started you can check and verify that the dbtune-agent is running by looking at the journal, for example: `journalctl -u dbtune-agent -f`

#### Docker
1. Pull our Docker image:

	```
	docker pull public.ecr.aws/dbtune/dbtune/agent:latest
	```

2. Run the agent via `docker`
 	- With mounted config file

		```
		docker run -v $(pwd)/dbtune.yaml:/app/dbtune.yaml \
		    --name dbtune-agent \
		     public.ecr.aws/dbtune/dbtune/agent:latest
		```
 	- With environment variables

		```
		docker run \
			  -e DBT_POSTGRESQL_CONNECTION_URL=postgresql://user:password@localhost:5432/database \
			  -e DBT_DBTUNE_SERVER_URL=https://app.dbtune.com \
			  -e DBT_DBTUNE_API_KEY=your-api-key \
			  -e DBT_DBTUNE_DATABASE_ID=your-database-id \
			  -e DBT_POSTGRESQL_INCLUDE_QUERIES=true \
			  public.ecr.aws/dbtune/dbtune/agent:latest

		```

#### Build from source

To build the DBtune agent from source, you'll need Go 1.23.1 or later installed on your system.

**Prerequisites:**
- Go 1.23.1 or later ([download from golang.org](https://golang.org/dl/))
- Git (to clone the repository)

**Build steps:**

1. Clone the repository:
   ```bash
   git clone https://github.com/dbtuneai/dbtune-agent.git
   cd dbtune-agent
   ```

2. Download dependencies:
   ```bash
   go mod download
   ```

3. Build the binary:
   ```bash
   go build -o dbtune-agent ./cmd/agent.go
   ```

4. Run the agent:
   ```bash
   ./dbtune-agent
   ```

**Installation:**

After building, you can install the binary system-wide:

```bash
# Copy to a directory in your PATH
sudo cp dbtune-agent /usr/local/bin/
```

#### AWS Fargate / ECS
Follow these [README](fargate/README.md) instructions to run the agent under AWS Fargate as a service.

## Configuration

### Configuration via YAML

Create the dbtune-agent configuration and store it where dbtune-agent can access it. The dbtune-agent searches for configuration to be in one of these 3 locations, ordered by priority:
* `/etc/dbtune.yaml`
* `/etc/dbtune/dbtune.yaml`
* `./dbtune.yaml` (Relative to the path from which `dbtune-agent` was executed)

```yaml
# Basic configuration
postgresql:
  connection_url: postgresql://user:password@localhost:5432/database # Required: Database connection string
  # Restart specific variable
  # This variable is about restarting postgresql service with the default PostgreSQL adapter.
  # It might need to run as root depending on your system and service setup.
  # Check the postgres.go adapter to see which commands are getting executed.
  service_name: postgresql-17 # (required) name of the service in case of restarts
  include_queries: true # (required) to include query text when sending stats

dbtune:
  server_url: https://app.dbtune.com # Optional: DBtune server endpoint (change for self-hosted)
  api_key: your-api-key # Required: Your DBtune API key
  database_id: your-database-id # Required: Unique identifier for your database

# Optional settings
debug: false # Enable debug logging

guardrail_settings:
  memory_threshold: 90  # percentage of total memory (90 is 90% of total memory).

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

cloudsql:
  DBT_GCP_PROJECT_ID: "<your-gcp-project-id>"
  DBT_GCP_DATABASE_NAME: "<name-of-your-gcp-cloud-sql-instance>"

# CloudNativePG (CNPG) settings
cnpg:
  namespace: "<your-kubernetes-namespace>" # Required: Namespace where CNPG cluster is deployed
  pod_name: "<your-pod-name>" # Required: Specific pod to monitor (one agent per pod)
  container_name: "postgres" # Optional: Container name within the pod (defaults to "postgres")
  kubeconfig_path: "/path/to/kubeconfig" # Optional: Path to kubeconfig file (uses in-cluster config or ~/.kube/config if not specified)
```

### Environment variables

All configuration options can be set through environment variables, prefixed with `DBT_` and following a screaming snake case format:

```bash
export DBT_POSTGRESQL_CONNECTION_URL=postgresql://user:password@localhost:5432/database
export DBT_DBTUNE_SERVER_URL=http://localhost:8000
export DBT_DBTUNE_API_KEY=your-api-key
export DBT_DBTUNE_DATABASE_ID=your-database-id
export DBT_POSTGRESQL_INCLUDE_QUERIES=true

# CNPG-specific environment variables
export DBT_CNPG_NAMESPACE=<your-kubernetes-namespace>
export DBT_CNPG_POD_NAME=<your-pod-name>
export DBT_CNPG_CONTAINER_NAME=postgres  # Optional, defaults to "postgres"
export DBT_CNPG_KUBECONFIG_PATH=/path/to/kubeconfig  # Optional
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
``
