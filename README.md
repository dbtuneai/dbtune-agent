<h1 align="center">DBtune agent</h1>

![Go](https://img.shields.io/badge/golang-00ADD8?&style=plastic&logo=go&logoColor=white)

The DBtune agent is a lightweight, extensible monitoring and configuration management tool for PostgreSQL databases. It collects system metrics, database performance data, and manages database configurations through a centralized DBtune software as a service.

## PostgreSQL providers supported

- PostgreSQL -
  [docs](https://docs.dbtune.com/postgresql),
  [config](pkg/pgprem/CONFIG.md)
- AWS Aurora PostgreSQL -
  [docs](https://docs.dbtune.com/aws-aurora),
  [config](pkg/rds/CONFIG.md)
- AWS RDS PostgreSQL -
  [docs](https://docs.dbtune.com/aws-rds),
  [config](pkg/rds/CONFIG.md)
- Google Cloud SQL -
  [docs](https://docs.dbtune.com/google-cloud-sql),
  [config](pkg/cloudsql/CONFIG.md)
- Aiven -
  [docs](https://docs.dbtune.com/aiven),
  [config](pkg/aiven/CONFIG.md)
- Azure Flexible Server -
  [docs](https://docs.dbtune.com/azure-flex-server),
  [config](pkg/azureflex/CONFIG.md)
- CloudNativePG -
  [docs](https://docs.dbtune.com/cnpg),
  [config](pkg/cnpg/CONFIG.md)

## Quick start
You can use the binary or our docker image to run the `dbtune-agent`.
[Configuration](#Configuration) is done with a `dbtune.yaml` and is required for running.
[See here for more advanced deployment options](#advanced-deployments)

> [!IMPORTANT]
> Please check the relevant documentation page for your provider for the pre-requisites required for `dbtune-agent` to operate properly.

### Docker

```bash
docker pull --platform linux/amd64 public.ecr.aws/dbtune/dbtune/agent:latest

# Run with a dbtune.yaml
docker run \
    -v $(pwd)/dbtune.yaml:/app/dbtune.yaml \
    --name dbtune-agent \
    public.ecr.aws/dbtune/dbtune/agent:latest

# OR... run with environment variables.
docker run \
    -e DBT_POSTGRESQL_CONNECTION_URL=postgresql://user:password@localhost:5432/database \
    -e DBT_DBTUNE_SERVER_URL=https://app.dbtune.com \
    -e DBT_DBTUNE_API_KEY=your-api-key \
    -e DBT_DBTUNE_DATABASE_ID=your-database-id \
    public.ecr.aws/dbtune/dbtune/agent:latest
```

### Binary
You can use the one-liner below to fetch the latest binary for your system, from our [releases page](https://github.com/dbtuneai/dbtune-agent/releases).

```bash
curl https://raw.githubusercontent.com/dbtuneai/dbtune-agent/refs/heads/main/setup.sh > /tmp/dbtune-agent.sh && sh /tmp/dbtune-agent.sh
./dbtune-agent
```

Alternatively, you can [build from source](#build-from-source).

You can further deploy this as a [systemd service](#systemd).

## Configuration
Configuration can be done via a `dbtune.yaml` file, which is looked up in the following places,
ordered by priority:
* `/etc/dbtune.yaml`
* `/etc/dbtune/dbtune.yaml`
* `./dbtune.yaml` (Relative to the path from which `dbtune-agent` was executed)

Each PostgreSQL provider has [different configuration options](#PostgreSQL-providers-supported), so please refer to their specific configuration options.
Please also take note of any pre-requisites required for the `dbtune-agent` to be able to read
system metrics.

The shared options for the `dbtune.yaml` are listed below:

```yaml
# dbtune.yaml
postgresql:
  connection_url: postgresql://user:password@localhost:5432/database # Connection url to your database
  allow_restart: false  # Whether to allow the agent to restart PostgreSQL when applying
                        # configuration changes that require a restart. Defaults to false.

# Query text is collected by the pg_stat_statements collector (enabled by
# default). To opt out, disable it on the collector:
# collectors:
#   pg_stat_statements:
#     include_queries: false  # or set DBT_COLLECTOR_PG_STAT_STATEMENTS_INCLUDE_QUERIES=false

# Optional
guardrail_settings:
  memory_threshold: 90  # The percentage at which the dbtune-agent triggers a memory gaurdrail
                        # DBtune will act to prevent an OOM if this threshold is reached
                        # during tuning.
dbtune:
  server_url: https://app.dbtune.com
  api_key:  "" # Provided when you create a database on DBtune
  database_id: "" # Provided when you create a database on DBtune

debug: false
```

The shared options for environment variables are listed below:

```bash
export DBT_DBTUNE_SERVER_URL="https://app.dbtune.com"
export DBT_DBTUNE_API_KEY=""
export DBT_DBTUNE_DATABASE_ID=""
export DBT_POSTGRESQL_ALLOW_RESTART=false  # Set to true to allow PostgreSQL restarts
export DBT_POSTGRESQL_USE_RESTART_COMMAND=false  # Set to true use a custom restart script.

# Your database specific
export DBT_POSTGRESQL_CONNECTION_URL=postgresql://user:password@localhost:5432/database
```

## Advanced deployments

### Build from source
To build the DBtune agent from source, you'll need Go 1.23.1 or later installed on your system.

```bash
git clone https://github.com/dbtuneai/dbtune-agent.git
cd dbtune-agent

# Download dependencies and build
go mod download
go build -o dbtune-agent ./cmd/agent.go
```

### Systemd
Create a systemd service with the following config and save it as `/etc/systemd/system/dbtune-agent.service`.

```bash
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

### Using a custom restart script

As mentioned above, one can specify a custom restart script in case restarting the database
can't be done through `systemctl`.

To do so, you will need to export the following environment variable
```bash
export DBT_POSTGRESQL_USE_RESTART_COMMAND=true
```
Afterwards, the agent will use a script located in `/opt/dbtune-agent/restart.sh` which
must be executable (i.e. `chmod +x`).

The script must signal success with exit code `0` and failure with any non-zero exit code. Output on stdout/stderr is logged on failure but does not by itself indicate failure — many tools (including `pg_ctl`) write to stderr during normal operation.

An example of such a restart script using `pg_ctl` instead of `systemctl`:
```sh 
#!/bin/bash
set -eo pipefail

# Restarts the database using pg_ctl, and a timeout of 10min.
# Assumes PGDATA points to the data folder of postgres.
exec gosu postgres pg_ctl restart -D "$PGDATA" -m fast -w -t 600
```


### AWS Fargate / ECS
Follow these [README](fargate/README.md) instructions to run the agent under AWS Fargate as a service.

## Metric collection

The agent collects essential metrics required for DBtune's optimization engine, you can find more information about the metrics [here](https://docs.dbtune.com/agent-monitoring-and-privacy).

## How to contribute

- Fork the repo and create a new branch
- Then make a PR to the main branch

### Getting help

- Check our [documentation](https://docs.dbtune.com)
- Email support: support at dbtune.com

## License

See github.com/dbtuneai/agent//blob/main/LICENSE
