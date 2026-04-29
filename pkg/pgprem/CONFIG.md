# Yaml config
```yaml
postgresql:
  connection_url: postgresql://user:password@localhost:5432/database # Database connection string
  service_name: "postgresql-17" # (required for restart unless `use_restart_command` is true) name of your database service running under systemctl
  # use_restart_command: false  # When true, restarts are performed by directly executing the script at
                                # /opt/dbtune-agent/restart.sh (no shell interpolation). Takes precedence
                                # over `service_name`. The script must exist and be executable by the agent.
  allow_restart: false  # Allow the agent to restart PostgreSQL. Defaults to false.
                        # When true, either `service_name` must be set or `use_restart_command` must be true
                        # (with /opt/dbtune-agent/restart.sh present).

dbtune:
  server_url: https://app.dbtune.com # DBtune server endpoint
  api_key: your-api-key # Your DBtune API key
  database_id: your-database-id # Unique identifier for your database

# Optional
guardrail_settings:
  memory_threshold: 90  # The percentage at which the dbtune-agent triggers a memory gaurdrail
                        # DBtune will act to prevent an OOM if this threshold is reached
                        # during tuning

debug: false # Enable debug logging
```

# Environment variables
```bash
# Dbtune specific
export DBT_DBTUNE_SERVER_URL=http://localhost:8000
export DBT_DBTUNE_API_KEY=your-api-key
export DBT_DBTUNE_DATABASE_ID=your-database-id

# Your database specific
export DBT_POSTGRESQL_CONNECTION_URL=postgresql://user:password@localhost:5432/database
export DBT_POSTGRESQL_SERVICE_NAME=
# export DBT_POSTGRESQL_USE_RESTART_COMMAND=false  # When true, restarts execute /opt/dbtune-agent/restart.sh directly. Takes precedence over SERVICE_NAME.
export DBT_POSTGRESQL_ALLOW_RESTART=false  # Set to true to allow PostgreSQL restarts.
                                           # When true, either SERVICE_NAME must be set or USE_RESTART_COMMAND must be true.
```