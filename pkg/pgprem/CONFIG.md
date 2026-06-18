# Yaml config
```yaml
postgresql:
  connection_url: postgresql://user:password@localhost:5432/database # Database connection string
  service_name: "postgresql-17" # (required for restart unless `restart_script_path` is set) name of your database service running under systemctl
  # restart_script_path: ""     # Optional. Path to an executable restart script. When set, restarts execute this
                                # script directly (no shell, no arguments) instead of `systemctl restart`. Takes
                                # precedence over `service_name`. The file must exist and be executable by the agent.
  allow_restart: false  # Allow the agent to restart PostgreSQL. Defaults to false.
                        # When true, either `service_name` must be set or `restart_script_path` must point to an
                        # existing executable script.

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
# export DBT_POSTGRESQL_RESTART_SCRIPT_PATH=  # Optional. Path to an executable restart script. When set, restarts execute it directly. Takes precedence over SERVICE_NAME.
export DBT_POSTGRESQL_ALLOW_RESTART=false  # Set to true to allow PostgreSQL restarts.
                                           # When true, either SERVICE_NAME must be set or RESTART_SCRIPT_PATH must point to an existing executable.
```