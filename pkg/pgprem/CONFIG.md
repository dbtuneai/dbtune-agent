# Yaml config
```yaml
postgresql:
  connection_url: postgresql://user:password@localhost:5432/database # Database connection string
  service_name: "postgresql-17" # (required for restart unless `restart_command` is set) name of your database service running under systemctl
  # restart_command: "/usr/local/bin/restart-pg.sh" # Optional shell command executed via `sh -c` to restart PostgreSQL. Takes precedence over `service_name` when set.
  allow_restart: false  # Allow the agent to restart PostgreSQL. Defaults to false.
                        # When true, at least one of `service_name` or `restart_command` must be set.

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
# export DBT_POSTGRESQL_RESTART_COMMAND=  # Optional: shell command executed via `sh -c`. Takes precedence over SERVICE_NAME.
export DBT_POSTGRESQL_ALLOW_RESTART=false  # Set to true to allow PostgreSQL restarts.
                                           # When true, at least one of SERVICE_NAME or RESTART_COMMAND must be set.
```