# Yaml config
```yaml
postgresql:
  connection_url: postgresql://user:password@localhost:5432/database # Database connection string
  allow_restart: false  # Allow the agent to restart PostgreSQL. Defaults to false.

# Only for plain RDS
# The parameter group is auto-discovered from the instance. The agent refuses
# to apply tuning changes when the attached group is a `default.*` group
# (AWS does not allow modifying those); attach a custom parameter group.
rds:
  # AWS credentials are optional
  # If not provided, the agent will use the default AWS credential chain, which includes WebIdentity tokens
  AWS_ACCESS_KEY_ID: ""
  AWS_SECRET_ACCESS_KEY: ""
  RDS_DATABASE_IDENTIFIER: "" # The Writer instance of the Aurora cluster

# Only for Aurora RDS
# Same auto-discovery as plain RDS; both the instance and cluster parameter
# group names are reported to DBtune in system info.
rds-aurora:
  # AWS credentials are optional
  # If not provided, the agent will use the default AWS credential chain, which includes WebIdentity tokens
  AWS_ACCESS_KEY_ID: ""
  AWS_SECRET_ACCESS_KEY: ""
  RDS_DATABASE_IDENTIFIER: "" # The Writer instance of the Aurora cluster


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
export RDS_DATABASE_IDENTIFIER=
export DBT_AWS_REGION=
export DBT_AWS_ACCESS_KEY_ID=  # Optional, see above in yaml config
export DBT_AWS_SECRET_ACCESS_KEY=  # Optional, see above in yaml config
export DBT_POSTGRESQL_ALLOW_RESTART=false  # Set to true to allow PostgreSQL restarts
```