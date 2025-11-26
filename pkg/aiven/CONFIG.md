# Yaml config
```yaml
postgresql:
  connection_url: postgresql://user:password@localhost:5432/database # Database connection string
  include_queries: true # Whether to include query text when sending stats

aiven:
  AIVEN_API_TOKEN: "" # API token obtained from the Aiven console.
  AIVEN_PROJECT_NAME: "" # The project name under which you have your PostgreSQL instance
  AIVEN_SERVICE_NAME: "" # The service name for your PostgreSQL instance

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
export DBT_POSTGRESQL_INCLUDE_QUERIES=true

# Your database specific
export DBT_POSTGRESQL_CONNECTION_URL=postgresql://user:password@localhost:5432/database 
export DBT_AIVEN_API_TOKEN="" 
export DBT_AIVEN_PROJECT_NAME=""
export DBT_AIVEN_SERVICE_NAME=""
```