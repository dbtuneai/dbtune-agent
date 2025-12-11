# Yaml config
```yaml
postgresql:
  connection_url: postgresql://user:password@cluster-rw:5432/database # Database connection string
  include_queries: true # Whether to include query text when sending stats

cnpg:
  namespace: "" # REQUIRED: The Kubernetes namespace where your CNPG cluster is deployed
  cluster_name: "" # REQUIRED: The name of your CNPG cluster resource
  pod_name: "" # Optional: Specific pod name. If not set, auto-discovers the current primary pod
  container_name: "postgres" # Optional: Container name within the pod. Defaults to "postgres"
  kubeconfig_path: "" # Optional: Path to kubeconfig file. Defaults to ~/.kube/config or in-cluster config

dbtune:
  server_url: https://app.dbtune.com # DBtune server endpoint
  api_key: your-api-key # Your DBtune API key
  database_id: your-database-id # Unique identifier for your database

# Optional
guardrail_settings:
  memory_threshold: 90  # The percentage at which the dbtune-agent triggers a memory guardrail
                        # DBtune will act to prevent an OOM if this threshold is reached
                        # during tuning

debug: false # Enable debug logging
```

# Environment variables
```bash
# Dbtune specific
export DBT_DBTUNE_SERVER_URL=https://app.dbtune.com
export DBT_DBTUNE_API_KEY=your-api-key
export DBT_DBTUNE_DATABASE_ID=your-database-id
export DBT_POSTGRESQL_INCLUDE_QUERIES=true

# Your database specific
export DBT_POSTGRESQL_CONNECTION_URL=postgresql://user:password@cluster-rw:5432/database

# CNPG specific (REQUIRED)
export DBT_CNPG_NAMESPACE=default
export DBT_CNPG_CLUSTER_NAME=my-cluster

# CNPG specific (OPTIONAL - only set if needed)
export DBT_CNPG_POD_NAME=my-cluster-1  # If not set, auto-discovers primary pod
export DBT_CNPG_CONTAINER_NAME=postgres  # Defaults to "postgres"
export DBT_CNPG_KUBECONFIG_PATH=/path/to/kubeconfig  # Defaults to ~/.kube/config or in-cluster
```
