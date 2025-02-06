#!/usr/bin/env bash
# This script is used to start a local testing database for development purposes.
# It requires SD and JQ to be installed.
# It used the DBtune platform and requires two env variables to be set:
# - DBT_DBTUNE_API_KEY
# - DBT_DBTUNE_SERVER_URL
#
# The script will:
# 1. Create a new database in the DBtune platform you are targeting and grab the ID
# 2. Start a new local compose file that will be used to start the database with an agent connected to it

set -e


if [ -z "${DBT_DBTUNE_API_KEY}" ] || [ -z "${DBT_DBTUNE_SERVER_URL}" ]; then
  echo "Error: DBT_DBTUNE_API_KEY and DBT_DBTUNE_SERVER_URL must be set"
  exit 1
fi

# Check if jq is installed
if ! command -v jq &> /dev/null; then
  echo "Error: jq is required but not installed. Please install jq first."
  exit 1
fi

# Check if sd is installed
if ! command -v sd &> /dev/null; then
  echo "Error: sd is required but not installed. Please install sd first."
  exit 1
fi

UUID=$(uuidgen | tr '[:upper:]' '[:lower:]')
DB_NAME="testing-db-${UUID}"
echo "Generated DB name: ${DB_NAME}"

# Create database through API
echo "Creating database through API..."
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${DBT_DBTUNE_SERVER_URL}/api/v1/databases" \
  -H "DBTUNE-API-KEY: ${DBT_DBTUNE_API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "'"${DB_NAME}"'",
    "restart_policy": true,
    "db_engine": "postgresql",
    "cloud_provider": "AWS",
    "hosting": "on_prem",
    "tuning_target": "average_query_runtime",
    "iteration_duration": 0.1
  }')

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_CODE" != "200" ] && [ "$HTTP_CODE" != "201" ]; then
  echo "Error creating database. HTTP Code: $HTTP_CODE"
  echo "Response: $BODY"
  exit 1
fi

# Extract the database UUID from the response using jq
DATABASE_UUID=$(echo "$BODY" | jq -r '.uuid')
if [ -z "$DATABASE_UUID" ] || [ "$DATABASE_UUID" = "null" ]; then
  echo "Error: Could not extract database UUID from response"
  echo "Response: $BODY"
  exit 1
fi
echo "Database UUID: ${DATABASE_UUID}"

# Create volumes directory if it doesn't exist
echo "Creating volumes directory..."
mkdir -p volumes

# Convert localhost to host.docker.internal for the agent
# for local development
AGENT_SERVER_URL=$(echo "${DBT_DBTUNE_SERVER_URL}" | sed 's/localhost/host.docker.internal/')

# Use sd to replace placeholders in the template
echo "Creating docker-compose file..."
cp templates/docker-compose.yml "docker-compose.${DB_NAME}.yml"
sd "DB_NAME" "${DB_NAME}" "docker-compose.${DB_NAME}.yml"
sd "DB_DATA_DIR" "${DB_NAME}" "docker-compose.${DB_NAME}.yml"
sd "DB_NETWORK_NAME" "${DB_NAME}_net" "docker-compose.${DB_NAME}.yml"
sd "SD_API_KEY" "${DBT_DBTUNE_API_KEY}" "docker-compose.${DB_NAME}.yml"
sd "SD_SERVER_URL" "${AGENT_SERVER_URL}" "docker-compose.${DB_NAME}.yml"
sd "DATABASE_UUID" "${DATABASE_UUID}" "docker-compose.${DB_NAME}.yml"

# Start the services
echo "Starting services..."
docker compose -f "docker-compose.${DB_NAME}.yml" up -d

echo "Setup completed successfully 🫡"