#!/bin/sh
CONTAINER_NAME="custom-agent"
IMAGE_NAME="custom-agent-image"

# Function to start the container and tail logs
start_and_tail() {
  # Check if the container is already running
  if docker ps --filter "name=$CONTAINER_NAME" | grep -q "$CONTAINER_NAME"; then
    echo "Container '$CONTAINER_NAME' is already running. Tail logs."
    docker logs -f "$CONTAINER_NAME"
  else
    echo "Starting container '$CONTAINER_NAME' and tailing logs..."
    docker build -t custom-agent-image .
    docker run --restart always \
      --name "$CONTAINER_NAME" \
      --network cloud-backend-network \
      -d \
      -v $(pwd)/dbtune.yaml:/app/dbtune.yaml \
      -v /var/run/docker.sock:/var/run/docker.sock \
      "$IMAGE_NAME" --aiven

    echo "Logging container"
    docker logs -f "$CONTAINER_NAME"
  fi
}

# Trap Ctrl+C to cleanly stop the container
trap 'echo "Stopping and removing container..."; docker stop "$CONTAINER_NAME" > /dev/null 2>&1; docker rm "$CONTAINER_NAME" > /dev/null 2>&1; exit 0' INT

# Run the function
start_and_tail
