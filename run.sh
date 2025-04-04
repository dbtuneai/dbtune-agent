#!/bin/sh
CONTAINER_NAME="custom-agent"
IMAGE_NAME="custom-agent-image"

# Function to start the container and tail logs
start_and_tail() {
  # Check if the container exists (running or exited)
  if docker ps -a --filter "name=$CONTAINER_NAME" | grep -q "$CONTAINER_NAME"; then
    # Check if it's running
    if docker ps --filter "name=$CONTAINER_NAME" | grep -q "$CONTAINER_NAME"; then
      echo "Container '$CONTAINER_NAME' is already running. Tail logs."
      docker logs -f "$CONTAINER_NAME"
    else
      # Container exists but is not running, remove it and start again
      echo "Container '$CONTAINER_NAME' exists but is not running. Removing and restarting..."
      docker rm "$CONTAINER_NAME" > /dev/null 2>&1
      start_new_container
    fi
  else
    # Container does not exist, start a new one
    start_new_container
  fi
}

# Function to start a new container
start_new_container() {
    echo "Starting container '$CONTAINER_NAME' and tailing logs..."
    if ! docker build -t "$IMAGE_NAME" .; then
      echo "Error: Docker build failed."
      exit 1 # Exit with an error code
    fi
    docker run \
        --name "$CONTAINER_NAME" \
        --network cloud-backend-network \
        -d \
        -v "$(pwd)/dbtune.yaml:/app/dbtune.yaml" \
        -v /var/run/docker.sock:/var/run/docker.sock \
        "$IMAGE_NAME" --aiven

    echo "Started container, logging container"
    docker logs -f "$CONTAINER_NAME"
}

# Trap Ctrl+C to cleanly stop the container
trap 'echo "Stopping and removing container..."; docker stop "$CONTAINER_NAME" > /dev/null 2>&1; docker rm "$CONTAINER_NAME" > /dev/null 2>&1; exit 0' INT

# Run the function
start_and_tail
