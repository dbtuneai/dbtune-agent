#!/bin/bash

# Function to detect OS and architecture
get_platform() {
    # Get OS (converted to lowercase)
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')

    # Get architecture
    ARCH=$(uname -m)

    # Convert architecture names to GoReleaser naming convention
    case "${ARCH}" in
        x86_64)
            ARCH="x86_64"
            ;;
        aarch64)
            ARCH="arm64"
            ;;
        armv7l)
            ARCH="arm"
            ;;
    esac

    # Handle macOS naming
    if [ "${OS}" = "darwin" ]; then
        OS="Darwin"
    elif [ "${OS}" = "linux" ]; then
        OS="Linux"
    fi

    echo "${OS}_${ARCH}"
}

# Get the platform identifier
PLATFORM=$(get_platform)

# Create temporary directory
TMP_DIR=$(mktemp -d)
cd "${TMP_DIR}"

echo "Detected platform: ${PLATFORM}"

# Get the latest release URL and version
LATEST_VERSION=$(curl -H "Accept: application/vnd.github+json" -s https://api.github.com/repos/dbtuneai/dbtune-agent/releases/latest | grep "tag_name" | cut -d '"' -f 4)
DOWNLOAD_URL="https://github.com/dbtuneai/dbtune-agent/releases/download/${LATEST_VERSION}/dbtune-agent_${PLATFORM}.tar.gz"

echo "Downloading DBtuneAgent ${LATEST_VERSION} for ${PLATFORM}..."

# Download the archive
if ! curl -H "Accept: application/vnd.github+json" -L -o dbtune-agent.tar.gz "${DOWNLOAD_URL}"; then
    echo "Error: Failed to download goreleaser"
    exit 1
fi

# Extract the archive
if ! tar xzf dbtune-agent.tar.gz; then
    echo "Error: Failed to extract archive"
    exit 1
fi

# Make the binary executable
if ! chmod +x dbtune-agent; then
    echo "Error: Failed to make goreleaser executable"
    exit 1
fi

# Move to a suitable location (you might need sudo for these operations)
echo "Installing dbtune-agent to /usr/local/bin (requires sudo)"
if ! sudo mv dbtune-agent /usr/local/bin/; then
    echo "Error: Failed to move dbtune-agent to /usr/local/bin/"
    exit 1
fi

# Clean up
cd - > /dev/null
rm -rf "${TMP_DIR}"

echo "DBtune agent ${LATEST_VERSION} has been successfully installed to /usr/local/bin/dbtune-agent"
echo "You can now run 'dbtune-agent' from anywhere in your terminal"