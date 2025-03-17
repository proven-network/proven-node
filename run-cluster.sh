#!/bin/bash
# Script to run the Proven Node cluster

set -e

echo "============================"
echo "Starting Proven Node Cluster"
echo "============================"

# Set up UPnP port forwarding for port 30000
echo "Setting up UPnP port forwarding for port 30000..."
if ! command -v upnpc &> /dev/null; then
  echo "upnpc not found. Installing miniupnpc package..."
  if command -v apt-get &> /dev/null; then
    sudo apt-get update && sudo apt-get install -y miniupnpc
  elif command -v brew &> /dev/null; then
    brew install miniupnpc
  else
    echo "Unable to install miniupnpc automatically. Please install it manually."
    exit 1
  fi
fi

# Get the local IP address in a cross-platform way
if [[ "$(uname)" == "Darwin" ]]; then
  # macOS way to get primary IP
  LOCAL_IP=$(ifconfig | grep "inet " | grep -v 127.0.0.1 | head -n 1 | awk '{print $2}')
else
  # Linux way
  LOCAL_IP=$(hostname -I | awk '{print $1}')
fi
echo "Local IP: $LOCAL_IP"

# Try to set up port forwarding
echo "Setting up UPnP port forwarding from external port 30001 to internal $LOCAL_IP:30001..."
upnpc -a "$LOCAL_IP" 30001 30001 TCP "Proven Node"
echo "UPnP port forwarding setup complete"

# Enable Docker BuildKit
export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1

# Build and run the Docker containers
cd integration-test

echo "Building containers..."
docker-compose build --build-arg BUILDKIT_INLINE_CACHE=1

echo "Starting node cluster"
# Run only the node containers, not the test-client
docker-compose up bulbasaur charmander squirtle

# This line will only be reached when the user stops the cluster with Ctrl+C
echo "Cluster stopped. Cleaning up..."

# Clean up UPnP port forwarding
echo "Cleaning up UPnP port forwarding..."
upnpc -d 30001 TCP

echo "Shutdown complete" 
