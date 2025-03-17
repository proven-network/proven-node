#!/bin/bash
# Script to run the Proven Node cluster

set -e

echo "============================"
echo "Starting Proven Node Cluster"
echo "============================"

# Set up UPnP port forwarding
./upnp-setup.sh

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
./../upnp-teardown.sh

echo "Shutdown complete" 
