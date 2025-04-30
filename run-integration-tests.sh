#!/bin/bash
# Main script to run the integration tests

set -e

# Default log level
RUST_LOG="info"

# Parse named arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --log)
      RUST_LOG="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

echo "======================================"
echo "Starting Proven Node Integration Tests"
echo "Log level: ${RUST_LOG}"
echo "======================================"

# Set up UPnP port forwarding
./upnp-setup.sh

# export variables for docker-compose
export COMPOSE_DOCKER_CLI_BUILD=1
export DOCKER_BUILDKIT=1
export RUST_LOG

# Build the Docker images and run the tests
cd integration-test

docker-compose build --build-arg BUILDKIT_INLINE_CACHE=1
docker-compose up --abort-on-container-exit

# Check if tests passed
exit_code=$?

# Clean up UPnP port forwarding
./../upnp-teardown.sh

if [ $exit_code -eq 0 ]; then
  echo "✅ All tests passed"
  docker-compose down
  exit 0
else
  echo "❌ Tests failed"
  docker-compose logs
  docker-compose down
  exit 1
fi 
