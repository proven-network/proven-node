#!/bin/bash
# Main script to run the integration tests

set -e

echo "========================================"
echo "Starting Proven Node Integration Tests"
echo "========================================"

# Build the Docker images and run the tests
cd integration-test
docker-compose build
docker-compose up --abort-on-container-exit

# Check if tests passed
exit_code=$?
if [ $exit_code -eq 0 ]; then
  echo "✅ All tests passed"
  docker-compose down -v
  exit 0
else
  echo "❌ Tests failed"
  docker-compose logs
  docker-compose down -v
  exit 1
fi 
