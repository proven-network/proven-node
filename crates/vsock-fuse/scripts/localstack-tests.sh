#!/bin/bash
set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Parse arguments
TEST_NAME="${1:-}"

# Show usage
usage() {
    echo -e "${BLUE}LocalStack Integration Test Runner${NC}"
    echo -e "${BLUE}===================================${NC}"
    echo ""
    echo "Usage: $0 [test_name]"
    echo ""
    echo "Run LocalStack integration tests with automatic setup and cleanup."
    echo ""
    echo "Options:"
    echo "  [test_name]   Run a specific test (optional)"
    echo "  -h, --help    Show this help message"
    echo ""
    echo "Available tests:"
    echo "  test_cold_tier_storage      - Basic S3 blob storage operations"
    echo "  test_multipart_upload       - Large file uploads using S3 multipart"
    echo "  test_cold_tier_caching      - Hot tier caching of cold tier data"
    echo "  test_s3_failure_recovery    - Error handling when S3 is unavailable"
    echo "  test_concurrent_migrations  - Concurrent blob operations"
    echo "  test_encryption_with_s3     - Data encryption before storing in S3"
    echo ""
    echo "Examples:"
    echo "  $0                          # Run all LocalStack tests"
    echo "  $0 test_cold_tier_storage   # Run a specific test"
    exit 0
}

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    cd "$PROJECT_DIR"
    
    # Stop and remove LocalStack
    if docker-compose -f docker-compose.localstack.yml ps -q 2>/dev/null | grep -q .; then
        echo "Stopping LocalStack..."
        docker-compose -f docker-compose.localstack.yml down -v || true
    fi
    
    # Remove any dangling containers
    docker ps -a | grep localstack | awk '{print $1}' | xargs -r docker rm -f 2>/dev/null || true
    
    # Clean up any stale volumes
    docker volume ls | grep localstack | awk '{print $2}' | xargs -r docker volume rm 2>/dev/null || true
    
    echo -e "${GREEN}Cleanup complete${NC}"
}

# Set trap to cleanup on exit
trap cleanup EXIT INT TERM

# Main script
main() {
    # Check for help flag
    if [[ "${TEST_NAME}" == "-h" ]] || [[ "${TEST_NAME}" == "--help" ]]; then
        usage
    fi
    
    echo -e "${GREEN}LocalStack Integration Test Runner${NC}"
    echo "=================================="
    
    # Change to project directory
    cd "$PROJECT_DIR"
    
    # Check if Docker is running
    if ! docker info >/dev/null 2>&1; then
        echo -e "${RED}Error: Docker is not running. Please start Docker and try again.${NC}"
        exit 1
    fi
    
    # Check if docker-compose is available
    if ! command -v docker-compose &> /dev/null; then
        echo -e "${RED}Error: docker-compose is not installed. Please install it and try again.${NC}"
        exit 1
    fi
    
    # Stop any existing LocalStack containers
    echo -e "\n${YELLOW}Stopping any existing LocalStack containers...${NC}"
    docker-compose -f docker-compose.localstack.yml down -v 2>/dev/null || true
    docker ps -a | grep localstack | awk '{print $1}' | xargs -r docker rm -f 2>/dev/null || true
    
    # Clean up any stale volumes that might be causing issues
    echo "Cleaning up stale volumes..."
    docker volume ls | grep localstack | awk '{print $2}' | xargs -r docker volume rm 2>/dev/null || true
    
    # Wait a moment for cleanup to complete
    sleep 2
    
    # Start LocalStack
    echo -e "\n${YELLOW}Starting LocalStack...${NC}"
    docker-compose -f docker-compose.localstack.yml up -d
    
    # Wait for LocalStack to be ready
    echo -e "\n${YELLOW}Waiting for LocalStack to be ready...${NC}"
    MAX_RETRIES=30
    RETRY_COUNT=0
    
    # Initial wait for container to start
    sleep 3
    
    while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
        if curl -s http://localhost:4566/_localstack/health 2>/dev/null | grep -q '"s3":[[:space:]]*"available"'; then
            echo -e "${GREEN}LocalStack is ready!${NC}"
            # Extra wait to ensure S3 service is fully initialized
            sleep 2
            break
        fi
        
        RETRY_COUNT=$((RETRY_COUNT + 1))
        echo "Waiting for LocalStack... (attempt $RETRY_COUNT/$MAX_RETRIES)"
        sleep 2
    done
    
    if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
        echo -e "${RED}Error: LocalStack failed to start after $MAX_RETRIES attempts${NC}"
        exit 1
    fi
    
    # Set environment variables
    export AWS_ACCESS_KEY_ID=test
    export AWS_SECRET_ACCESS_KEY=test
    export AWS_ENDPOINT_URL=http://localhost:4566
    export AWS_EC2_METADATA_DISABLED=true
    export AWS_DEFAULT_REGION=us-east-1
    export AWS_ENDPOINT_URL_S3=http://localhost:4566
    
    # Run the tests
    if [ -n "$TEST_NAME" ]; then
        echo -e "\n${YELLOW}Running LocalStack integration test: ${TEST_NAME}${NC}"
        TEST_CMD="cargo test --test localstack_integration --features testing $TEST_NAME -- --ignored --nocapture --exact"
    else
        echo -e "\n${YELLOW}Running all LocalStack integration tests...${NC}"
        TEST_CMD="cargo test --test localstack_integration --features testing -- --ignored --nocapture"
    fi
    
    echo "Environment variables set:"
    echo "  AWS_ENDPOINT_URL=$AWS_ENDPOINT_URL"
    echo "  AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION"
    echo "  AWS_EC2_METADATA_DISABLED=$AWS_EC2_METADATA_DISABLED"
    echo ""
    
    # Run tests with proper error handling
    if $TEST_CMD; then
        echo -e "\n${GREEN}Tests passed!${NC}"
        exit 0
    else
        echo -e "\n${RED}Tests failed!${NC}"
        
        # Show LocalStack logs on failure
        echo -e "\n${YELLOW}LocalStack logs:${NC}"
        docker-compose -f docker-compose.localstack.yml logs --tail=50
        
        exit 1
    fi
}

# Run main function
main "$@"