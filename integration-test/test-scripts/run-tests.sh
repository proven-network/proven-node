#!/bin/bash
set -e

echo "===== Starting Integration Tests ====="

# Define node IP addresses
NODE1_IP="172.28.1.2"
NODE2_IP="172.28.1.3"
NODE3_IP="172.28.1.4"

# Wait for nodes to be ready
echo "Waiting for nodes to be ready..."
for port in 3201 3202 3203; do
  case $port in
    3201) host=$NODE1_IP ;;
    3202) host=$NODE2_IP ;;
    3203) host=$NODE3_IP ;;
  esac
  
  until curl -s "http://$host:$port/health" > /dev/null; do
    echo "Waiting for node $host on port $port..."
    sleep 2
  done
  echo "Node $host on port $port is ready"
done

# Run the individual test scripts
for test in /test-scripts/test-*.sh; do
  if [ -f "$test" ] && [ "$test" != "/test-scripts/run-tests.sh" ]; then
    echo "Running test: $test"
    bash "$test"
    
    if [ $? -eq 0 ]; then
      echo "✅ Test passed: $test"
    else
      echo "❌ Test failed: $test"
      exit 1
    fi
  fi
done

echo "===== All Integration Tests Completed Successfully =====" 
