#!/bin/bash
# Main test runner script

set -e

echo "====================================="
echo "Starting Proven Node Integration Tests"
echo "====================================="

# Run each test script
for test_script in /test-scripts/test-*.sh; do
  echo "Running test: $test_script"
  bash "$test_script"
  if [ $? -eq 0 ]; then
    echo "✅ Test passed: $test_script"
  else
    echo "❌ Test failed: $test_script"
    exit 1
  fi
  echo ""
done

echo "All tests passed! ✅"
exit 0 
