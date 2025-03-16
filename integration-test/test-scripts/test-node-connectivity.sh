#!/bin/bash
set -e

# Test that we can connect to all nodes
echo "Testing connectivity to all nodes..."

# Define node IP addresses
NODE1_IP="172.28.1.2"
NODE2_IP="172.28.1.3"
NODE3_IP="172.28.1.4"

for port in 3201 3202 3203; do
  case $port in
    3201) host=$NODE1_IP ;;
    3202) host=$NODE2_IP ;;
    3203) host=$NODE3_IP ;;
  esac
  
  response=$(curl -s "http://$host:$port/health")
  if [ $? -ne 0 ]; then
    echo "Failed to connect to node $host on port $port"
    exit 1
  fi
  echo "Successfully connected to node $host on port $port"
done

echo "Node connectivity test passed" 
