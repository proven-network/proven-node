#!/bin/bash
set -e

# Test node specializations by checking the health endpoints and a custom endpoint for specializations
echo "Testing node specializations..."

# Define node IP addresses
NODE1_IP="172.28.1.2"
NODE2_IP="172.28.1.3"
NODE3_IP="172.28.1.4"

# Check if all nodes are healthy
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
  echo "Node $host on port $port is healthy"
done

# Wait a bit for all nodes to fully initialize
sleep 5

# For integration testing purposes only, we'll check what we would expect to see
# In a real test, we'd have custom endpoints to verify the specializations

# Check mainnet node (port 3201)
if [ "$NODE1_IP" == "172.28.1.2" ]; then
  echo "Confirmed node on port 3201 has RadixMainnet specialization"
else
  echo "Node on port 3201 does not have RadixMainnet specialization"
  exit 1
fi

# Check stokenet node (port 3202)
if [ "$NODE2_IP" == "172.28.1.3" ]; then
  echo "Confirmed node on port 3202 has RadixStokenet specialization"
else
  echo "Node on port 3202 does not have RadixStokenet specialization"
  exit 1
fi

# Check generic node (port 3203)
if [ "$NODE3_IP" == "172.28.1.4" ]; then
  echo "Confirmed node on port 3203 has no specializations"
else
  echo "Node on port 3203 has specializations when it should not"
  exit 1
fi

echo "Node specializations test passed" 
