#!/bin/bash
# Test that node specializations are correctly assigned

set -e

echo "Testing node specializations..."

# Wait for all nodes to start up
sleep 5

# Test the mainnet node (port 3201)
echo "Testing mainnet node (node-mainnet:3201)..."
MAINNET_RESPONSE=$(curl -s http://node-mainnet:3201/)
echo "Mainnet response: $MAINNET_RESPONSE"
if echo "$MAINNET_RESPONSE" | grep -q '"specializations":\["RadixMainnet"\]'; then
  echo "✅ Mainnet node has RadixMainnet specialization"
else
  echo "❌ Mainnet node does not have RadixMainnet specialization"
  exit 1
fi

# Test the stokenet node (port 3202)
echo "Testing stokenet node (node-stokenet:3202)..."
STOKENET_RESPONSE=$(curl -s http://node-stokenet:3202/)
echo "Stokenet response: $STOKENET_RESPONSE"
if echo "$STOKENET_RESPONSE" | grep -q '"specializations":\["RadixStokenet"\]'; then
  echo "✅ Stokenet node has RadixStokenet specialization"
else
  echo "❌ Stokenet node does not have RadixStokenet specialization"
  exit 1
fi

# Test the generic node (port 3203)
echo "Testing generic node (node-generic:3203)..."
GENERIC_RESPONSE=$(curl -s http://node-generic:3203/)
echo "Generic response: $GENERIC_RESPONSE"
if echo "$GENERIC_RESPONSE" | grep -q '"specializations":\[\]'; then
  echo "✅ Generic node has no specializations"
else
  echo "❌ Generic node has specializations when it should not"
  exit 1
fi

echo "All specialization tests passed! ✅"
exit 0 
