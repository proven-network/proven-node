#!/bin/bash

# Script to run 3 instances of proven-local with network.json configuration
# Each instance corresponds to one of the nodes defined in network.json

set -e

# Colors for output
RED=$'\033[0;31m'
GREEN=$'\033[0;32m'
BLUE=$'\033[0;34m'
YELLOW=$'\033[1;33m'
NC=$'\033[0m' # No Color

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NETWORK_CONFIG="$SCRIPT_DIR/network.json"

# Check if network.json exists
if [[ ! -f "$NETWORK_CONFIG" ]]; then
    echo -e "${RED}Error: network.json not found at $NETWORK_CONFIG${NC}"
    exit 1
fi

# Matches public keys in network.json
NODE1_KEY="5625b9a5552793f321c6b1178fd20be8f8ab83fbab91a729fb9d5d222fc830c2"
NODE2_KEY="76709830bdd5eee1601fc8dee71713a9dde29c9895d15b3208bd26ce0c9e266d"
NODE3_KEY="9d3585d534880f32264b35c087cbc84d774b53fcbebc9649fec7bdf2db04ffe4"

# Ports for each instance (based on network.json origins)
NODE1_PORT=3201
NODE2_PORT=3202
NODE3_PORT=3203

# NATS ports (each instance needs unique NATS ports)
NODE1_NATS_CLIENT_PORT=4222
NODE1_NATS_CLUSTER_PORT=6222
NODE1_NATS_HTTP_PORT=8222

NODE2_NATS_CLIENT_PORT=4223
NODE2_NATS_CLUSTER_PORT=6223
NODE2_NATS_HTTP_PORT=8223

NODE3_NATS_CLIENT_PORT=4224
NODE3_NATS_CLUSTER_PORT=6224
NODE3_NATS_HTTP_PORT=8224

NODE1_BITCOIN_MAINNET_PROXY_PORT=11000
NODE2_BITCOIN_MAINNET_PROXY_PORT=11001
NODE3_BITCOIN_MAINNET_PROXY_PORT=11002

NODE1_BITCOIN_TESTNET_PROXY_PORT=11003
NODE2_BITCOIN_TESTNET_PROXY_PORT=11004
NODE3_BITCOIN_TESTNET_PROXY_PORT=11005

# Store directories for each instance
NODE1_STORE_BASE="/tmp/proven/node1"
NODE2_STORE_BASE="/tmp/proven/node2"
NODE3_STORE_BASE="/tmp/proven/node3"

# Function to run a single instance
run_instance() {
    local instance_name="$1"
    local node_key="$2"
    local port="$3"
    local nats_client_port="$4"
    local nats_cluster_port="$5"
    local nats_http_port="$6"
    local bitcoin_mainnet_proxy_port="$7"
    local bitcoin_testnet_proxy_port="$8"
    local store_base="$9"

    echo "Starting $instance_name..."
    
    cd "$SCRIPT_DIR"
    
    cargo run -- \
        --network-config-path "$NETWORK_CONFIG" \
        --node-key "$node_key" \
        --port "$port" \
        --nats-client-port "$nats_client_port" \
        --nats-cluster-port "$nats_cluster_port" \
        --nats-http-port "$nats_http_port" \
        --nats-store-dir "$store_base/nats" \
        --bitcoin-mainnet-proxy-port "$bitcoin_mainnet_proxy_port" \
        --bitcoin-testnet-proxy-port "$bitcoin_testnet_proxy_port" \
        2>&1 | sed "s/^/[$instance_name] /"
}

# Function to cleanup background processes
cleanup() {
    echo -e "\n${YELLOW}Shutting down all instances...${NC}"
    jobs -p | xargs -r kill
    wait
    echo -e "${GREEN}All instances stopped.${NC}"
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM EXIT

echo -e "${GREEN}Starting 3 instances of proven-local with network configuration from $NETWORK_CONFIG${NC}"
echo -e "${BLUE}Instance URLs will be:${NC}"
echo -e "  ${BLUE}Node 1 (bulbasaur): http://localhost:$NODE1_PORT${NC}"
echo -e "  ${BLUE}Node 2 (charmander): http://localhost:$NODE2_PORT${NC}"
echo -e "  ${BLUE}Node 3 (squirtle): http://localhost:$NODE3_PORT${NC}"
echo ""

# Start all instances in background
run_instance "bulbasaur" "$NODE1_KEY" "$NODE1_PORT" "$NODE1_NATS_CLIENT_PORT" "$NODE1_NATS_CLUSTER_PORT" "$NODE1_NATS_HTTP_PORT" "$NODE1_BITCOIN_MAINNET_PROXY_PORT" "$NODE1_BITCOIN_TESTNET_PROXY_PORT" "$NODE1_STORE_BASE" &
run_instance "charmander" "$NODE2_KEY" "$NODE2_PORT" "$NODE2_NATS_CLIENT_PORT" "$NODE2_NATS_CLUSTER_PORT" "$NODE2_NATS_HTTP_PORT" "$NODE2_BITCOIN_MAINNET_PROXY_PORT" "$NODE2_BITCOIN_TESTNET_PROXY_PORT" "$NODE2_STORE_BASE" &
run_instance "squirtle" "$NODE3_KEY" "$NODE3_PORT" "$NODE3_NATS_CLIENT_PORT" "$NODE3_NATS_CLUSTER_PORT" "$NODE3_NATS_HTTP_PORT" "$NODE3_BITCOIN_MAINNET_PROXY_PORT" "$NODE3_BITCOIN_TESTNET_PROXY_PORT" "$NODE3_STORE_BASE" &

echo -e "${GREEN}All instances started! Press Ctrl+C to stop all instances.${NC}"

# Wait for all background jobs
wait 
