#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Define base directory in /tmp
BASE_DIR="/tmp/nats_cluster_$$"
mkdir -p $BASE_DIR

# Node configurations
declare -a ports=(4222 4223 4224)
declare -a cluster_ports=(6222 6223 6224)
declare -a node_names=("node1" "node2" "node3")

# Kill any existing nats-server processes listening on these ports
echo "Stopping existing nats-server processes..."
for port in "${ports[@]}" "${cluster_ports[@]}"; do
    lsof -ti tcp:${port} | xargs --no-run-if-empty kill -9
done

# Clean up previous run (if any) based on a pattern - be careful with this in production
# find /tmp -maxdepth 1 -name 'nats_cluster_*' -type d -exec rm -rf {} +

echo "Setting up NATS cluster in $BASE_DIR"

# Generate config files and start nodes
for i in "${!ports[@]}"; do
    NODE_NAME=${node_names[$i]}
    PORT=${ports[$i]}
    CLUSTER_PORT=${cluster_ports[$i]}
    DATA_DIR="$BASE_DIR/$NODE_NAME"
    CONFIG_FILE="$BASE_DIR/${NODE_NAME}.conf"

    mkdir -p $DATA_DIR

    # Create node configuration file
    cat > $CONFIG_FILE <<EOF
port: $PORT
server_name: $NODE_NAME
jetstream {
    store_dir: "$DATA_DIR"
}

cluster {
  name: "nats_cluster"
  port: $CLUSTER_PORT
  routes = [
    "nats-route://localhost:6222",
    "nats-route://localhost:6223",
    "nats-route://localhost:6224"
  ]
}
EOF

    echo "Starting $NODE_NAME on port $PORT (cluster port $CLUSTER_PORT)..."
    nats-server -c $CONFIG_FILE &> "$BASE_DIR/${NODE_NAME}.log" &
    echo "  PID: $!, Log: $BASE_DIR/${NODE_NAME}.log"
done

echo "NATS cluster setup complete. Nodes running in the background."
echo "Data directories:"
for i in "${!ports[@]}"; do
    echo "  ${node_names[$i]}: $BASE_DIR/${node_names[$i]}"
done
echo "To connect, use: nats://localhost:4222,nats://localhost:4223,nats://localhost:4224"
echo "To stop the cluster, run: pkill -f nats-server"
