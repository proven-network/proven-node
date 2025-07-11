#!/bin/bash
set -e

# Set file descriptor limit
ulimit -n 524288

# Bring up loopback interface (disabled by default in enclave)
echo "Bringing up loopback interface..."
ip link set lo up

# Start vsock proxy in background
echo "Starting vsock proxy..."
/apps/proven/proxy/vsock-proxy-enclave-frameless \
    --vsock-port 5000 \
    --tun-addr 10.0.1.1 \
    --tun-mask 24 &
PROXY_PID=$!

# Wait for TUN interface to come up
echo "Waiting for TUN interface..."
for i in {1..30}; do
    if ip link show tun0 &>/dev/null; then
        echo "TUN interface is up"
        break
    fi
    sleep 1
done

# Setup default gateway and network routes
echo "Setting up network routes..."
# Add default route via the host IP
ip route add default via 10.0.1.1 dev tun0 2>/dev/null || true
# Add the network route for the CIDR
ip route add 10.0.1.0/24 dev tun0 2>/dev/null || true

# Function to cleanup on exit
cleanup() {
    echo "Shutting down..."
    kill $PROXY_PID 2>/dev/null || true
    exit
}

# Set trap for cleanup
trap cleanup EXIT INT TERM

# Start proven-enclave
echo "Starting proven-enclave..."
exec /apps/proven/enclave/proven-enclave "$@"
