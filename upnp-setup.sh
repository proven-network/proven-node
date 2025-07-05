#!/bin/bash
# Script to set up UPnP port forwarding

# Check for upnpc installation
if ! command -v upnpc &> /dev/null; then
  echo "Installing miniupnpc package..."
  if command -v apt-get &> /dev/null; then
    sudo apt-get update -q && sudo apt-get install -y -q miniupnpc
  elif command -v brew &> /dev/null; then
    brew install miniupnpc
  else
    echo "Unable to install miniupnpc automatically. Please install it manually."
    exit 1
  fi
fi

# Get the local IP address in a cross-platform way
if [[ "$(uname)" == "Darwin" ]]; then
  # macOS way to get primary IP
  LOCAL_IP=$(ifconfig | grep "inet " | grep -v 127.0.0.1 | head -n 1 | awk '{print $2}')
else
  # Linux way
  LOCAL_IP=$(hostname -I | awk '{print $1}')
fi
echo "Local IP: $LOCAL_IP"

# Set up port forwarding
upnpc -a "$LOCAL_IP" 30001 30001 TCP "Proven Node - Radix"
# Reth ports
upnpc -a "$LOCAL_IP" 30304 30304 TCP "Proven Node - Reth P2P"
upnpc -a "$LOCAL_IP" 30304 30304 UDP "Proven Node - Reth P2P UDP"
# Lighthouse ports
upnpc -a "$LOCAL_IP" 9000 9000 TCP "Proven Node - Lighthouse libp2p"
upnpc -a "$LOCAL_IP" 9000 9000 UDP "Proven Node - Lighthouse discovery"
upnpc -a "$LOCAL_IP" 9001 9001 UDP "Proven Node - Lighthouse QUIC"

echo "UPnP port forwarding setup complete"
