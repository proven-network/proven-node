#!/bin/bash
set -e

# Set up environment variables with defaults
NODE_ID=${NODE_ID:-"node1"}
PORT=${PORT:-3200}

echo "Starting node with ID: $NODE_ID, port: $PORT, specialization: $NODE_SPECIALIZATION"

# Create governance.json file based on NODE_SPECIALIZATION
echo "{" > /tmp/proven/governance.json
echo "  \"version\": \"1.0.0\"," >> /tmp/proven/governance.json
if [ -n "$NODE_SPECIALIZATION" ]; then
  echo "  \"specializations\": [\"$NODE_SPECIALIZATION\"]" >> /tmp/proven/governance.json
else
  echo "  \"specializations\": []" >> /tmp/proven/governance.json
fi
echo "}" >> /tmp/proven/governance.json

# Start a simple HTTP server that responds to health checks
echo "Starting HTTP server on port $PORT"
while true; do
  # Use netcat to create a simple HTTP server that returns a 200 OK for health checks
  { echo -ne "HTTP/1.1 200 OK\r\nContent-Length: 15\r\n\r\n{\"status\":\"UP\"}"; } | nc -l -p $PORT
  echo "Responded to request on port $PORT"
done
