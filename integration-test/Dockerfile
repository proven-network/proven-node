# Start with a clean Debian base
FROM debian:bookworm-slim

# Install necessary dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    wget \
    gnupg \
    lsb-release \
    unzip \
    libpq5 \
    libevent-* \
    libgomp1 \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Install PostgreSQL
RUN sh -c 'echo "deb https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list' && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    apt-get update && \
    apt-get install -y postgresql-15 postgresql-client-15 && \
    rm -rf /var/lib/apt/lists/*

# Install .NET Runtime directly from Microsoft's script
RUN apt-get update && \
    apt-get install -y curl && \
    curl -sSL https://dot.net/v1/dotnet-install.sh | bash -s -- --channel 8.0 && \
    ln -s /root/.dotnet/dotnet /usr/local/bin/dotnet && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create directories
RUN mkdir -p /tmp/proven/kv/application /tmp/proven/kv/personal /tmp/proven/kv/nft \
    /tmp/proven/sql/application /tmp/proven/sql/personal /tmp/proven/sql/nft \
    /tmp/proven/sessions \
    /usr/local/bin/ethereum \
    /usr/local/bin/radix

# Download Bitcoin Core binaries
WORKDIR /downloads
RUN wget https://bitcoincore.org/bin/bitcoin-core-27.0/bitcoin-27.0-aarch64-linux-gnu.tar.gz && \
    tar -xzf bitcoin-27.0-aarch64-linux-gnu.tar.gz && \
    cp bitcoin-27.0/bin/bitcoind /usr/local/bin/ && \
    cp bitcoin-27.0/bin/bitcoin-cli /usr/local/bin/ && \
    rm -rf bitcoin-27.0 bitcoin-27.0-aarch64-linux-gnu.tar.gz

# Download Go-Ethereum binary
RUN wget https://gethstore.blob.core.windows.net/builds/geth-linux-arm64-1.15.5-4263936a.tar.gz && \
    tar -xzf geth-linux-arm64-1.15.5-4263936a.tar.gz && \
    mkdir -p /usr/local/bin/ethereum && \
    cp geth-linux-arm64-1.15.5-4263936a/geth /usr/local/bin/ethereum/ && \
    rm -rf geth-linux-arm64-1.15.5-4263936a geth-linux-arm64-1.15.5-4263936a.tar.gz

# Download Lighthouse binary
RUN wget https://github.com/sigp/lighthouse/releases/download/v5.1.0/lighthouse-v5.1.0-aarch64-unknown-linux-gnu.tar.gz && \
    tar -xzf lighthouse-v5.1.0-aarch64-unknown-linux-gnu.tar.gz && \
    mv lighthouse /usr/local/bin/ethereum/ && \
    rm -rf lighthouse-v5.1.0-aarch64-unknown-linux-gnu.tar.gz

# Download NATS Server binary
RUN wget https://github.com/nats-io/nats-server/releases/download/v2.10.11/nats-server-v2.10.11-linux-arm64.zip && \
    unzip nats-server-v2.10.11-linux-arm64.zip && \
    cp nats-server-v2.10.11-linux-arm64/nats-server /usr/local/bin/ && \
    rm -rf nats-server-v2.10.11-linux-arm64 nats-server-v2.10.11-linux-arm64.zip

# Create a dummy implementation for testing purposes
RUN mkdir -p /usr/local/bin/proven-local && \
    echo '#!/bin/bash' > /usr/local/bin/proven-local/entrypoint.sh && \
    echo 'echo "{\"version\": \"1.0.0\", \"specializations\": $([ -n \"$NODE_SPECIALIZATION\" ] && echo "[\\"$NODE_SPECIALIZATION\\"]" || echo "[]")}" > /tmp/proven/governance.json' >> /usr/local/bin/proven-local/entrypoint.sh && \
    echo 'while true; do sleep 10; echo "Node is running..."; done' >> /usr/local/bin/proven-local/entrypoint.sh && \
    chmod +x /usr/local/bin/proven-local/entrypoint.sh

# Add entrypoint script to handle specializations
COPY integration-test/entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh

# Make sure /etc/hosts has proven.local
RUN echo "127.0.0.1 localhost proven.local" > /tmp/hosts && \
    cat /etc/hosts >> /tmp/hosts && \
    # We can't modify /etc/hosts directly as it's read-only, so we'll use the entrypoint script to handle this
    cp /tmp/hosts /tmp/proven/hosts

# Set PATH
ENV PATH="/usr/local/bin:/usr/local/bin/ethereum:/usr/local/bin/radix:${PATH}" \
    PROVEN_HOST="proven.local"

# Expose port for API
EXPOSE 3201 3202 3203

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"] 
