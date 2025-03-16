FROM rustlang/rust:nightly-bookworm-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    libssl-dev \
    nodejs \
    npm \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Set up the working directory
WORKDIR /app

# Copy the entire source code first
COPY . .

# Build with cargo cache mounts
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release --bin proven-local && \
    mkdir -p /tmp/build && \
    cp -r /app/target/release/proven-local /tmp/build/

# Start new stage for runtime
FROM rust:1.85-slim-bookworm

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    jq \
    netcat-openbsd \
    wget \
    unzip \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Install PostgreSQL
RUN sh -c 'echo "deb https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" > /etc/apt/sources.list.d/pgdg.list' \
    && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && apt-get update && apt-get install -y postgresql-15 \
    && rm -rf /var/lib/apt/lists/*

# Install .NET Runtime
RUN apt-get update && \
    apt-get install -y curl && \
    curl -sSL https://dot.net/v1/dotnet-install.sh | bash -s -- --channel 8.0 && \
    ln -s /root/.dotnet/dotnet /usr/local/bin/dotnet && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create necessary directories
RUN mkdir -p /tmp/proven/kv/application /tmp/proven/kv/persona /etc/proven

# Download and install Bitcoin Core
WORKDIR /downloads
RUN wget https://bitcoincore.org/bin/bitcoin-core-27.0/bitcoin-27.0-aarch64-linux-gnu.tar.gz \
    && tar -xzf bitcoin-27.0-aarch64-linux-gnu.tar.gz \
    && install -m 0755 -o root -g root -t /usr/local/bin bitcoin-27.0/bin/bitcoind bitcoin-27.0/bin/bitcoin-cli

# Download and install Go-Ethereum
RUN wget https://gethstore.blob.core.windows.net/builds/geth-linux-arm64-1.15.4-8ccca244.tar.gz \
    && tar -xzf geth-linux-arm64-1.15.4-8ccca244.tar.gz \
    && install -m 0755 -o root -g root -t /usr/local/bin geth-linux-arm64-1.15.4-8ccca244/geth

# Download and install Lighthouse for Ethereum PoS consensus
RUN wget https://github.com/sigp/lighthouse/releases/download/v4.5.0/lighthouse-v4.5.0-aarch64-unknown-linux-gnu.tar.gz \
    && tar -xzf lighthouse-v4.5.0-aarch64-unknown-linux-gnu.tar.gz \
    && install -m 0755 -o root -g root -t /usr/local/bin lighthouse

# Download and install NATS server
RUN wget https://github.com/nats-io/nats-server/releases/download/v2.10.4/nats-server-v2.10.4-linux-arm64.tar.gz \
    && tar -xzf nats-server-v2.10.4-linux-arm64.tar.gz \
    && install -m 0755 -o root -g root -t /usr/local/bin nats-server-v2.10.4-linux-arm64/nats-server

# Download and install Babylon Node
RUN wget -q https://github.com/radixdlt/babylon-node/releases/download/v1.3.0.2/babylon-node-rust-arch-linux-aarch64-release-v1.3.0.2.zip \
    && wget -q https://github.com/radixdlt/babylon-node/releases/download/v1.3.0.2/babylon-node-v1.3.0.2.zip \
    && unzip babylon-node-rust-arch-linux-aarch64-release-v1.3.0.2.zip \
    && unzip babylon-node-v1.3.0.2.zip \
    && mkdir -p /bin/babylon-node \
    && mv core-v1.3.0.2 /bin/babylon-node/ \
    && mv libcorerust.so /bin/babylon-node/ \
    && rm -f *.zip

# Copy the built binary from the builder stage
COPY --from=builder /tmp/build/proven-local /usr/local/bin/

# Copy entrypoint script
COPY integration-test/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Copy configuration files
COPY integration-test/topology.json /etc/proven/topology.json

# Expose port for API
EXPOSE 3201 3202 3203

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"] 
