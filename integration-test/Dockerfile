# Build .NET applications
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS dotnet-builder
WORKDIR /

RUN git clone https://github.com/radixdlt/babylon-gateway.git
WORKDIR /babylon-gateway
RUN git checkout v1.9.2

# Build Gateway applications
RUN mkdir -p /apps/radix-gateway/v1.9.2 && \
    cd /babylon-gateway/apps/DataAggregator && \
    dotnet publish -c Release -o /apps/radix-gateway/v1.9.2/DataAggregator && \
    cd /babylon-gateway/apps/DatabaseMigrations && \
    dotnet publish -c Release -o /apps/radix-gateway/v1.9.2/DatabaseMigrations && \
    cd /babylon-gateway/apps/GatewayApi && \
    dotnet publish -c Release -o /apps/radix-gateway/v1.9.2/GatewayApi

# Build postgres which can handle root user
FROM debian:bookworm-20240904-slim AS postgres

# Install dependencies to build from source
RUN apt-get update && apt-get install -y \
    bison \
    build-essential \
    flex \
    gcc \
    libicu-dev \
    libreadline6-dev \
    perl \
    pkg-config \
    wget \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Download source code
WORKDIR /tmp
RUN wget -q https://ftp.postgresql.org/pub/source/v17.4/postgresql-17.4.tar.bz2
RUN tar -xvf postgresql-17.4.tar.bz2
WORKDIR /tmp/postgresql-17.4

# Overwrite root check
RUN sed -i 's/do_check_root = true/do_check_root = false/g' src/backend/main/main.c
RUN sed -i 's/if (geteuid() == 0)/if (false)/g' src/bin/initdb/initdb.c

# Build and install
RUN mkdir -p /apps/postgres/v17.4 && \
    ./configure --prefix=/apps/postgres/v17.4 && \
    make -j$(nproc) && \
    make install

# Build proven
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

# Start new stage for runtime - switch to plain Debian
FROM debian:bookworm-slim

# Install runtime dependencies and debugging tools
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    iptables \
    jq \
    libssl3 \
    netcat-openbsd \
    openjdk-17-jre-headless \
    unzip \
    wget \
    # Debug tools
    dnsutils \
    iftop \
    iproute2 \
    htop \
    iputils-ping \
    lsof \
    net-tools \
    procps \
    strace \
    tcpdump \
    telnet \
    traceroute \
    && rm -rf /var/lib/apt/lists/*

# Install .NET Runtime
RUN curl -sSL https://dot.net/v1/dotnet-install.sh | bash /dev/stdin --runtime aspnetcore --version 8.0.12 --install-dir /usr/share/dotnet
RUN ln -s /usr/share/dotnet/dotnet /usr/bin/dotnet

# Create necessary directories
RUN mkdir -p /tmp/proven/kv/application /tmp/proven/kv/persona /etc/proven /apps

# Download and install Bitcoin Core
WORKDIR /downloads
RUN mkdir -p /apps/bitcoin/v28.1 && \
    wget https://bitcoincore.org/bin/bitcoin-core-28.1/bitcoin-28.1-aarch64-linux-gnu.tar.gz \
    && tar -xzf bitcoin-28.1-aarch64-linux-gnu.tar.gz \
    && install -m 0755 -o root -g root -t /apps/bitcoin/v28.1 bitcoin-28.1/bin/bitcoind bitcoin-28.1/bin/bitcoin-cli

# Download and install Reth Ethereum client
RUN mkdir -p /apps/ethereum-reth/v1.3.8 && \
    wget https://github.com/paradigmxyz/reth/releases/download/v1.3.8/reth-v1.3.8-aarch64-unknown-linux-gnu.tar.gz \
    && tar -xzf reth-v1.3.8-aarch64-unknown-linux-gnu.tar.gz \
    && install -m 0755 -o root -g root -t /apps/ethereum-reth/v1.3.8 reth

# Download and install Lighthouse for Ethereum PoS consensus
RUN mkdir -p /apps/ethereum-lighthouse/v7.0.0-beta.5 && \
    wget https://github.com/sigp/lighthouse/releases/download/v7.0.0-beta.5/lighthouse-v7.0.0-beta.5-aarch64-unknown-linux-gnu.tar.gz \
    && tar -xzf lighthouse-v7.0.0-beta.5-aarch64-unknown-linux-gnu.tar.gz \
    && install -m 0755 -o root -g root -t /apps/ethereum-lighthouse/v7.0.0-beta.5 lighthouse

# Download and install NATS server
RUN mkdir -p /apps/nats/v2.11.0 && \
    wget https://github.com/nats-io/nats-server/releases/download/v2.11.0/nats-server-v2.11.0-linux-arm64.tar.gz \
    && tar -xzf nats-server-v2.11.0-linux-arm64.tar.gz \
    && install -m 0755 -o root -g root -t /apps/nats/v2.11.0 nats-server-v2.11.0-linux-arm64/nats-server

# Download and install Babylon Node
RUN mkdir -p /apps/radix-node/v1.3.0.2 && \
    wget -q https://github.com/radixdlt/babylon-node/releases/download/v1.3.0.2/babylon-node-rust-arch-linux-aarch64-release-v1.3.0.2.zip \
    && wget -q https://github.com/radixdlt/babylon-node/releases/download/v1.3.0.2/babylon-node-v1.3.0.2.zip \
    && unzip babylon-node-rust-arch-linux-aarch64-release-v1.3.0.2.zip \
    && unzip babylon-node-v1.3.0.2.zip \
    && mv core-v1.3.0.2 /apps/radix-node/v1.3.0.2/ \
    && mv libcorerust.so /apps/radix-node/v1.3.0.2/

# Copy .NET applications from builder
COPY --from=dotnet-builder /apps/radix-gateway/v1.9.2 /apps/radix-gateway/v1.9.2

# Copy postgres binaries
COPY --from=postgres /apps/postgres/v17.4 /apps/postgres/v17.4

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
