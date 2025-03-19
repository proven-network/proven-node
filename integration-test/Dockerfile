# Build .NET applications
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS dotnet-builder
WORKDIR /

RUN git clone https://github.com/radixdlt/babylon-gateway.git
WORKDIR /babylon-gateway
RUN git checkout v1.9.2

WORKDIR /babylon-gateway/apps/DataAggregator
RUN dotnet publish -c Release -o /bin/DataAggregator
WORKDIR /babylon-gateway/apps/DatabaseMigrations
RUN dotnet publish -c Release -o /bin/DatabaseMigrations
WORKDIR /babylon-gateway/apps/GatewayApi
RUN dotnet publish -c Release -o /bin/GatewayApi

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
RUN wget -q https://ftp.postgresql.org/pub/source/v17.2/postgresql-17.2.tar.bz2
RUN tar -xvf postgresql-17.2.tar.bz2
WORKDIR /tmp/postgresql-17.2

# Overwrite root check
RUN sed -i 's/do_check_root = true/do_check_root = false/g' src/backend/main/main.c
RUN sed -i 's/if (geteuid() == 0)/if (false)/g' src/bin/initdb/initdb.c

# Build and install
RUN ./configure
RUN make -j$(nproc)
RUN make install

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
    jq \
    libssl3 \
    netcat-openbsd \
    wget \
    unzip \
    openjdk-17-jre-headless \
    # Debug tools
    procps \
    net-tools \
    iproute2 \
    tcpdump \
    dnsutils \
    iputils-ping \
    lsof \
    strace \
    htop \
    telnet \
    traceroute \
    iftop \
    && rm -rf /var/lib/apt/lists/*

# Install PostgreSQL
RUN sh -c 'echo "deb https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" > /etc/apt/sources.list.d/pgdg.list' \
    && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && apt-get update && apt-get install -y postgresql-15 \
    && rm -rf /var/lib/apt/lists/*

# Install .NET Runtime
RUN curl -sSL https://dot.net/v1/dotnet-install.sh | bash /dev/stdin --runtime aspnetcore --version 8.0.12 --install-dir /usr/share/dotnet
RUN ln -s /usr/share/dotnet/dotnet /usr/bin/dotnet

# Create necessary directories
RUN mkdir -p /tmp/proven/kv/application /tmp/proven/kv/persona /etc/proven

# Download and install Bitcoin Core
WORKDIR /downloads
RUN wget https://bitcoincore.org/bin/bitcoin-core-27.0/bitcoin-27.0-aarch64-linux-gnu.tar.gz \
    && tar -xzf bitcoin-27.0-aarch64-linux-gnu.tar.gz \
    && install -m 0755 -o root -g root -t /usr/local/bin bitcoin-27.0/bin/bitcoind bitcoin-27.0/bin/bitcoin-cli

# Download and install Reth Ethereum client
RUN wget https://github.com/paradigmxyz/reth/releases/download/v1.3.1/reth-v1.3.1-aarch64-unknown-linux-gnu.tar.gz \
    && tar -xzf reth-v1.3.1-aarch64-unknown-linux-gnu.tar.gz \
    && install -m 0755 -o root -g root -t /usr/local/bin reth

# Download and install Lighthouse for Ethereum PoS consensus
RUN wget https://github.com/sigp/lighthouse/releases/download/v7.0.0-beta.3/lighthouse-v7.0.0-beta.3-aarch64-unknown-linux-gnu.tar.gz \
    && tar -xzf lighthouse-v7.0.0-beta.3-aarch64-unknown-linux-gnu.tar.gz \
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

# Copy .NET applications
COPY --from=dotnet-builder /bin/DataAggregator /bin/DataAggregator
COPY --from=dotnet-builder /bin/DatabaseMigrations /bin/DatabaseMigrations
COPY --from=dotnet-builder /bin/GatewayApi /bin/GatewayApi

# Copy postgres binaries
COPY --from=postgres /usr/local/pgsql /usr/local/pgsql

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
