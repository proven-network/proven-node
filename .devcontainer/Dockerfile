FROM mcr.microsoft.com/devcontainers/rust:1-1-bookworm

# Install nightly Rust and set it as default
RUN rustup update && \
    rustup toolchain install nightly && \
    rustup default nightly && \
    rustup component add rustfmt --toolchain nightly && \
    rustup component add clippy --toolchain nightly

# Install build dependencies and debugging tools
RUN apt-get update && apt-get install -y \
    bison \
    build-essential \
    ca-certificates \
    cmake \
    curl \
    dnsutils \
    flex \
    gcc \
    gnupg \
    htop \
    iftop \
    iproute2 \
    iptables \
    iputils-ping \
    jq \
    libicu-dev \
    libreadline6-dev \
    libssl-dev \
    lsof \
    net-tools \
    netcat-openbsd \
    openjdk-17-jre-headless \
    perl \
    pkg-config \
    procps \
    strace \
    tcpdump \
    telnet \
    traceroute \
    unzip \
    wget \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Install .NET SDK
RUN curl -sSL https://dot.net/v1/dotnet-install.sh | bash /dev/stdin --channel 8.0 --install-dir /usr/share/dotnet
RUN ln -s /usr/share/dotnet/dotnet /usr/bin/dotnet

# Build custom postgres that can handle root user
WORKDIR /tmp
RUN wget -q https://ftp.postgresql.org/pub/source/v17.4/postgresql-17.4.tar.bz2 && \
    tar -xvf postgresql-17.4.tar.bz2
WORKDIR /tmp/postgresql-17.4

# Overwrite root check
RUN sed -i 's/do_check_root = true/do_check_root = false/g' src/backend/main/main.c && \
    sed -i 's/if (geteuid() == 0)/if (false)/g' src/bin/initdb/initdb.c

# Build and install postgres
RUN mkdir -p /apps/postgres/v17.4 && \
    ./configure --prefix=/apps/postgres/v17.4 && \
    make -j$(nproc) && \
    make install

# Create necessary directories
RUN mkdir -p /tmp/proven/kv/application /tmp/proven/kv/persona /etc/proven

# Create base apps directory
RUN mkdir -p /apps

# Download and install Bitcoin Core for ARM64
WORKDIR /downloads
RUN mkdir -p /apps/bitcoin/v28.1 && \
    wget https://bitcoincore.org/bin/bitcoin-core-28.1/bitcoin-28.1-aarch64-linux-gnu.tar.gz \
    && tar -xzf bitcoin-28.1-aarch64-linux-gnu.tar.gz \
    && install -m 0755 -o root -g root -t /apps/bitcoin/v28.1 bitcoin-28.1/bin/bitcoind bitcoin-28.1/bin/bitcoin-cli

# Download and install x86_64 version as fallback for non-ARM systems
RUN wget https://bitcoincore.org/bin/bitcoin-core-28.1/bitcoin-28.1-x86_64-linux-gnu.tar.gz \
    && tar -xzf bitcoin-28.1-x86_64-linux-gnu.tar.gz \
    && if [ "$(uname -m)" = "x86_64" ]; then \
       install -m 0755 -o root -g root -t /apps/bitcoin/v28.1 bitcoin-28.1/bin/bitcoind bitcoin-28.1/bin/bitcoin-cli; \
    fi

# Download and install Reth Ethereum client
RUN mkdir -p /apps/ethereum-reth/v1.3.12 && \
    wget https://github.com/paradigmxyz/reth/releases/download/v1.3.12/reth-v1.3.12-aarch64-unknown-linux-gnu.tar.gz \
    && tar -xzf reth-v1.3.12-aarch64-unknown-linux-gnu.tar.gz \
    && install -m 0755 -o root -g root -t /apps/ethereum-reth/v1.3.12 reth

# Download and install Lighthouse for Ethereum PoS consensus
RUN mkdir -p /apps/ethereum-lighthouse/v7.0.0 && \
    wget https://github.com/sigp/lighthouse/releases/download/v7.0.0/lighthouse-v7.0.0-aarch64-unknown-linux-gnu.tar.gz \
    && tar -xzf lighthouse-v7.0.0-aarch64-unknown-linux-gnu.tar.gz \
    && install -m 0755 -o root -g root -t /apps/ethereum-lighthouse/v7.0.0 lighthouse

# Download and install NATS server
RUN mkdir -p /apps/nats/v2.11.3 && \
    wget https://github.com/nats-io/nats-server/releases/download/v2.11.3/nats-server-v2.11.3-linux-arm64.tar.gz \
    && tar -xzf nats-server-v2.11.3-linux-arm64.tar.gz \
    && install -m 0755 -o root -g root -t /apps/nats/v2.11.3 nats-server-v2.11.3-linux-arm64/nats-server

# Download and install Radix Node
RUN mkdir -p /apps/radix-node/v1.3.0.2 && \
    wget -q https://github.com/radixdlt/babylon-node/releases/download/v1.3.0.2/babylon-node-rust-arch-linux-aarch64-release-v1.3.0.2.zip \
    && wget -q https://github.com/radixdlt/babylon-node/releases/download/v1.3.0.2/babylon-node-v1.3.0.2.zip \
    && unzip babylon-node-rust-arch-linux-aarch64-release-v1.3.0.2.zip \
    && unzip babylon-node-v1.3.0.2.zip \
    && mv core-v1.3.0.2 /apps/radix-node/v1.3.0.2/ \
    && mv libcorerust.so /apps/radix-node/v1.3.0.2/

# Clone and build .NET applications
WORKDIR /
RUN git clone https://github.com/radixdlt/babylon-gateway.git
WORKDIR /babylon-gateway
RUN git checkout v1.9.2

# Build and install Gateway applications
RUN mkdir -p /apps/radix-gateway/v1.9.2 && \
    cd /babylon-gateway/apps/DataAggregator && \
    dotnet publish -c Release -o /apps/radix-gateway/v1.9.2/DataAggregator && \
    cd /babylon-gateway/apps/DatabaseMigrations && \
    dotnet publish -c Release -o /apps/radix-gateway/v1.9.2/DatabaseMigrations && \
    cd /babylon-gateway/apps/GatewayApi && \
    dotnet publish -c Release -o /apps/radix-gateway/v1.9.2/GatewayApi

# Clean up
WORKDIR /
RUN rm -rf /downloads /tmp/postgresql-17.4 /tmp/postgresql-17.4.tar.bz2

# Reset to start directory
WORKDIR /workspaces/proven-node 
