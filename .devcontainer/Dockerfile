FROM mcr.microsoft.com/devcontainers/rust:1-1-bookworm

# Install nightly Rust and set it as default
RUN rustup update && \
    rustup toolchain install nightly && \
    rustup default nightly

# Install build dependencies and debugging tools
RUN apt-get update && apt-get install -y \
    bison \
    build-essential \
    ca-certificates \
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
RUN ./configure && \
    make -j$(nproc) && \
    make install

# Create necessary directories
RUN mkdir -p /tmp/proven/kv/application /tmp/proven/kv/persona /etc/proven

# Download and install Bitcoin Core for ARM64
WORKDIR /downloads
RUN wget https://bitcoincore.org/bin/bitcoin-core-28.1/bitcoin-28.1-aarch64-linux-gnu.tar.gz \
    && tar -xzf bitcoin-28.1-aarch64-linux-gnu.tar.gz \
    && install -m 0755 -o root -g root -t /usr/local/bin bitcoin-28.1/bin/bitcoind bitcoin-28.1/bin/bitcoin-cli

# Download and install x86_64 version as fallback for non-ARM systems
RUN wget https://bitcoincore.org/bin/bitcoin-core-28.1/bitcoin-28.1-x86_64-linux-gnu.tar.gz \
    && tar -xzf bitcoin-28.1-x86_64-linux-gnu.tar.gz \
    && if [ "$(uname -m)" = "x86_64" ]; then \
       install -m 0755 -o root -g root -t /usr/local/bin bitcoin-28.1/bin/bitcoind bitcoin-28.1/bin/bitcoin-cli; \
    fi

# Download and install Reth Ethereum client
RUN wget https://github.com/paradigmxyz/reth/releases/download/v1.3.8/reth-v1.3.8-aarch64-unknown-linux-gnu.tar.gz \
    && tar -xzf reth-v1.3.8-aarch64-unknown-linux-gnu.tar.gz \
    && install -m 0755 -o root -g root -t /usr/local/bin reth

# Download and install Lighthouse for Ethereum PoS consensus
RUN wget https://github.com/sigp/lighthouse/releases/download/v7.0.0-beta.5/lighthouse-v7.0.0-beta.5-aarch64-unknown-linux-gnu.tar.gz \
    && tar -xzf lighthouse-v7.0.0-beta.5-aarch64-unknown-linux-gnu.tar.gz \
    && install -m 0755 -o root -g root -t /usr/local/bin lighthouse

# Download and install NATS server
RUN wget https://github.com/nats-io/nats-server/releases/download/v2.11.0/nats-server-v2.11.0-linux-arm64.tar.gz \
    && tar -xzf nats-server-v2.11.0-linux-arm64.tar.gz \
    && install -m 0755 -o root -g root -t /usr/local/bin nats-server-v2.11.0-linux-arm64/nats-server

# Download and install Babylon Node
RUN wget -q https://github.com/radixdlt/babylon-node/releases/download/v1.3.0.2/babylon-node-rust-arch-linux-aarch64-release-v1.3.0.2.zip \
    && wget -q https://github.com/radixdlt/babylon-node/releases/download/v1.3.0.2/babylon-node-v1.3.0.2.zip \
    && unzip babylon-node-rust-arch-linux-aarch64-release-v1.3.0.2.zip \
    && unzip babylon-node-v1.3.0.2.zip \
    && mkdir -p /bin/babylon-node \
    && mv core-v1.3.0.2 /bin/babylon-node/ \
    && mv libcorerust.so /bin/babylon-node/

# Clone and build .NET applications
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

# Clean up
WORKDIR /
RUN rm -rf /downloads /tmp/postgresql-17.4 /tmp/postgresql-17.4.tar.bz2

# Reset to start directory
WORKDIR /workspaces/proven-node 
