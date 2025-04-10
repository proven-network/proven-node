# ------------------
# DOTNET BUILD STEPS
# ------------------
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

# --------------------
# POSTGRES BUILD STEPS
# --------------------
FROM debian:bookworm-20240904-slim AS postgres-builder

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
RUN ./configure
RUN make -j$(nproc)
RUN make install

# --------------
# GO BUILD STEPS
# --------------
FROM golang:1.23.6-alpine3.21 AS go-builder
WORKDIR /

# Install dependencies
RUN apk add --no-cache bash git make gcc musl-dev linux-headers

# Build gocryptfs
RUN git clone https://github.com/rfjakob/gocryptfs.git
WORKDIR /gocryptfs
RUN git checkout v2.5.1
RUN ./build-without-openssl.bash

# Move gocryptfs to /bin
RUN cp /gocryptfs/gocryptfs /bin/
RUN chown root:root /bin/gocryptfs
RUN chmod 0755      /bin/gocryptfs

# ----------------
# RUST BUILD STEPS
# ----------------
FROM rustlang/rust:nightly-bookworm-slim AS rust-builder
WORKDIR /

# Install base dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    curl \
    g++ \
    git \
    gcc \
    libclang-dev \
    libssl-dev \
    make \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js (for vendored runtime dependencies)
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get update && apt-get install -y \
    nodejs \
    && rm -rf /var/lib/apt/lists/*

# Build proven-enclave
ARG COMMIT
RUN git clone --recurse-submodules https://github.com/proven-network/proven-node.git
WORKDIR /proven-node
RUN git checkout ${COMMIT}
RUN cargo build --release --package proven-enclave

# Move proven-enclave to /bin
RUN cp /proven-node/target/release/proven-enclave /bin/
RUN chown root:root /bin/proven-enclave
RUN chmod 0755      /bin/proven-enclave

# Remove target dir to speed up snapshot
RUN rm -rf /proven-node/target

# --------------
# EXTRA BINARIES
# --------------
FROM alpine:3.21 AS extra-binaries

RUN apk add --no-cache unzip wget

WORKDIR /tmp

# Download
RUN wget -q https://bitcoincore.org/bin/bitcoin-core-28.1/bitcoin-28.1-aarch64-linux-gnu.tar.gz
RUN wget -q https://github.com/paradigmxyz/reth/releases/download/v1.3.8/reth-v1.3.8-aarch64-unknown-linux-gnu.tar.gz
RUN wget -q https://github.com/sigp/lighthouse/releases/download/v7.0.0-beta.5/lighthouse-v7.0.0-beta.5-aarch64-unknown-linux-gnu.tar.gz
RUN wget -q https://github.com/radixdlt/babylon-node/releases/download/v1.3.0.2/babylon-node-rust-arch-linux-aarch64-release-v1.3.0.2.zip
RUN wget -q https://github.com/radixdlt/babylon-node/releases/download/v1.3.0.2/babylon-node-v1.3.0.2.zip
RUN wget -q https://github.com/nats-io/nats-server/releases/download/v2.11.0/nats-server-v2.11.0-linux-arm64.tar.gz
RUN wget -q https://github.com/DNSCrypt/dnscrypt-proxy/releases/download/2.1.7/dnscrypt-proxy-linux_arm64-2.1.7.tar.gz
RUN wget -q https://github.com/librespeed/speedtest-cli/releases/download/v1.0.11/librespeed-cli_1.0.11_linux_arm64.tar.gz

# Unzip
RUN tar -xvzf bitcoin-28.1-aarch64-linux-gnu.tar.gz
RUN tar -xvzf reth-v1.3.8-aarch64-unknown-linux-gnu.tar.gz
RUN tar -xvzf lighthouse-v7.0.0-beta.5-aarch64-unknown-linux-gnu.tar.gz
RUN unzip babylon-node-rust-arch-linux-aarch64-release-v1.3.0.2.zip
RUN unzip babylon-node-v1.3.0.2.zip
RUN tar -xvzf nats-server-v2.11.0-linux-arm64.tar.gz
RUN tar -xvzf dnscrypt-proxy-linux_arm64-2.1.7.tar.gz
RUN tar -xvzf librespeed-cli_1.0.11_linux_arm64.tar.gz

# Move to /bin
RUN mv bitcoin-28.1/bin/bitcoind /bin/
RUN mv bitcoin-28.1/bin/bitcoin-cli /bin/
RUN mv reth /bin/
RUN mv lighthouse /bin/
RUN mkdir -p /bin/babylon-node
RUN mv core-v1.3.0.2 /bin/babylon-node
RUN mv libcorerust.so /bin/babylon-node
RUN mv nats-server-v2.11.0-linux-arm64/nats-server /bin/
RUN mv linux-arm64/dnscrypt-proxy /bin/
RUN mv librespeed-cli /bin/

# -----------
# FINAL IMAGE
# -----------
FROM debian:bookworm-20240904-slim

RUN apt-get update && apt-get install -y \
    curl \
    fuse \
    iproute2 \
    openjdk-17-jre-headless \
    net-tools \
    nfs-common \
    nfs-kernel-server

# Remove non-deterministic items (copy in from known image at the end)
RUN rm -f /etc/ssl/certs/java/cacerts /var/lib/dbus/machine-id

# Remove apt-get side effects
RUN rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/* /var/log/apt/* /var/log/dpkg.log /var/cache/ldconfig/aux-cache /var/log/alternatives.log /var/log/apt/history.log /var/log/apt/term.log /var/log/dpkg.log

RUN curl -sSL https://dot.net/v1/dotnet-install.sh | bash /dev/stdin --runtime aspnetcore --version 8.0.12 --install-dir /usr/share/dotnet
RUN ln -s /usr/share/dotnet/dotnet /usr/bin/dotnet

COPY --from=dotnet-builder /bin/DataAggregator /bin/DataAggregator
COPY --from=dotnet-builder /bin/DatabaseMigrations /bin/DatabaseMigrations
COPY --from=dotnet-builder /bin/GatewayApi /bin/GatewayApi
COPY --from=postgres-builder /usr/local/pgsql /usr/local/pgsql
COPY --from=go-builder /bin/gocryptfs /bin/
COPY --from=rust-builder /bin/proven-enclave /bin/
COPY --from=extra-binaries /bin/bitcoind /bin/
COPY --from=extra-binaries /bin/bitcoin-cli /bin/
COPY --from=extra-binaries /bin/reth /bin/
COPY --from=extra-binaries /bin/lighthouse /bin/
COPY --from=extra-binaries /bin/babylon-node /bin/babylon-node
COPY --from=extra-binaries /bin/nats-server /bin/
COPY --from=extra-binaries /bin/dnscrypt-proxy /bin/
COPY --from=extra-binaries /bin/librespeed-cli /bin/
COPY --from=openjdk:17-jdk-slim /usr/local/openjdk-17/lib/security/cacerts /etc/ssl/certs/java/cacerts

# Run with increased file descriptor limit
CMD ulimit -n 524288 && proven-enclave
