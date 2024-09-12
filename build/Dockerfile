# --------------
# GO BUILD STEPS
# --------------
FROM golang:alpine3.20 as go-builder
WORKDIR /

# Install dependencies
RUN apk add --no-cache bash git

# Build gocryptfs
RUN git clone https://github.com/rfjakob/gocryptfs.git
WORKDIR /gocryptfs
RUN ./build-without-openssl.bash

# Move gocryptfs to /bin
RUN cp /gocryptfs/gocryptfs /bin/
RUN chown root:root /bin/gocryptfs
RUN chmod 0755      /bin/gocryptfs

# ----------------
# RUST BUILD STEPS
# ----------------
FROM messense/rust-musl-cross:${RUSTARCH}-musl as rust-builder
WORKDIR /

# Set Rust target
ARG RUSTTARGET
RUN rustup target add ${RUSTTARGET}

# Build proven-enclave
WORKDIR /
ARG COMMIT
RUN git clone git@github.com:proven-network/proven-node.git
WORKDIR /proven-node
RUN git checkout ${COMMIT}
RUN cargo build --release --target ${RUSTTARGET} --package proven-enclave

# Move proven-enclave to /bin
RUN cp /proven-node/target/${RUSTTARGET}/release/proven-enclave /bin/
RUN chown root:root /bin/proven-enclave
RUN chmod 0755      /bin/proven-enclave

# --------------
# EXTRA BINARIES
# --------------
FROM alpine:3.20 as extra-binaries

RUN apk add --no-cache unzip wget

WORKDIR /tmp

# Download
RUN wget https://github.com/radixdlt/babylon-node/releases/download/v1.2.2/babylon-node-rust-arch-linux-aarch64-release-v1.2.2.zip
RUN wget https://github.com/radixdlt/babylon-node/releases/download/v1.2.3/babylon-node-v1.2.3.zip
RUN wget https://github.com/nats-io/nats-server/releases/download/v2.10.20/nats-server-v2.10.20-linux-arm64.zip
RUN wget https://github.com/DNSCrypt/dnscrypt-proxy/releases/download/2.1.5/dnscrypt-proxy-linux_arm64-2.1.5.tar.gz

# Unzip
RUN unzip babylon-node-rust-arch-linux-aarch64-release-v1.2.2.zip
RUN unzip babylon-node-v1.2.3.zip
RUN unzip nats-server-v2.10.20-linux-arm64.zip
RUN tar -xvzf dnscrypt-proxy-linux_arm64-2.1.5.tar.gz

# Move to /bin
RUN mkdir -p /bin/babylon-node
RUN mv core-v1.2.3 /bin/babylon-node
RUN mv libcorerust.so /bin/babylon-node
RUN chown -R root:root /bin/babylon-node
RUN mv nats-server-v2.10.20-linux-arm64/nats-server /bin/
RUN chown root:root /bin/nats-server
RUN mv linux-arm64/dnscrypt-proxy /bin/
RUN chown root:root /bin/dnscrypt-proxy

# -----------
# FINAL IMAGE
# -----------
FROM debian:bookworm-20240904-slim

RUN apt-get update && apt-get install -y \
    fuse \
    iproute2 \
    openjdk-17-jre-headless \
    net-tools \
    nfs-common \
    && rm -rf /var/lib/apt/lists/*

COPY --from=go-builder /bin/gocryptfs /bin/
COPY --from=rust-builder /bin/proven-enclave /bin/
COPY --from=extra-binaries /bin/babylon-node /bin/babylon-node
COPY --from=extra-binaries /bin/nats-server /bin/
COPY --from=extra-binaries /bin/dnscrypt-proxy /bin/

CMD proven-enclave
