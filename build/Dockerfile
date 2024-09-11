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

# ------------
# BABYLON NODE
# ------------
FROM alpine:3.20 as babylon-node

RUN apk add --no-cache unzip wget

WORKDIR /babylon-node

# Download babylon-node
RUN wget https://github.com/radixdlt/babylon-node/releases/download/v1.2.2/babylon-node-rust-arch-linux-aarch64-release-v1.2.2.zip
RUN wget https://github.com/radixdlt/babylon-node/releases/download/v1.2.3/babylon-node-v1.2.3.zip

# Unzip babylon-node
RUN unzip babylon-node-rust-arch-linux-aarch64-release-v1.2.2.zip
RUN unzip babylon-node-v1.2.3.zip

# Move babylon-node to /bin
RUN mkdir -p /bin/babylon-node
RUN mv core-v1.2.3 /bin/babylon-node
RUN mv libcorerust.so /bin/babylon-node
RUN chown -R root:root /bin/babylon-node

# -----------
# FINAL IMAGE
# -----------
FROM nats:alpine3.20

RUN apk add --no-cache dnscrypt-proxy iproute2 iproute2-tc openjdk17 net-tools nfs-utils

COPY --from=go-builder /bin/gocryptfs /bin/
# COPY --from=rust-builder /bin/proven-enclave /bin/
COPY --from=babylon-node /bin/babylon-node /bin/babylon-node

RUN ls -lah /bin

CMD proven-enclave
