# --------------
# GO BUILD STEPS
# --------------
FROM golang:alpine as go-builder
WORKDIR /

# Install dependencies
RUN apk add --no-cache git

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

# Build Rust component of babylon-node
RUN git clone git@github.com:radixdlt/babylon-node.git
WORKDIR /babylon-node/core-rust
RUN cargo build --release --target ${RUSTTARGET}

# Move libcorerust.so to /bin
RUN cp /babylon-node/core-rust/target/${RUSTTARGET}/release/libcorerust.so /bin/
RUN chown root:root /bin/libcorerust.so
RUN chmod 0755      /bin/libcorerust.so

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

# -----------
# FINAL IMAGE
# -----------
FROM nats:alpine3.20

RUN apk add --no-cache dnscrypt-proxy iproute2 iproute2-tc net-tools nfs-utils

COPY --from=go-builder /bin/gocryptfs /bin/
COPY --from=rust-builder /bin/proven-enclave /bin/
COPY --from=rust-builder /bin/libcorerust.so /bin/

CMD proven-enclave
