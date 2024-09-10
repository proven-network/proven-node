# --------------
# GO BUILD STEPS
# --------------
# FROM golang:alpine3.20 as go-builder
# WORKDIR /

# # Install dependencies
# RUN apk add --no-cache bash git

# # Build gocryptfs
# RUN git clone https://github.com/rfjakob/gocryptfs.git
# WORKDIR /gocryptfs
# RUN ./build-without-openssl.bash

# # Move gocryptfs to /bin
# RUN cp /gocryptfs/gocryptfs /bin/
# RUN chown root:root /bin/gocryptfs
# RUN chmod 0755      /bin/gocryptfs

# ----------------
# RUST BUILD STEPS
# ----------------
# FROM messense/rust-musl-cross:${RUSTARCH}-musl as rust-builder
# WORKDIR /

# # Set Rust target
# ARG RUSTTARGET
# RUN rustup target add ${RUSTTARGET}

# # Build proven-enclave
# WORKDIR /
# ARG COMMIT
# RUN git clone git@github.com:proven-network/proven-node.git
# WORKDIR /proven-node
# RUN git checkout ${COMMIT}
# RUN cargo build --release --target ${RUSTTARGET} --package proven-enclave

# # Move proven-enclave to /bin
# RUN cp /proven-node/target/${RUSTTARGET}/release/proven-enclave /bin/
# RUN chown root:root /bin/proven-enclave
# RUN chmod 0755      /bin/proven-enclave

# -----------
# FINAL IMAGE
# -----------
FROM nats:alpine3.20

RUN apk add --no-cache dnscrypt-proxy iproute2 iproute2-tc net-tools nfs-utils wget

# Download babylon-node
RUN wget https://github.com/radixdlt/babylon-node/releases/download/v1.2.2/babylon-node-rust-arch-linux-aarch64-release-v1.2.2.zip
RUN wget https://github.com/radixdlt/babylon-node/releases/download/v1.2.3/babylon-node-v1.2.3.zip

# Unzip babylon-node
RUN unzip babylon-node-rust-arch-linux-aarch64-release-v1.2.2.zip
RUN unzip babylon-node-v1.2.3.zip
RUN ls -lah

# COPY --from=go-builder /bin/gocryptfs /bin/
# COPY --from=rust-builder /bin/proven-enclave /bin/

CMD proven-enclave
