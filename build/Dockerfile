FROM messense/rust-musl-cross:${RUSTARCH}-musl as builder

WORKDIR /

ARG COMMIT
RUN git clone git@github.com:proven-network/proven-node.git
WORKDIR /proven-node
RUN git checkout ${COMMIT}

ARG RUSTTARGET
RUN rustup target add ${RUSTTARGET}
RUN cargo build --release --target ${RUSTTARGET}

RUN cp /proven-node/target/${RUSTTARGET}/release/proven-enclave /bin/
RUN chown root:root /bin/proven-enclave
RUN chmod 0755      /bin/proven-enclave

FROM nats:alpine3.19

RUN apk add --no-cache iproute2 net-tools iproute2-tc nfs-utils

COPY --from=builder /bin/proven-enclave /bin/

CMD proven-enclave
