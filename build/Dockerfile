FROM messense/rust-musl-cross:${RUSTARCH}-musl as builder

WORKDIR /

RUN wget https://github.com/nats-io/natscli/releases/download/v0.1.4/nats-0.1.4-linux-amd64.zip
RUN unzip nats-0.1.4-linux-amd64.zip
RUN cp nats-0.1.4-linux-amd64/nats /bin/
RUN chown root:root /bin/nats
RUN chmod 0755      /bin/nats

ARG COMMIT
RUN git clone git@github.com:proven-network/proven-node.git
WORKDIR /proven-node
RUN git checkout ${COMMIT}

ARG RUSTTARGET
RUN rustup target add ${RUSTTARGET}
RUN cargo build --release --target ${RUSTTARGET} --package proven-enclave

RUN cp /proven-node/target/${RUSTTARGET}/release/proven-enclave /bin/
RUN chown root:root /bin/proven-enclave
RUN chmod 0755      /bin/proven-enclave

FROM nats:alpine3.20

RUN apk add --no-cache dnscrypt-proxy iproute2 iproute2-tc net-tools nfs-utils

COPY --from=builder /bin/proven-enclave /bin/
COPY --from=builder /bin/nats /bin/

CMD proven-enclave
