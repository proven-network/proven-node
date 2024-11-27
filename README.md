# Proven Node

This repo contains code for Proven Network nodes. Proven is a novel TEE-based platform designed to complement the Radix DLT. It provides:

⭐ End-to-end zero-trust networking and full execution verifiability through trusted-execution environments.

⭐ A distributed highly-available control plane which scales horizontally to a global super-cluster.

⭐ Fully embeded Radix DLT nodes to ensure complete verifiability of network read operations.

⭐ A custom and highly-performant (sub-millisecond) runtime for application code based on V8 isolates.

⭐ The ability to trigger FaaS code via front-end SDK RPC, HTTP APIs, or events from Radix DLT or other Proven apps.

⭐ Tight integrations with Radix DLT SDKs, tooling, and network primitives (such as identity and accounts).

⭐ A novel deployment model for application code which gives full visibility and auditability to dApps.

⭐ Batteries-included access to encrypted storage (both KV and SQL) scopable to any of: app, user (persona), or NFT.

## Subdirectories
- [build](build): Build process for bundling all enclave components of the node into an EIF (enclave image format) - including related items such as the Radix Babylon Node. Uses [kaniko](https://github.com/GoogleContainerTools/kaniko) to ensure reproducibility of build.
- [crates](crates): All of the proven node components. Described below in more detail.
- [kernel](kernel): Build process for enclave's Linux micro-kernel; used as part of creating the final EIF.

## Crates
- [applications](crates/applications): Manages databse of all currently deployed applications.
- [attestation](crates/attestation): Abstract interface for managing remote attestation and interacting with hardware-based security modules.
- [attestation-dev](crates/attestation-dev): Noop implementation of attestation for local development.
- [attestation-nsm](crates/attestation-nsm): Implementation of attestation using the Nitro Security Module.
- [core](crates/core): Core logic for the Proven node handling all RPC and user interactions.
- [dnscrypt-proxy](crates/dnscrypt-proxy): Configures and runs a DNSCrypt proxy to ensure all DNS runs over tamper-proof HTTPS.
- [enclave](crates/enclave): Main entrypoint for enclave images. Bootstraps all other components before handing off to core.
- [external-fs](crates/external-fs): Mounts external filesystems into the enclave via NFS, intermediated by a layer of FUSE-based AES-GCM disk-encryption based on enclave-internal cryptographic keys.
- [host](crates/host): Binary to run on the host machine to manage enclave lifecycle.
- [http](crates/http): Abstract interface for running an Axum-based HTTP server.
- [http-insecure](crates/http-insecure): Implementation of simple non-secure HTTP for local development.
- [http-letsencrypt](crates/http-letsencrypt): Implementation of secure HTTPS server using Let's Encrypt and enclave-only storage of certificates provisioned using draft-ietf-acme-tls-alpn-01.
- [imds](crates/imds): Helper crate to interact with the Instance Metadata Service. Verifies all recieved data via embedded public certificates.
- [instance-details](crates/instance-details): Helper crate to interact with the EC2 APIs and securely retrieve info about runtime environment.
- [kms](crates/kms): Manages encryption/decryption of plain/cipher text using AWS KMS keys which are scoped to the EIF PCRs.
- [libsql](crates/libsql): Wrapper around [libsql](https://github.com/tursodatabase/libsql) which provides additional functionality like migration management.
- [local](crates/local): Binary to bootstrap other components locally. Similar to [enclave](crates/enclave) but for local development.
- [locks](crates/locks): Abstract interface for managing system-global distributed locks.
- [locks-memory](crates/locks-memory): In-memory (single node) implementation of locks for local development.
- locks-nats: TODO: Implementation of distributed locks using NATS with HA replication.
- [nats-monitor](crates/nats-monitor): Helper crate for querying NATS HTTP monitoring endpoints.
- [nats-server](crates/nats-server): Configures and runs a NATS server for inter-enclave communication.
- [postgres](crates/postgres): Configures and runs a Postgres server to provide storage for Radix Gateway.
- [radix-aggregator](crates/radix-aggregator): Configures and runs Radix Gateway's data aggregator. Also manages migration process as part of boot.
- [radix-gateway](crates/radix-gateway): Configures and runs a local Radix Gateway.
- [radix-gateway-sdk](crates/radix-gateway-sdk): Rust port of the Radix Gateway SDK.
- [radix-node](crates/radix-node): Configures and runs a local Radix Babylon Node.
- [radix-rola](crates/radix-rola): Rust port of the Radix ROLA example code.
- [runtime](crates/runtime): Manages a pool of V8 isolates for running proven application code.
- [sessions](crates/sessions): Manages all user sessions (created via ROLA) and their associated data.
- [sql](crates/sql): Abstract interface for managing SQL storage.
- [sql-direct](crates/sql-direct): Implementation of SQL storage using files on disk, for local development.
- [sql-streamed](crates/sql-streamed): Implementation of SQL storage using a streams as an append-only log.
- [store](crates/store): Abstract interface for managing key-value storage.
- [store-asm](crates/store-asm): Implementation of key-value storage using AWS Secrets Manager. Values typically double-encrypted using KMS with enclave-specific keys.
- [store-fs](crates/store-fs): Implementation of key-value storage using files on disk, for local development.
- [store-memory](crates/store-memory): In-memory (single node) implementation of key-value storage for local development.
- [store-nats](crates/store-nats): Implementation of key-value storage using NATS with HA replication.
- [store-s3](crates/store-s3): Implementation of key-value storage using AWS S3. Values encrypted using AES-256 via SSE-C.
- [stream](crates/stream): Abstract interface for managing streams.
- [stream-memory](crates/stream-memory): In-memory (single node) implementation of streams for local development.
- [stream-nats](crates/stream-nats): Implementation of streams using NATS with HA replication.
- [vsock-proxy](crates/vsock-proxy): High performance layer-3 proxy for enabling enclave networking over virtio sockets.
- [vsock-rpc](crates/vsock-rpc): Manages RPC between host and enclave over virtio sockets. Defines commands like initialize, shutdown, add peer, etc.
- [vsock-tracing](crates/vsock-tracing): Provides a tracing subscriber to allow logs to be sent from enclave to host for processing.
