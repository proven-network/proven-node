# Proven Node

This repo contains code for Proven Network nodes. Proven is a novel TEE-based platform designed to complement blockchains. It provides:

⭐ End-to-end zero-trust networking and full execution verifiability through trusted-execution environments.

⭐ A distributed highly-available system which can scale horizontally to a geo-distributed edge network.

⭐ Embeded blockchain/DLT full nodes to ensure verifiability of all network read operations.

⭐ A custom and highly-performant (sub-millisecond) runtime for application code based on V8 isolates.

⭐ The ability to trigger FaaS code via front-end SDK RPC, HTTPS APIs, cron schedules, or events from blockchains and other Proven apps.

⭐ Close integrations with Web3 SDKs, tooling, and network primitives (such as identity and accounts).

⭐ A novel deployment model for application code which gives end-users full visibility and auditability of dApps.

⭐ Batteries-included access to encrypted storage (KV, SQL, and file) scopable to any of: app, user (persona), or NFT.

⭐ Best-in-class DevEx for TEE development, tight integration with typical front-end tooling/LSPs, and simple on-chain settlement for fees.

## Integated full nodes

- [Bitcoin](https://bitcoin.org)
- [Ethereum](https://ethereum.org)
- [Radix DLT](https://www.radixdlt.com)
- More to come...

## Security Model

The Proven Network operates under a security model which can roughly be classified into four layers:

**The Governance Layer**

Network governance is managed as a decentralized autonomous organisation on a decentralised blockchain. Network stakeholders (primarily application developers and infrastructure providers) vote on both the node membership to the network, as well as on any versioned updates to network code. This decentralised source of truth is read directly by both nodes and end-user SDKs, using Ethereum Light Clients at runtime, to provide a consistent view of which [other] nodes and versions should be considered trust-worthy.

Ethereum was selected as the "homebase" blockchain for this governance process - given that it has the highest economic security available, as well as the highest level of maturity in it's light client tooling. Additionally, the relative slowness of Ethereum block-times makes syncing light clients against Ethereum mainnet much quicker than any other option.

**The Hardware Layer**

The code for any version of a Proven node can be built by any third-party through a reproducible process resulting in concrete build measurements. These measurements: Platform Configuration Registers (or PCRs) ensure bit-for-bit that the packaged application (including kernel, tempfs, etc.) is the same as is deployed to any live node.<br><br>
Nodes can confirm these PCR measurements to any stakeholder (whether that is an end-user, application developer, or another Proven node) via a process of remote attestation. This involves sending a request (including a requestor-defined nonce) which the TEE hardware will reply to with a hardware-signed attestation report - containing both the nonce and the PCR measurements. These reports can then be verified using the well-known public keys of the hardware vendor, as well as cross-referencing active versions from the governance layer described above.

**The Node Layer**

Given the node software is fully auditable via the above process - we can now [carefully] build back networked capabilities. Critically, this means, all functionality must start from a baseline of zero-trust in external systems (or even the parties running the infrastructure). All inputs and outputs of the TEE must be encrypted, or otherwise cryptographically verified by trust models rooted in the shipped images. Practically, this means things like:
<br>

- Nodes can provide an API, but only if TLS termination is contained within the enclave, and with certificates fully provisioned and stored within the enclave only.
- Nodes can resolve DNS, but only over a tamper-resistent protocols like DoH, and with explicit acknowledgement of the DNS resolver's role in the overall threat model.
- Nodes can connect to external APIs and systems, but only if the connection is over encrypted channels (like HTTPS or WSS), and using a root of trust which is shipped explicitly as part of the node software.
- Nodes can mount external file-systems, but only if all data is encrypted before it leaves the enclave, and decrypted only within the enclave - using keys which are only available inside the enclave.
- Nodes can trust the state of blockchains, but only if each enclave runs a complete stack of node software itself, and doesn't rely on any third-party nodes, RPCs, or gateways.
- Nodes can trust each other, but only if they pass each other's remote attestation checks, ensuring true peer-ship and a uniform threat model.

**The Application Layer**

Finally, we can use this hardware-secured platform, with capabilities enabled by zero-trust networking - to provide a scalable serverless platform for deploying trust-minimised backends. These applications are deployed to the Proven Network as self-describing ESM modules which are fully scrutable to end-users. The upgradability of Proven applications also somewhat mirrors the hardware layer - where new versions are new modules, with new hashes. These new versions may be deployed by developers - but only in accordance with well-described upgrade policies (and with audit logs) transparent to any user.

## Subdirectories

- [build](build): Build process for bundling all enclave components of the node into an EIF (enclave image format) - including related dependencies described below. The process uses [kaniko](https://github.com/GoogleContainerTools/kaniko) to ensure reproducibility of builds.
- [crates](crates): All of the proven node components. Described below in more detail.
- [kernel](kernel): Build process for enclave's Linux micro-kernel; used as part of creating the final EIF.
- [vendor](vendor): Vendored forks of select other crates. Mostly for the purpose of resolving thorny cargo dependency issues.

## TEE Base Environment

Presently uses a Debian Bookworm base distro running on a Linux 6.12 micro-kernel. The kernel config generally mirrors the default AWS Nitro Image, packaged with their CLI tooling, except for using a more up-to-date kernel version, and enabling of FUSE and NFSv3 sub-systems.

## Bundled TEE Dependencies

Updates to these components will generally require a rebuild and upgrade of Proven nodes.

- [babylon-gateway](https:://github.com/radixdlt/babylon-gateway): Radix Babylon Gateway for accessing read data from the Radix DLT.
- [babylon-node](https://github.com/radixdlt/babylon-node): Radix Babylon Core Node for driving above gateway.
- [bitcoin-core](https://github.com/bitcoin/bitcoin): Bitcoin Core full node for interacting with the Bitcoin network.
- [dnscrypt-proxy](https://github.com/DNSCrypt/dnscrypt-proxy): DNSCrypt Proxy to ensure host cannot tamper with DNS.
- [dotnet](https://dotnet.microsoft.com/): .NET runtime for running gateway.
- [gocryptfs](https://github.com/rfjakob/gocryptfs): Used for encrypting NFS-mounted external filesystems. Threat model described [here](https://nuetzlich.net/gocryptfs/threat_model/).
- [lighthouse](https://github.com/sigp/lighthouse): Ethereum consensus client to pair with Reth for full Ethereum node functionality.
- [nats-server](https://github.com/nats-io/nats-server): NATS server for inter-node communication and control plane.
- [openjdk](https://openjdk.java.net/): Java runtime for Java component of Radix DLT core node.
- [postgres](https://www.postgresql.org/): Postgres server for storing gateway data.
- [reth](https://github.com/paradigmxyz/reth): Ethereum execution client for running an Ethereum full node.

## Crates

- [applications](crates/applications): Manages database of all currently deployed applications.
- [attestation](crates/attestation): Abstract interface for managing remote attestation and interacting with hardware-based security modules.
- [attestation-mock](crates/attestation-mock): Noop implementation of attestation for local development.
- [attestation-nsm](crates/attestation-nsm): Implementation of attestation using the Nitro Security Module.
- [bitcoin-core](crates/bitcoin-core): Configures and runs a local Bitcoin Core full node.
- [bootable](crates/bootable): Abstract interface for bootable services.
- [cert-store](crates/cert-store): Shared cert store for both HTTPS and NATS.
- [code-package](crates/code-package): Tools for creating and working with code packages runnable in the Proven runtime.
- [core](crates/core): Core logic for the Proven node and the entrypoint for all user interactions.
- [dnscrypt-proxy](crates/dnscrypt-proxy): Configures and runs a DNSCrypt proxy to ensure all DNS runs over tamper-proof HTTPS.
- [enclave](crates/enclave): Main entrypoint for enclave images. Bootstraps all other components before handing off to core.
- [ethereum-lighthouse](crates/ethereum-lighthouse): Configures and runs a local Lighthouse consensus client.
- [ethereum-reth](crates/ethereum-reth): Configures and runs a local Reth execution client.
- [external-fs](crates/external-fs): Mounts external filesystems into the enclave via NFS, intermediated by a layer of FUSE-based AES-GCM disk-encryption based on enclave-internal cryptographic keys.
- [governance](crates/governance): Abstract interface for getting active version information, network topology, etc. from a governance mechanism.
- [governance-helios](crates/governance-helios): Helios light-client based implementation of the governance interface.
- [governance-mock](crates/governance-mock): Mock implementation of the governance interface for testing.
- [headers](crates/headers): Common HTTP header definitions for use in other crates.
- [host](crates/host): Binary to run on the host machine to manage enclave lifecycle.
- [http](crates/http): Abstract interface for running an Axum-based HTTP server.
- [http-insecure](crates/http-insecure): Implementation of simple non-secure HTTP for local development.
- [http-letsencrypt](crates/http-letsencrypt): Implementation of secure HTTPS server using Let's Encrypt and enclave-only storage of certificates provisioned using draft-ietf-acme-tls-alpn-01.
- [http-proxy](crates/http-proxy): Routes local HTTP requests from loopback to target services over messaging primitives.
- [identity](crates/identity): Manages user identities and associated data.
- [imds](crates/imds): Helper crate to interact with the Instance Metadata Service. Verifies all recieved data via embedded public certificates.
- [instance-details](crates/instance-details): Helper crate to interact with the EC2 APIs and securely retrieve info about runtime environment.
- [isolation](crates/isolation): Isolation primitives for spawning untrusted processes (particularly third-party nodes) in a secure manner.
- [kms](crates/kms): Manages encryption/decryption of plain/cipher text using AWS KMS keys which are scoped to the EIF PCRs.
- [libsql](crates/libsql): Wrapper around [libsql](https://github.com/tursodatabase/libsql) which provides additional functionality like migration management.
- [local](crates/local): Binary to bootstrap other components locally. Similar to [enclave](crates/enclave) but for local development.
- [locks](crates/locks): Abstract interface for managing system-global distributed locks.
- [locks-memory](crates/locks-memory): In-memory (single node) implementation of locks for local development.
- [locks-nats](crates/locks-nats): Implementation of distributed locks using NATS Jetstream with HA replication.
- [messaging](crates/messaging): Abstract interface for pub/sub messaging across subjects.
- [messaging-memory](crates/messaging-memory): In-memory (single node) implementation of messaging for local development.
- [messaging-nats](crates/messaging-nats): Implementation of messaging using Core NATS.
- [nats-monitor](crates/nats-monitor): Helper crate for querying NATS HTTP monitoring endpoints.
- [nats-server](crates/nats-server): Configures and runs a NATS server for inter-node communication.
- [network](crates/network): Manages network configuration and point-to-point node communication.
- [passkeys](crates/passkeys): Manages active passkeys for discoverable WebAuthn.
- [postgres](crates/postgres): Configures and runs a Postgres server to provide storage for Radix Gateway.
- [radix-aggregator](crates/radix-aggregator): Configures and runs Radix Gateway's data aggregator. Also manages migration process as part of boot.
- [radix-gateway](crates/radix-gateway): Configures and runs a local Radix Gateway.
- [radix-gateway-sdk](crates/radix-gateway-sdk): Rust port of the Radix Gateway SDK.
- [radix-nft-verifier](crates/radix-nft-verifier): Verifies NFTs on Radix DLT.
- [radix-nft-verifier-gateway](crates/radix-nft-verifier-gateway): An implementation of the NFT verifier which uses the Radix Gateway.
- [radix-nft-verifier-mock](crates/radix-nft-verifier-mock): Mock implementation of NFT verifier for testing.
- [radix-node](crates/radix-node): Configures and runs a local Radix Babylon Node.
- [radix-rola](crates/radix-rola): Rust port of the Radix ROLA example code.
- [radix-stream](crates/radix-stream): Handles processing transactions and events from Radix DLT.
- [runtime](crates/runtime): Manages a pool of V8 isolates for running proven application code.
- [sessions](crates/sessions): Manages RPC sessions and associated data.
- [sql](crates/sql): Abstract interface for managing SQL storage.
- [sql-direct](crates/sql-direct): Implementation of SQL storage using files on disk, for local development.
- [sql-streamed](crates/sql-streamed): Implementation of SQL storage using a streams as an append-only log.
- [store](crates/store): Abstract interface for managing key-value storage.
- [store-asm](crates/store-asm): Implementation of key-value storage using AWS Secrets Manager. Values typically double-encrypted using KMS with enclave-specific keys.
- [store-fs](crates/store-fs): Implementation of key-value storage using files on disk, for local development.
- [store-memory](crates/store-memory): In-memory (single node) implementation of key-value storage for local development.
- [store-nats](crates/store-nats): Implementation of key-value storage using NATS Jetstream with HA replication.
- [store-s3](crates/store-s3): Implementation of key-value storage using AWS S3. Values encrypted using AES-256 via SSE-C.
- [util](crates/util): Misc common utilities and value types.
- [vsock-proxy](crates/vsock-proxy): High performance layer-3 proxy for enabling enclave networking over virtio sockets.
- [vsock-rpc](crates/vsock-rpc): Manages RPC between host and enclave over virtio sockets. Defines commands like initialize, shutdown, add peer, etc.
- [vsock-tracing](crates/vsock-tracing): Provides a tracing subscriber to allow logs to be sent from enclave to host for processing.
