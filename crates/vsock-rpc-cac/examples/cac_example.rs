//! Example of using the CAC RPC system.

use async_trait::async_trait;
use cidr::Ipv4Cidr;
use proven_vsock_rpc::VsockAddr;
use proven_vsock_rpc_cac::{
    CacServer, InitializeRequest, InitializeResponse, Result, ShutdownResponse,
    commands::ShutdownRequest, server::CacCommandHandler,
};
use std::net::Ipv4Addr;
use std::str::FromStr;

/// Example command handler implementation.
struct ExampleHandler;

#[async_trait]
impl CacCommandHandler for ExampleHandler {
    async fn handle_initialize(&self, request: InitializeRequest) -> Result<InitializeResponse> {
        println!("Received initialize request:");
        println!("  Enclave IP: {}", request.enclave_ip);
        println!("  Host IP: {}", request.host_ip);
        println!("  HTTPS Port: {}", request.https_port);
        println!("  Testnet: {}", request.testnet);

        // In a real implementation, you would:
        // 1. Configure networking
        // 2. Set up certificates
        // 3. Initialize services
        // 4. etc.

        Ok(InitializeResponse {
            success: true,
            error: None,
        })
    }

    async fn handle_shutdown(&self, request: ShutdownRequest) -> Result<ShutdownResponse> {
        println!("Received shutdown request:");
        if let Some(grace_period) = request.grace_period_secs {
            println!("  Grace period: {grace_period} seconds");
        }

        // In a real implementation, you would:
        // 1. Stop accepting new requests
        // 2. Wait for in-flight requests to complete
        // 3. Clean up resources
        // 4. Exit gracefully

        Ok(ShutdownResponse {
            success: true,
            message: Some("Shutdown initiated".to_string()),
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("CAC RPC Example");
    println!("===============");
    println!();

    // Server setup
    println!("Server Configuration:");
    let server_addr = VsockAddr::new(2, 5001); // CID 2, Port 5001
    println!("  Address: {server_addr:?}");

    let handler = ExampleHandler;
    let _server = CacServer::new(server_addr, handler);
    println!("  Server created (would call server.serve() to start)");
    println!();

    // Client example
    println!("Client Usage Example:");
    println!("```rust");
    println!("// Create client");
    println!("let client = CacClient::new(server_addr).await?;");
    println!();
    println!("// Initialize enclave");
    println!("let init_request = InitializeRequest {{");
    println!("    enclave_ip: Ipv4Addr::new(10, 0, 0, 2),");
    println!("    host_ip: Ipv4Addr::new(10, 0, 0, 1),");
    println!("    cidr: Ipv4Cidr::from_str(\"10.0.0.0/24\")?,");
    println!("    // ... other fields ...");
    println!("}};");
    println!();
    println!("let response = client.initialize(init_request).await?;");
    println!("if response.success {{");
    println!("    println!(\"Enclave initialized successfully\");");
    println!("}} else {{");
    println!("    println!(\"Initialization failed: {{:?}}\", response.error);");
    println!("}}");
    println!();
    println!("// Shutdown enclave");
    println!("let response = client.shutdown().await?;");
    println!("println!(\"Shutdown response: {{:?}}\", response);");
    println!("```");

    // Show sample request
    println!();
    println!("Sample Initialize Request:");
    let sample_request = InitializeRequest {
        certificates_bucket: "my-certs-bucket".to_string(),
        cidr: Ipv4Cidr::from_str("10.0.0.0/24").unwrap(),
        email: vec!["admin@example.com".to_string()],
        enclave_ip: Ipv4Addr::new(10, 0, 0, 2),
        file_systems_bucket: "my-fs-bucket".to_string(),
        host_dns_resolv: "nameserver 8.8.8.8\nnameserver 8.8.4.4".to_string(),
        host_ip: Ipv4Addr::new(10, 0, 0, 1),
        https_port: 443,
        kms_key_id: "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"
            .to_string(),
        max_runtime_workers: 4,
        nats_cluster_port: 4222,
        node_key: "ed25519_private_key_here".to_string(),
        nfs_mount_point: "/mnt/external".to_string(),
        proxy_port: 3000,
        skip_fsck: false,
        skip_speedtest: true,
        skip_vacuum: false,
        sql_snapshots_bucket: "my-sql-snapshots".to_string(),
        radix_mainnet_port: 30000,
        radix_stokenet_port: 30001,
        testnet: false,
    };

    println!("{sample_request:#?}");

    Ok(())
}
