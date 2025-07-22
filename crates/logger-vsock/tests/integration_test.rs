//! Integration test with both client and server

#[cfg(test)]
mod tests {
    use proven_logger_vsock::{
        LogLevel,
        client::{VsockLoggerConfig, VsockSubscriber},
        server::{ChannelLogProcessor, VsockLogServerBuilder},
    };
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::timeout;
    use tracing::{debug, error, info, trace, warn};
    use tracing_subscriber::prelude::*;

    #[tokio::test]
    async fn test_client_server_integration() {
        // Use TCP for testing since VSOCK requires special setup
        // Using a fixed port for testing
        let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 5558));

        // Create a channel processor to capture logs
        let (processor, mut receiver) = ChannelLogProcessor::new(100);

        // Start server
        let server = VsockLogServerBuilder::new(addr)
            .processor(Arc::new(processor))
            .build()
            .await
            .expect("Failed to build server");

        // Start server in background
        let server_handle = tokio::spawn(async move { server.serve().await });

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create client
        let client_config = VsockLoggerConfig::builder()
            .vsock_addr(addr)
            .batch_size(5)
            .batch_interval(Duration::from_millis(50))
            .min_level(LogLevel::Info)
            .build();

        let vsock_subscriber = VsockSubscriber::new(client_config)
            .await
            .expect("Failed to create subscriber");

        // Initialize tracing
        let _guard = tracing_subscriber::registry()
            .with(vsock_subscriber)
            .set_default();

        // Send some logs
        for i in 0..10 {
            info!("Test log message {}", i);
        }

        // Wait for batches to be sent
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Check that we received logs
        let mut total_entries = 0;
        let mut batches_received = 0;

        // Collect all batches with timeout
        while let Ok(Some(batch)) = timeout(Duration::from_millis(100), receiver.recv()).await {
            batches_received += 1;
            total_entries += batch.entries.len();

            // Verify batch contents
            for entry in &batch.entries {
                assert_eq!(entry.level, LogLevel::Info);
                assert!(entry.message.starts_with("Test log message"));
            }
        }

        // We sent 10 messages with batch size 5, so we should get 2 batches
        assert_eq!(batches_received, 2);
        assert_eq!(total_entries, 10);

        // Shutdown
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_level_filtering() {
        let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 5559));

        // Create a channel processor
        let (processor, mut receiver) = ChannelLogProcessor::new(100);

        // Start server
        let server = VsockLogServerBuilder::new(addr)
            .processor(Arc::new(processor))
            .build()
            .await
            .expect("Failed to build server");

        let server_handle = tokio::spawn(async move {
            let _ = server.serve().await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create client with Info level filter
        let client_config = VsockLoggerConfig::builder()
            .vsock_addr(addr)
            .batch_size(10)
            .batch_interval(Duration::from_millis(50))
            .min_level(LogLevel::Info)
            .build();

        let vsock_subscriber = VsockSubscriber::new(client_config)
            .await
            .expect("Failed to create subscriber");

        // Initialize tracing
        let _guard = tracing_subscriber::registry()
            .with(vsock_subscriber)
            .set_default();

        // Log at different levels
        trace!("This should be filtered out");
        debug!("This should also be filtered");
        info!("This should be sent");
        warn!("This should be sent too");
        error!("And this");

        // Force flush
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Check received logs
        if let Ok(Some(batch)) = timeout(Duration::from_millis(100), receiver.recv()).await {
            assert_eq!(batch.entries.len(), 3);
            assert_eq!(batch.entries[0].level, LogLevel::Info);
            assert_eq!(batch.entries[1].level, LogLevel::Warn);
            assert_eq!(batch.entries[2].level, LogLevel::Error);
        } else {
            panic!("Did not receive expected batch");
        }

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_span_context() {
        let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 5560));

        // Create a channel processor
        let (processor, mut receiver) = ChannelLogProcessor::new(100);

        // Start server
        let server = VsockLogServerBuilder::new(addr)
            .processor(Arc::new(processor))
            .build()
            .await
            .expect("Failed to build server");

        let server_handle = tokio::spawn(async move {
            let _ = server.serve().await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create client
        let client_config = VsockLoggerConfig::builder()
            .vsock_addr(addr)
            .batch_size(10)
            .batch_interval(Duration::from_millis(50))
            .build();

        let vsock_subscriber = VsockSubscriber::new(client_config)
            .await
            .expect("Failed to create subscriber");

        // Initialize tracing
        let _guard = tracing_subscriber::registry()
            .with(vsock_subscriber)
            .set_default();

        // Log with span context
        {
            let _span = tracing::info_span!("node:test-node-1").entered();
            info!("Message with node context");

            {
                let _inner_span = tracing::info_span!("inner_operation").entered();
                info!("Message in nested span");
            }
        }

        // Force flush
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Check received logs
        if let Ok(Some(batch)) = timeout(Duration::from_millis(100), receiver.recv()).await {
            assert_eq!(batch.entries.len(), 2);

            // First message should have node_id from span
            assert_eq!(batch.entries[0].node_id, Some("test-node-1".to_string()));
            assert_eq!(
                batch.entries[0].component,
                Some("node:test-node-1".to_string())
            );

            // Second message should also have node_id (inherited from parent span)
            assert_eq!(batch.entries[1].node_id, Some("test-node-1".to_string()));
            assert_eq!(
                batch.entries[1].component,
                Some("inner_operation".to_string())
            );
        } else {
            panic!("Did not receive expected batch");
        }

        server_handle.abort();
    }
}
