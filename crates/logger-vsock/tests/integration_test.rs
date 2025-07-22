//! Integration test with both client and server

#[cfg(test)]
mod tests {
    use proven_logger::{Level, LoggerExt};
    use proven_logger_vsock::{
        client::{VsockLogger, VsockLoggerConfig},
        server::{ChannelLogProcessor, VsockLogServerBuilder},
    };
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::timeout;

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
            .flush_interval(Duration::from_millis(50))
            .min_level(Level::Info)
            .build();

        let logger = Arc::new(
            VsockLogger::new(client_config)
                .await
                .expect("Failed to create logger"),
        );

        // Initialize global logger
        let _ = proven_logger::init(logger.clone());

        // Send some logs
        for i in 0..10 {
            proven_logger::info!("Test log message {i}");
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
                assert_eq!(entry.level, Level::Info);
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
            .flush_interval(Duration::from_millis(50))
            .min_level(Level::Info)
            .build();

        let logger = Arc::new(
            VsockLogger::new(client_config)
                .await
                .expect("Failed to create logger"),
        );

        // Log at different levels
        logger.trace("This should be filtered out");
        logger.debug("This should also be filtered");
        logger.info("This should be sent");
        logger.warn("This should be sent too");
        logger.error("And this");

        // Force flush
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Check received logs
        if let Ok(Some(batch)) = timeout(Duration::from_millis(100), receiver.recv()).await {
            assert_eq!(batch.entries.len(), 3);
            assert_eq!(batch.entries[0].level, Level::Info);
            assert_eq!(batch.entries[1].level, Level::Warn);
            assert_eq!(batch.entries[2].level, Level::Error);
        } else {
            panic!("Did not receive expected batch");
        }

        server_handle.abort();
    }
}
