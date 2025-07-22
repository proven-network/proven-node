//! Basic example of using the libsql logger

use proven_logger::{Context, Logger, LoggerExt};
use proven_logger_libsql::{LibsqlLogger, LibsqlLoggerConfig, LogQuery};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create logger with default config
    let logger = LibsqlLogger::new(LibsqlLoggerConfig::default()).await?;

    // Initialize global logger
    proven_logger::init(logger.clone()).unwrap();

    // Create a logger with context
    let app_logger = logger.with_context(Context::new("example-app").with_node_id("node1"));

    // Log some messages
    app_logger.info("Application started");
    app_logger.debug("Debug information");
    app_logger.warn("This is a warning");
    app_logger.error("This is an error");

    // Wait for batch processing
    sleep(Duration::from_millis(50)).await;

    // Query logs
    let conn = logger.get_connection();
    let conn = conn.lock().await;

    println!("\n=== All logs ===");
    let query = LogQuery::new().limit(10);
    let (sql, params) = query.build_sql();

    let mut rows = conn.query(&sql, params).await?;
    while let Some(row) = rows.next().await? {
        let log = proven_logger_libsql::LogRow::from_row(row)?;
        println!(
            "[{}] {}: {}",
            log.timestamp.format("%H:%M:%S"),
            log.level,
            log.message
        );
    }

    println!("\n=== Warnings and errors only ===");
    let query = LogQuery::new()
        .with_min_level(proven_logger::Level::Warn)
        .limit(10);
    let (sql, params) = query.build_sql();

    let mut rows = conn.query(&sql, params).await?;
    while let Some(row) = rows.next().await? {
        let log = proven_logger_libsql::LogRow::from_row(row)?;
        println!(
            "[{}] {}: {}",
            log.timestamp.format("%H:%M:%S"),
            log.level,
            log.message
        );
    }

    Ok(())
}
