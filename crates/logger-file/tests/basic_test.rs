//! Basic tests for file-based logger

use proven_logger::{Logger, LoggerExt};
use proven_logger_file::{
    DefaultCategorizer, FileLogger, FileLoggerConfigBuilder, JsonFormatter, LevelCategorizer,
    PlainTextFormatter,
};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

#[tokio::test]
async fn test_basic_file_logging() {
    let temp_dir = TempDir::new().unwrap();
    println!("Test directory: {:?}", temp_dir.path());

    let config = FileLoggerConfigBuilder::new()
        .log_dir(temp_dir.path())
        .file_prefix("test")
        .flush_interval(Duration::from_millis(50))
        .build();

    let logger = Arc::new(
        FileLogger::new(
            config,
            Arc::new(PlainTextFormatter::default()),
            Arc::new(DefaultCategorizer),
        )
        .await
        .unwrap(),
    );

    println!("Logger created, logging messages...");

    // Log some messages
    logger.info("Test info message");
    logger.error("Test error message");
    logger.debug("Test debug message");

    // Flush and wait
    logger.flush();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Check that files were created
    println!("Checking for files in: {:?}", temp_dir.path());
    let entries: Vec<_> = std::fs::read_dir(temp_dir.path()).unwrap().collect();
    println!("Found {} entries", entries.len());
    for e in entries.iter().flatten() {
        println!("  - {:?}", e.file_name());
    }
    assert!(!entries.is_empty(), "No log files created");

    // Read the log file
    let log_file = entries
        .iter()
        .find(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_str()
                .unwrap()
                .ends_with(".log")
        })
        .expect("No .log file found");

    let content = std::fs::read_to_string(log_file.as_ref().unwrap().path()).unwrap();
    assert!(content.contains("Test info message"));
    assert!(content.contains("Test error message"));
    assert!(content.contains("Test debug message"));
}

#[tokio::test]
async fn test_json_formatting() {
    let temp_dir = TempDir::new().unwrap();

    let config = FileLoggerConfigBuilder::new()
        .log_dir(temp_dir.path())
        .file_prefix("json_test")
        .flush_interval(Duration::from_millis(50))
        .build();

    let logger = Arc::new(
        FileLogger::new(
            config,
            Arc::new(JsonFormatter::default()),
            Arc::new(DefaultCategorizer),
        )
        .await
        .unwrap(),
    );

    logger.info("JSON formatted message");
    logger.flush();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Find and read the log file
    let entries: Vec<_> = std::fs::read_dir(temp_dir.path()).unwrap().collect();
    let log_file = entries
        .iter()
        .find(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_str()
                .unwrap()
                .ends_with(".log")
        })
        .expect("No .log file found");

    let content = std::fs::read_to_string(log_file.as_ref().unwrap().path()).unwrap();

    // Parse as JSON
    for line in content.lines() {
        let json: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(json["message"], "JSON formatted message");
        assert_eq!(json["level"], "INFO");
    }
}

#[tokio::test]
async fn test_level_categorization() {
    let temp_dir = TempDir::new().unwrap();

    let config = FileLoggerConfigBuilder::new()
        .log_dir(temp_dir.path())
        .file_prefix("level")
        .flush_interval(Duration::from_millis(50))
        .build();

    let logger = Arc::new(
        FileLogger::new(
            config,
            Arc::new(PlainTextFormatter::default()),
            Arc::new(LevelCategorizer),
        )
        .await
        .unwrap(),
    );

    logger.error("Error message");
    logger.info("Info message");
    logger.flush();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Check that subdirectories were created
    let error_dir = temp_dir.path().join("error");
    let info_dir = temp_dir.path().join("info");

    assert!(error_dir.exists(), "Error directory not created");
    assert!(info_dir.exists(), "Info directory not created");

    // Check content
    let error_files: Vec<_> = std::fs::read_dir(&error_dir).unwrap().collect();
    assert!(!error_files.is_empty(), "No error log files");

    let info_files: Vec<_> = std::fs::read_dir(&info_dir).unwrap().collect();
    assert!(!info_files.is_empty(), "No info log files");
}
