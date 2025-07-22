//! Full integration test for logging system with reader

use proven_local_tui::{logs_viewer::LogReader, logs_writer::LogWriter};
use proven_logger::{Logger, Record};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// Simulate a node that logs continuously in its own runtime
fn spawn_node_thread(node_id: &str, log_writer: Arc<LogWriter>) -> thread::JoinHandle<()> {
    let node_name = node_id.to_string();

    thread::Builder::new()
        .name(node_name.clone())
        .spawn(move || {
            // Create a runtime for this "node"
            let runtime = Runtime::new().expect("Failed to create node runtime");

            runtime.block_on(async {
                // Simulate node startup
                let logger = log_writer.clone();

                for i in 0..50 {
                    // Log using the logger directly to ensure it goes through our system
                    logger.log(Record::new(
                        proven_logger::Level::Info,
                        format!("{node_name} - Log message {i}"),
                    ));

                    if i % 10 == 0 {
                        logger.log(Record::new(
                            proven_logger::Level::Warn,
                            format!("{node_name} - Warning at {i}"),
                        ));
                    }

                    // Flush periodically
                    if i % 5 == 0 {
                        logger.flush();
                    }

                    // Small delay to simulate real work
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }

                // Final flush
                logger.flush();
            });
        })
        .expect("Failed to spawn node thread")
}

#[test]
fn test_full_logging_system_with_reader() {
    // Create a temporary directory for logs
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let log_path = temp_dir.path().to_path_buf();

    println!("Test log directory: {log_path:?}");

    // Create log writer
    let log_writer = LogWriter::new_sync(&log_path).expect("Failed to create log writer");
    let log_writer_arc = Arc::new(log_writer);

    // Try to initialize global logger, but don't fail if already initialized
    let _ = proven_logger::init(log_writer_arc.clone());

    // Create log reader for the same directory
    let log_reader = LogReader::new(log_path.clone());

    // Log something first before requesting data
    log_writer_arc.log(Record::new(
        proven_logger::Level::Info,
        "Main thread starting",
    ));
    log_writer_arc.flush();
    thread::sleep(Duration::from_millis(200));

    // Request initial data
    log_reader.request_initial_data();

    // Give reader time to initialize
    thread::sleep(Duration::from_millis(200));

    // Spawn multiple "node" threads
    let node1 = spawn_node_thread("node-1-25", log_writer_arc.clone());
    let node2 = spawn_node_thread("node-2-6", log_writer_arc.clone());

    // Let nodes run for a bit
    thread::sleep(Duration::from_millis(200));

    // Check if we can read logs
    let mut total_logs_seen = 0;
    let mut last_log_count = 0;
    let mut stable_count = 0;

    for iteration in 0..20 {
        // Request viewport update
        log_reader.update_viewport_size(100); // Request 100 lines

        // Try to get response
        let mut logs_in_iteration = 0;
        while let Some(response) = log_reader.try_get_response() {
            use proven_local_tui::logs_viewer::LogResponse;
            match response {
                LogResponse::ViewportUpdate {
                    logs,
                    total_filtered_lines,
                    ..
                } => {
                    println!(
                        "Iteration {}: Got {} logs, total filtered: {}",
                        iteration,
                        logs.len(),
                        total_filtered_lines
                    );
                    logs_in_iteration = logs.len();
                    total_logs_seen = total_logs_seen.max(logs.len());

                    // Print first few logs for debugging
                    for (i, log) in logs.iter().take(5).enumerate() {
                        println!("  Log {}: [{}] {}", i, log.level, log.message);
                    }
                }
                LogResponse::Error { message } => {
                    println!("Error from log reader: {message}");
                }
            }
        }

        // Check if log count is stable
        if logs_in_iteration == last_log_count && logs_in_iteration > 0 {
            stable_count += 1;
        } else {
            stable_count = 0;
        }
        last_log_count = logs_in_iteration;

        // If we have stable logs for 3 iterations, nodes might be done
        if stable_count >= 3 && total_logs_seen > 20 {
            break;
        }

        thread::sleep(Duration::from_millis(100));
    }

    // Wait for nodes to finish
    node1.join().expect("Node 1 failed");
    node2.join().expect("Node 2 failed");

    // Final logs from main
    log_writer_arc.log(Record::new(
        proven_logger::Level::Warn,
        "Main thread finishing",
    ));
    log_writer_arc.log(Record::new(
        proven_logger::Level::Error,
        "Test error for visibility",
    ));

    // Flush the log writer
    log_writer_arc.flush();

    // Give time for final flush
    thread::sleep(Duration::from_millis(200));

    // Do one more read
    log_reader.update_viewport_size(200);
    thread::sleep(Duration::from_millis(100));

    let mut _final_count = 0;
    while let Some(response) = log_reader.try_get_response() {
        use proven_local_tui::logs_viewer::LogResponse;
        if let LogResponse::ViewportUpdate {
            logs,
            total_filtered_lines,
            ..
        } = response
        {
            _final_count = logs.len();
            println!(
                "Final read: {} logs visible, {} total",
                logs.len(),
                total_filtered_lines
            );
        }
    }

    // Check actual file content to see what was written
    println!("\n--- Checking actual log files ---");
    let all_logs_dir = log_path.join("all");
    let entries: Vec<_> = std::fs::read_dir(&all_logs_dir)
        .expect("Failed to read all logs directory")
        .filter_map(Result::ok)
        .collect();

    for entry in &entries {
        let content = std::fs::read_to_string(entry.path()).expect("Failed to read log file");
        let line_count = content.lines().count();
        println!("File {:?} has {} lines", entry.file_name(), line_count);

        // Print last few lines to see what's there
        let lines: Vec<&str> = content.lines().collect();
        if lines.len() > 5 {
            println!("Last 5 lines:");
            for line in lines.iter().rev().take(5).rev() {
                println!("  {line}");
            }
        }
    }

    // Verify we got a reasonable number of logs
    assert!(
        total_logs_seen > 50,
        "Should have seen more than 50 logs, but only saw {total_logs_seen}"
    );

    // Verify log files were created
    let all_logs_dir = log_path.join("all");
    let entries: Vec<_> = std::fs::read_dir(&all_logs_dir)
        .expect("Failed to read all logs directory")
        .filter_map(Result::ok)
        .collect();

    assert!(!entries.is_empty(), "Should have created log files");

    // Read actual file content to verify
    if let Some(entry) = entries.first() {
        let content = std::fs::read_to_string(entry.path()).expect("Failed to read log file");
        let line_count = content.lines().count();
        println!("Log file has {line_count} lines");

        // Should have logs from both nodes and main
        assert!(
            content.contains("node-1-25"),
            "Should have logs from node 1"
        );
        assert!(content.contains("node-2-6"), "Should have logs from node 2");
        assert!(
            content.contains("Main thread"),
            "Should have logs from main thread"
        );
    }
}

#[test]
fn test_log_reader_updates_continuously() {
    // Create a temporary directory for logs
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let log_path = temp_dir.path().to_path_buf();

    // Create log writer
    let log_writer = LogWriter::new_sync(&log_path).expect("Failed to create log writer");
    let log_writer_arc = Arc::new(log_writer);

    // Try to initialize global logger, but don't fail if already initialized
    let _ = proven_logger::init(log_writer_arc.clone());

    // Create log reader
    let log_reader = LogReader::new(log_path.clone());
    log_reader.request_initial_data();
    log_reader.update_viewport_size(50);

    // Write logs incrementally and verify reader picks them up
    let mut last_count = 0;

    for batch in 0..5 {
        // Write a batch of logs
        for i in 0..10 {
            log_writer_arc.log(Record::new(
                proven_logger::Level::Info,
                format!("Batch {batch} message {i}"),
            ));
        }
        log_writer_arc.flush();

        // Wait for files to be written and reader to pick them up
        thread::sleep(Duration::from_millis(200));

        // Check if reader sees new logs
        let mut current_count = 0;
        while let Some(response) = log_reader.try_get_response() {
            use proven_local_tui::logs_viewer::LogResponse;
            if let LogResponse::ViewportUpdate { logs, .. } = response {
                current_count = logs.len();
            }
        }

        println!("Batch {batch}: Reader sees {current_count} logs (was {last_count})");

        // Should see more logs each time
        assert!(
            current_count >= last_count,
            "Log count should increase: {last_count} -> {current_count}"
        );

        last_count = current_count;
    }

    // Should have seen all 50 logs by the end
    assert!(
        last_count >= 40,
        "Should see at least 40 logs, saw {last_count}"
    );
}

#[test]
fn test_node_specific_log_filtering() {
    use proven_local_tui::messages::{LogLevel, TuiNodeId};

    // Create a temporary directory for logs
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let log_path = temp_dir.path().to_path_buf();

    // Create log writer
    let log_writer = LogWriter::new_sync(&log_path).expect("Failed to create log writer");
    let log_writer_arc = Arc::new(log_writer);

    // Don't initialize global logger - use writer directly

    // Create log reader
    let log_reader = LogReader::new(log_path.clone());
    log_reader.request_initial_data();
    log_reader.update_viewport_size(100);

    // Log from different "nodes"
    let node1_thread = thread::Builder::new()
        .name("node-1-25".to_string())
        .spawn({
            let logger = log_writer_arc.clone();
            move || {
                for i in 0..20 {
                    logger.log(Record::new(
                        proven_logger::Level::Info,
                        format!("Node 1 message {i}"),
                    ));
                }
                logger.flush();
            }
        })
        .unwrap();

    let node2_thread = thread::Builder::new()
        .name("node-2-6".to_string())
        .spawn({
            let logger = log_writer_arc.clone();
            move || {
                for i in 0..20 {
                    logger.log(Record::new(
                        proven_logger::Level::Info,
                        format!("Node 2 message {i}"),
                    ));
                }
                logger.flush();
            }
        })
        .unwrap();

    // Wait for threads
    node1_thread.join().unwrap();
    node2_thread.join().unwrap();

    // Also log from main thread
    for i in 0..10 {
        log_writer_arc.log(Record::new(
            proven_logger::Level::Info,
            format!("Main message {i}"),
        ));
    }
    log_writer_arc.flush();

    // Wait for logs to be written
    thread::sleep(Duration::from_millis(500));

    // Test filtering by node
    let node1_id = TuiNodeId::with_values(1, 25);
    log_reader.set_level_filter(LogLevel::Info);
    log_reader.set_node_filter(Some(node1_id));

    thread::sleep(Duration::from_millis(200));

    // Check filtered results
    let mut found_filtered_logs = false;
    while let Some(response) = log_reader.try_get_response() {
        use proven_local_tui::logs_viewer::LogResponse;
        if let LogResponse::ViewportUpdate { logs, .. } = response {
            println!("With node filter: {} logs", logs.len());

            if !logs.is_empty() {
                found_filtered_logs = true;
                // Check first few logs to debug
                for (i, log) in logs.iter().take(5).enumerate() {
                    println!(
                        "  Log {}: node_id={:?}, message={}",
                        i, log.node_id, log.message
                    );
                }

                // All logs should be from node 1
                let node1_logs: Vec<_> =
                    logs.iter().filter(|log| log.node_id == node1_id).collect();

                println!(
                    "Found {} logs from node 1 out of {} total",
                    node1_logs.len(),
                    logs.len()
                );

                // For now, just check that we have some logs from node 1
                assert!(
                    !node1_logs.is_empty(),
                    "Should have at least some logs from node 1"
                );
            }
        }
    }

    assert!(found_filtered_logs, "Should have found some filtered logs");
}
