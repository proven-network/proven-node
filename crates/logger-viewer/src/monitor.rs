//! File monitoring for real-time log updates

use crate::error::{Error, Result};
use crossbeam_channel::{Receiver, bounded};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use parking_lot::RwLock;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// File system events
#[derive(Debug, Clone)]
pub enum FileEvent {
    /// File was created
    Created(PathBuf),
    /// File was modified
    Modified(PathBuf),
    /// File was deleted
    Deleted(PathBuf),
    /// File was renamed
    Renamed { from: PathBuf, to: PathBuf },
}

/// Log file monitor that watches for changes
pub struct LogMonitor {
    /// Paths being watched
    watched_paths: Arc<RwLock<Vec<PathBuf>>>,
    /// Event receiver
    event_receiver: Receiver<FileEvent>,
    /// Watcher handle
    _watcher: RecommendedWatcher,
    /// Worker thread handle
    _worker_thread: Option<thread::JoinHandle<()>>,
}

impl LogMonitor {
    /// Create a new log monitor
    pub fn new(paths: Vec<PathBuf>) -> Result<Self> {
        let (tx, rx) = bounded(1000);
        let watched_paths = Arc::new(RwLock::new(paths.clone()));

        // Create file watcher
        let mut watcher = notify::recommended_watcher(move |res: notify::Result<Event>| {
            if let Ok(event) = res {
                let file_event = match event.kind {
                    EventKind::Create(_) => {
                        event.paths.first().map(|p| FileEvent::Created(p.clone()))
                    }
                    EventKind::Modify(_) => {
                        event.paths.first().map(|p| FileEvent::Modified(p.clone()))
                    }
                    EventKind::Remove(_) => {
                        event.paths.first().map(|p| FileEvent::Deleted(p.clone()))
                    }
                    EventKind::Other => None,
                    _ => None,
                };

                if let Some(file_event) = file_event {
                    let _ = tx.try_send(file_event);
                }
            }
        })
        .map_err(|e| Error::WatchError(e.to_string()))?;

        // Watch all paths
        for path in &paths {
            let mode = RecursiveMode::NonRecursive;

            watcher.watch(path, mode).map_err(|e| {
                Error::WatchError(format!("Failed to watch {}: {}", path.display(), e))
            })?;
        }

        Ok(Self {
            watched_paths,
            event_receiver: rx,
            _watcher: watcher,
            _worker_thread: None,
        })
    }

    /// Create a monitor that watches a directory for log files
    pub fn watch_directory(dir: impl AsRef<Path>, pattern: &str) -> Result<Self> {
        let dir = dir.as_ref();
        if !dir.is_dir() {
            return Err(Error::FileNotFound(dir.to_path_buf()));
        }

        let (event_tx, event_rx) = bounded(1000);
        let watched_paths = Arc::new(RwLock::new(vec![dir.to_path_buf()]));
        let pattern = pattern.to_string();
        let dir_path = dir.to_path_buf();

        // Create debouncer to avoid duplicate events
        let (debounce_tx, debounce_rx) = bounded::<FileEvent>(1000);

        // Spawn debouncing thread
        let worker_thread = thread::spawn(move || {
            let mut last_events = std::collections::HashMap::new();
            let debounce_duration = Duration::from_millis(50);

            loop {
                match debounce_rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(event) => {
                        let path = match &event {
                            FileEvent::Created(p)
                            | FileEvent::Modified(p)
                            | FileEvent::Deleted(p) => p.clone(),
                            FileEvent::Renamed { to, .. } => to.clone(),
                        };

                        let now = std::time::Instant::now();
                        if let Some(last_time) = last_events.get(&path)
                            && now.duration_since(*last_time) < debounce_duration
                        {
                            continue;
                        }

                        last_events.insert(path, now);
                        let _ = event_tx.try_send(event);
                    }
                    Err(_) => {
                        // Check if we should exit
                        // Check if sender is gone
                        thread::sleep(Duration::from_millis(10));
                    }
                }
            }
        });

        // Create file watcher
        let pattern_clone = pattern.clone();
        let mut watcher = notify::recommended_watcher(move |res: notify::Result<Event>| {
            if let Ok(event) = res {
                for path in &event.paths {
                    // Filter by pattern
                    if let Some(file_name) = path.file_name().and_then(|n| n.to_str())
                        && !glob::Pattern::new(&pattern_clone)
                            .map(|p| p.matches(file_name))
                            .unwrap_or(false)
                    {
                        continue;
                    }

                    let file_event = match event.kind {
                        EventKind::Create(_) => Some(FileEvent::Created(path.clone())),
                        EventKind::Modify(_) => Some(FileEvent::Modified(path.clone())),
                        EventKind::Remove(_) => Some(FileEvent::Deleted(path.clone())),
                        _ => None,
                    };

                    if let Some(file_event) = file_event {
                        let _ = debounce_tx.try_send(file_event);
                    }
                }
            }
        })
        .map_err(|e| Error::WatchError(e.to_string()))?;

        // Watch directory
        watcher
            .watch(&dir_path, RecursiveMode::NonRecursive)
            .map_err(|e| Error::WatchError(format!("Failed to watch directory: {e}")))?;

        Ok(Self {
            watched_paths,
            event_receiver: event_rx,
            _watcher: watcher,
            _worker_thread: Some(worker_thread),
        })
    }

    /// Try to receive a file event (non-blocking)
    pub fn try_recv(&self) -> Option<FileEvent> {
        self.event_receiver.try_recv().ok()
    }

    /// Receive a file event (blocking with timeout)
    pub fn recv_timeout(&self, timeout: Duration) -> Option<FileEvent> {
        self.event_receiver.recv_timeout(timeout).ok()
    }

    /// Add a path to watch
    pub fn watch(&mut self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();

        let mode = RecursiveMode::NonRecursive;

        self._watcher
            .watch(path, mode)
            .map_err(|e| Error::WatchError(format!("Failed to watch {}: {}", path.display(), e)))?;

        self.watched_paths.write().push(path.to_path_buf());
        Ok(())
    }

    /// Remove a path from watching
    pub fn unwatch(&mut self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();

        self._watcher.unwatch(path).map_err(|e| {
            Error::WatchError(format!("Failed to unwatch {}: {}", path.display(), e))
        })?;

        self.watched_paths.write().retain(|p| p != path);
        Ok(())
    }

    /// Get list of watched paths
    pub fn watched_paths(&self) -> Vec<PathBuf> {
        self.watched_paths.read().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_file_monitor() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let log_file = temp_dir.path().join("test.log");

        // Create initial file
        fs::write(&log_file, "initial content\n")?;

        // Create monitor
        let monitor = LogMonitor::new(vec![log_file.clone()])?;

        // Modify file
        fs::write(&log_file, "modified content\n")?;

        // Wait for event
        let event = monitor.recv_timeout(Duration::from_secs(1));
        assert!(event.is_some());

        match event.unwrap() {
            FileEvent::Modified(path) => assert_eq!(path, log_file),
            _ => panic!("Expected Modified event"),
        }

        Ok(())
    }

    #[test]
    fn test_directory_monitor() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();

        // Create monitor for *.log files
        let monitor = LogMonitor::watch_directory(temp_dir.path(), "*.log")?;

        // Create a log file
        let log_file = temp_dir.path().join("test.log");
        fs::write(&log_file, "new log\n")?;

        // Wait for event
        let event = monitor.recv_timeout(Duration::from_secs(1));
        assert!(event.is_some());

        match event.unwrap() {
            FileEvent::Created(path) => assert_eq!(path.file_name().unwrap(), "test.log"),
            _ => panic!("Expected Created event"),
        }

        // Create non-log file (should be ignored)
        let other_file = temp_dir.path().join("test.txt");
        fs::write(&other_file, "not a log\n")?;

        // Should not receive event for non-log file
        let event = monitor.recv_timeout(Duration::from_millis(100));
        assert!(event.is_none());

        Ok(())
    }
}
