//! Multi-file log reader that handles multiple log files in chronological order

use crate::LogReader;
use crate::config::ViewerConfig;
use crate::error::{Error, Result};
use crate::mmap_reader::MmapLogReader;
use chrono::{DateTime, Utc};
use glob::glob;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

/// Information about a log file
#[derive(Debug, Clone)]
pub struct LogFileInfo {
    pub path: PathBuf,
    pub timestamp: DateTime<Utc>,
    pub size: u64,
    pub line_count: usize,
    pub first_line_index: usize, // Global line index where this file starts
}

/// Multi-file reader that seamlessly handles multiple log files
pub struct MultiFileReader {
    config: ViewerConfig,
    /// Map from file path to reader
    readers: Arc<RwLock<BTreeMap<PathBuf, Arc<MmapLogReader>>>>,
    /// Ordered list of files
    file_info: Arc<RwLock<Vec<LogFileInfo>>>,
    /// Total line count cache
    total_lines: Arc<RwLock<usize>>,
    /// Last discovery time
    last_discovery: Arc<RwLock<Instant>>,
}

impl MultiFileReader {
    /// Create a new multi-file reader
    pub fn new(config: ViewerConfig) -> Result<Self> {
        let reader = Self {
            config,
            readers: Arc::new(RwLock::new(BTreeMap::new())),
            file_info: Arc::new(RwLock::new(Vec::new())),
            total_lines: Arc::new(RwLock::new(0)),
            last_discovery: Arc::new(RwLock::new(Instant::now())),
        };

        reader.discover_files()?;
        Ok(reader)
    }

    /// Parse timestamp from filename (e.g., "2024-01-15_14-30-45.log")
    fn parse_timestamp_from_filename(filename: &str) -> Option<DateTime<Utc>> {
        // Try common timestamp formats
        let name = filename.strip_suffix(".log")?;

        // Format: YYYY-MM-DD_HH-MM-SS
        if let Ok(dt) = DateTime::parse_from_str(&format!("{name} +0000"), "%Y-%m-%d_%H-%M-%S %z") {
            return Some(dt.with_timezone(&Utc));
        }

        // Format: YYYY-MM-DD-HH-MM-SS
        if let Ok(dt) = DateTime::parse_from_str(&format!("{name} +0000"), "%Y-%m-%d-%H-%M-%S %z") {
            return Some(dt.with_timezone(&Utc));
        }

        // Fall back to file modification time
        None
    }

    /// Discover log files in the directory
    pub fn discover_files(&self) -> Result<()> {
        let pattern = self.config.log_dir.join(&self.config.file_pattern);
        let pattern_str = pattern
            .to_str()
            .ok_or_else(|| Error::InvalidPattern("Invalid path encoding".to_string()))?;

        let mut new_files = Vec::new();

        for entry in glob(pattern_str).map_err(|e| Error::InvalidPattern(e.to_string()))? {
            let path = entry?;

            if !path.is_file() {
                continue;
            }

            // Skip symlinks if not following them
            if !self.config.follow_symlinks && path.is_symlink() {
                continue;
            }

            let metadata = std::fs::metadata(&path)?;
            let timestamp = Self::parse_timestamp_from_filename(
                path.file_name().and_then(|n| n.to_str()).unwrap_or(""),
            )
            .unwrap_or_else(|| {
                // Fall back to modification time
                metadata
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| {
                        DateTime::from_timestamp(d.as_secs() as i64, d.subsec_nanos()).unwrap()
                    })
                    .unwrap_or_else(Utc::now)
            });

            new_files.push((path, timestamp, metadata.len()));
        }

        // Sort by timestamp
        new_files.sort_by(|a, b| a.1.cmp(&b.1));

        // Update readers and file info
        let mut readers = self.readers.write();
        let mut file_info = self.file_info.write();
        let mut total_lines = 0;

        // Keep existing readers that are still valid
        let mut updated_info = Vec::new();

        for (path, timestamp, size) in new_files {
            let reader = match MmapLogReader::new(&path) {
                Ok(reader) => Arc::new(reader),
                Err(_) => continue,
            };

            // Always update the readers map with the new reader
            readers.insert(path.clone(), reader.clone());

            let line_count = reader.line_count();
            updated_info.push(LogFileInfo {
                path,
                timestamp,
                size,
                line_count,
                first_line_index: total_lines,
            });

            total_lines += line_count;
        }

        // Remove readers for files that no longer exist
        readers.retain(|path, _| updated_info.iter().any(|info| &info.path == path));

        *file_info = updated_info;
        *self.total_lines.write() = total_lines;
        *self.last_discovery.write() = Instant::now();

        Ok(())
    }

    /// Check if we should rediscover files
    fn should_rediscover(&self) -> bool {
        self.last_discovery.read().elapsed() >= self.config.discovery_interval
    }

    /// Ensure files are discovered
    fn ensure_discovered(&self) -> Result<()> {
        if self.should_rediscover() {
            self.discover_files()?;
        }
        Ok(())
    }

    /// Find which file contains a given global line index
    #[allow(dead_code)]
    fn find_file_for_line(&self, global_line_index: usize) -> Option<(usize, LogFileInfo)> {
        let file_info = self.file_info.read();

        // Binary search for the file containing this line
        match file_info.binary_search_by(|info| {
            if global_line_index < info.first_line_index {
                std::cmp::Ordering::Greater
            } else if global_line_index >= info.first_line_index + info.line_count {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        }) {
            Ok(idx) => Some((idx, file_info[idx].clone())),
            Err(_) => None,
        }
    }
}

impl LogReader for MultiFileReader {
    fn total_lines(&self) -> Result<usize> {
        self.ensure_discovered()?;
        Ok(*self.total_lines.read())
    }

    fn read_line(&self, line_index: usize) -> Result<Option<String>> {
        self.ensure_discovered()?;

        let file_info = self.file_info.read();

        // Find which file contains this line
        let (_file_idx, info) = match file_info.iter().enumerate().find(|(_, info)| {
            line_index >= info.first_line_index
                && line_index < info.first_line_index + info.line_count
        }) {
            Some((idx, info)) => (idx, info),
            None => return Ok(None),
        };

        // Calculate local line index within the file
        let local_line_index = line_index - info.first_line_index;

        // Get the reader and read the line
        let readers = self.readers.read();
        if let Some(reader) = readers.get(&info.path) {
            reader.read_line(local_line_index)
        } else {
            Ok(None)
        }
    }

    fn read_lines(&self, start: usize, count: usize) -> Result<Vec<String>> {
        self.ensure_discovered()?;

        let mut lines = Vec::with_capacity(count);
        let mut current_index = start;
        let mut remaining = count;

        while remaining > 0 && current_index < self.total_lines()? {
            if let Some(line) = self.read_line(current_index)? {
                lines.push(line);
                current_index += 1;
                remaining -= 1;
            } else {
                break;
            }
        }

        Ok(lines)
    }

    fn get_file_for_line(&self, line_index: usize) -> Result<Option<PathBuf>> {
        self.ensure_discovered()?;

        let file_info = self.file_info.read();

        // Find which file contains this line
        match file_info.iter().find(|info| {
            line_index >= info.first_line_index
                && line_index < info.first_line_index + info.line_count
        }) {
            Some(info) => Ok(Some(info.path.clone())),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn test_multi_file_reader() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path();

        // Create multiple log files
        let mut file1 = std::fs::File::create(log_dir.join("2024-01-01_00-00-00.log"))?;
        writeln!(file1, "File 1 Line 1")?;
        writeln!(file1, "File 1 Line 2")?;
        file1.flush()?;

        let mut file2 = std::fs::File::create(log_dir.join("2024-01-01_00-00-01.log"))?;
        writeln!(file2, "File 2 Line 1")?;
        writeln!(file2, "File 2 Line 2")?;
        writeln!(file2, "File 2 Line 3")?;
        file2.flush()?;

        // Create reader
        let config = crate::ViewerConfigBuilder::new()
            .log_dir(log_dir.to_path_buf())
            .build();

        let reader = MultiFileReader::new(config)?;

        // Test total lines
        assert_eq!(reader.total_lines()?, 5);

        // Test reading lines across files
        assert_eq!(reader.read_line(0)?.unwrap(), "File 1 Line 1");
        assert_eq!(reader.read_line(1)?.unwrap(), "File 1 Line 2");
        assert_eq!(reader.read_line(2)?.unwrap(), "File 2 Line 1");
        assert_eq!(reader.read_line(3)?.unwrap(), "File 2 Line 2");
        assert_eq!(reader.read_line(4)?.unwrap(), "File 2 Line 3");

        // Test reading multiple lines
        let lines = reader.read_lines(1, 3)?;
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], "File 1 Line 2");
        assert_eq!(lines[1], "File 2 Line 1");
        assert_eq!(lines[2], "File 2 Line 2");

        // Test file lookup
        assert!(
            reader
                .get_file_for_line(0)?
                .unwrap()
                .to_str()
                .unwrap()
                .contains("2024-01-01_00-00-00.log")
        );
        assert!(
            reader
                .get_file_for_line(3)?
                .unwrap()
                .to_str()
                .unwrap()
                .contains("2024-01-01_00-00-01.log")
        );

        Ok(())
    }
}
