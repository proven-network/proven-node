//! Memory-mapped log file reader for high performance

use crate::error::{Error, Result};
use crate::file_index::FileIndex;
use memmap2::{Mmap, MmapOptions};
use parking_lot::RwLock;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A memory-mapped log file reader
pub struct MmapLogReader {
    /// Path to the file
    path: PathBuf,
    /// Memory-mapped file
    mmap: Arc<Mmap>,
    /// File index
    index: Arc<RwLock<FileIndex>>,
}

impl MmapLogReader {
    /// Create a new memory-mapped log reader
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;

        // Memory map the file
        let mmap = unsafe {
            MmapOptions::new()
                .map(&file)
                .map_err(|e| Error::MemoryMap {
                    path: path.clone(),
                    source: e,
                })?
        };

        // Build the index
        let index = FileIndex::build(path.clone())?;

        Ok(Self {
            path,
            mmap: Arc::new(mmap),
            index: Arc::new(RwLock::new(index)),
        })
    }

    /// Refresh the memory map and index if the file has changed
    pub fn refresh(&mut self) -> Result<bool> {
        let needs_refresh = self.index.read().is_stale()?;

        if needs_refresh {
            // Re-open and remap the file
            let file = File::open(&self.path)?;
            let new_mmap = unsafe {
                MmapOptions::new()
                    .map(&file)
                    .map_err(|e| Error::MemoryMap {
                        path: self.path.clone(),
                        source: e,
                    })?
            };

            // Rebuild the index
            let new_index = FileIndex::build(self.path.clone())?;

            // Update atomically
            self.mmap = Arc::new(new_mmap);
            *self.index.write() = new_index;
        }

        Ok(needs_refresh)
    }

    /// Get the total number of lines
    pub fn line_count(&self) -> usize {
        self.index.read().line_count()
    }

    /// Read a line using memory mapping (zero-copy when possible)
    pub fn read_line(&self, line_index: usize) -> Result<Option<String>> {
        let index = self.index.read();

        if line_index >= index.lines.len() {
            return Ok(None);
        }

        let line_info = &index.lines[line_index];
        let start = line_info.offset as usize;
        let end = (line_info.offset + line_info.length) as usize;

        // Bounds check
        if end > self.mmap.len() {
            return Err(Error::InvalidLineIndex {
                index: line_index,
                total: index.lines.len(),
            });
        }

        // Get the line data from memory map
        let line_data = &self.mmap[start..end];

        // Remove trailing newline
        let line_data = if line_data.ends_with(b"\n") {
            &line_data[..line_data.len() - 1]
        } else {
            line_data
        };

        let line_data = if line_data.ends_with(b"\r") {
            &line_data[..line_data.len() - 1]
        } else {
            line_data
        };

        // Convert to string
        std::str::from_utf8(line_data)
            .map(|s| Some(s.to_string()))
            .map_err(|e| Error::ParseError(format!("Invalid UTF-8: {e}")))
    }

    /// Read multiple lines efficiently
    pub fn read_lines(&self, start_index: usize, count: usize) -> Result<Vec<String>> {
        let index = self.index.read();

        if start_index >= index.lines.len() {
            return Ok(Vec::new());
        }

        let end_index = (start_index + count).min(index.lines.len());
        let mut lines = Vec::with_capacity(end_index - start_index);

        for i in start_index..end_index {
            let line_info = &index.lines[i];
            let start = line_info.offset as usize;
            let end = (line_info.offset + line_info.length) as usize;

            if end > self.mmap.len() {
                break;
            }

            let mut line_data = &self.mmap[start..end];

            // Remove trailing newline
            if line_data.ends_with(b"\n") {
                line_data = &line_data[..line_data.len() - 1];
            }
            if line_data.ends_with(b"\r") {
                line_data = &line_data[..line_data.len() - 1];
            }

            match std::str::from_utf8(line_data) {
                Ok(s) => lines.push(s.to_string()),
                Err(e) => {
                    return Err(Error::ParseError(format!("Invalid UTF-8 at line {i}: {e}")));
                }
            }
        }

        Ok(lines)
    }

    /// Get a slice of the raw data for a line (zero-copy)
    pub fn get_line_bytes(&self, line_index: usize) -> Result<Option<&[u8]>> {
        let index = self.index.read();

        if line_index >= index.lines.len() {
            return Ok(None);
        }

        let line_info = &index.lines[line_index];
        let start = line_info.offset as usize;
        let end = (line_info.offset + line_info.length) as usize;

        if end > self.mmap.len() {
            return Err(Error::InvalidLineIndex {
                index: line_index,
                total: index.lines.len(),
            });
        }

        let mut line_data = &self.mmap[start..end];

        // Remove trailing newline
        if line_data.ends_with(b"\n") {
            line_data = &line_data[..line_data.len() - 1];
        }
        if line_data.ends_with(b"\r") {
            line_data = &line_data[..line_data.len() - 1];
        }

        Ok(Some(line_data))
    }

    /// Search for lines containing a pattern (uses memory-mapped data)
    pub fn search(
        &self,
        pattern: &str,
        start_line: usize,
        max_results: usize,
    ) -> Result<Vec<(usize, String)>> {
        let index = self.index.read();
        let mut results = Vec::new();

        for i in start_line..index.lines.len() {
            if results.len() >= max_results {
                break;
            }

            if let Some(line_bytes) = self.get_line_bytes(i)?
                && let Ok(line_str) = std::str::from_utf8(line_bytes)
                && line_str.contains(pattern)
            {
                results.push((i, line_str.to_string()));
            }
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_mmap_reader() -> Result<()> {
        // Create a test file
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "Line 1")?;
        writeln!(file, "Line 2")?;
        writeln!(file, "Line 3")?;
        file.flush()?;

        // Create reader
        let reader = MmapLogReader::new(file.path())?;

        // Test line count
        assert_eq!(reader.line_count(), 3);

        // Test reading individual lines
        assert_eq!(reader.read_line(0)?.unwrap(), "Line 1");
        assert_eq!(reader.read_line(1)?.unwrap(), "Line 2");
        assert_eq!(reader.read_line(2)?.unwrap(), "Line 3");
        assert!(reader.read_line(3)?.is_none());

        // Test reading multiple lines
        let lines = reader.read_lines(0, 2)?;
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], "Line 1");
        assert_eq!(lines[1], "Line 2");

        // Test search
        let results = reader.search("Line 2", 0, 10)?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);
        assert_eq!(results[0].1, "Line 2");

        Ok(())
    }

    #[test]
    fn test_refresh() -> Result<()> {
        // Create a test file
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "Initial line")?;
        file.flush()?;

        // Create reader
        let mut reader = MmapLogReader::new(file.path())?;
        assert_eq!(reader.line_count(), 1);

        // Append more data
        writeln!(file, "New line")?;
        file.flush()?;

        // Refresh should detect changes
        assert!(reader.refresh()?);
        assert_eq!(reader.line_count(), 2);
        assert_eq!(reader.read_line(1)?.unwrap(), "New line");

        Ok(())
    }
}
