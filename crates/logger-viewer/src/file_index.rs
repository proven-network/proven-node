//! File indexing for efficient line navigation

use crate::error::{Error, Result};
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::PathBuf;

/// Information about a single line in a file
#[derive(Debug, Clone)]
pub struct LineInfo {
    /// Byte offset where the line starts
    pub offset: u64,
    /// Length of the line in bytes (including newline)
    pub length: u64,
}

/// Index for a single file with line offsets
#[derive(Debug)]
pub struct FileIndex {
    /// Path to the file
    pub path: PathBuf,
    /// Line information
    pub lines: Vec<LineInfo>,
    /// Total size of the file
    pub file_size: u64,
    /// Last modification time
    pub last_modified: std::time::SystemTime,
}

impl FileIndex {
    /// Build an index for a file
    pub fn build(path: PathBuf) -> Result<Self> {
        let file = File::open(&path)?;
        let metadata = file.metadata()?;
        let file_size = metadata.len();
        let last_modified = metadata.modified()?;

        let mut reader = BufReader::new(file);
        let mut lines = Vec::new();
        let mut offset = 0u64;
        let mut buffer = Vec::new();

        loop {
            buffer.clear();
            let bytes_read = reader.read_until(b'\n', &mut buffer)?;
            if bytes_read == 0 {
                break; // EOF
            }

            lines.push(LineInfo {
                offset,
                length: bytes_read as u64,
            });

            offset += bytes_read as u64;
        }

        Ok(FileIndex {
            path,
            lines,
            file_size,
            last_modified,
        })
    }

    /// Check if the file has been modified since indexing
    pub fn is_stale(&self) -> Result<bool> {
        let metadata = std::fs::metadata(&self.path)?;
        Ok(metadata.modified()? > self.last_modified || metadata.len() != self.file_size)
    }

    /// Read a specific line from the file
    pub fn read_line(&self, line_index: usize) -> Result<Option<String>> {
        if line_index >= self.lines.len() {
            return Ok(None);
        }

        let line_info = &self.lines[line_index];
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(line_info.offset))?;

        let mut buffer = vec![0u8; line_info.length as usize];
        use std::io::Read;
        file.read_exact(&mut buffer)?;

        // Remove trailing newline if present
        if buffer.last() == Some(&b'\n') {
            buffer.pop();
            if buffer.last() == Some(&b'\r') {
                buffer.pop();
            }
        }

        String::from_utf8(buffer)
            .map(Some)
            .map_err(|e| Error::ParseError(format!("Invalid UTF-8: {e}")))
    }

    /// Read multiple lines efficiently
    pub fn read_lines(&self, start_index: usize, count: usize) -> Result<Vec<String>> {
        if start_index >= self.lines.len() {
            return Ok(Vec::new());
        }

        let end_index = (start_index + count).min(self.lines.len());
        let mut lines = Vec::with_capacity(end_index - start_index);

        let mut file = File::open(&self.path)?;
        let start_offset = self.lines[start_index].offset;
        file.seek(SeekFrom::Start(start_offset))?;

        let mut reader = BufReader::new(file);
        for _i in start_index..end_index {
            let mut line = String::new();
            reader.read_line(&mut line)?;

            // Remove trailing newline
            if line.ends_with('\n') {
                line.pop();
                if line.ends_with('\r') {
                    line.pop();
                }
            }

            lines.push(line);
        }

        Ok(lines)
    }

    /// Get total number of lines
    pub fn line_count(&self) -> usize {
        self.lines.len()
    }

    /// Find line at or before a given byte offset
    pub fn find_line_at_offset(&self, offset: u64) -> Option<usize> {
        self.lines.iter().rposition(|line| line.offset <= offset)
    }
}
