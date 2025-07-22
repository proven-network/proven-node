//! Search functionality for log files

use crate::LogReader;
use crate::error::Result;
use parking_lot::RwLock;
use regex::Regex;
use std::sync::Arc;

/// Search options
#[derive(Debug, Clone)]
pub struct SearchOptions {
    /// Case sensitive search
    pub case_sensitive: bool,
    /// Use regex instead of plain text search
    pub use_regex: bool,
    /// Maximum number of results to return
    pub max_results: usize,
    /// Context lines before match
    pub before_context: usize,
    /// Context lines after match
    pub after_context: usize,
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            case_sensitive: false,
            use_regex: false,
            max_results: 1000,
            before_context: 0,
            after_context: 0,
        }
    }
}

/// Search result
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// Line number (0-based)
    pub line_number: usize,
    /// The matching line
    pub line: String,
    /// Context lines before the match
    pub before_context: Vec<String>,
    /// Context lines after the match
    pub after_context: Vec<String>,
    /// Match positions in the line (start, end)
    pub match_positions: Vec<(usize, usize)>,
}

/// Search engine for log files
pub struct SearchEngine {
    /// Compiled regex cache
    regex_cache: Arc<RwLock<Option<(String, Regex)>>>,
}

impl SearchEngine {
    /// Create a new search engine
    pub fn new() -> Self {
        Self {
            regex_cache: Arc::new(RwLock::new(None)),
        }
    }

    /// Search for a pattern in a log reader
    pub fn search(
        &self,
        reader: &dyn LogReader,
        pattern: &str,
        options: &SearchOptions,
        start_line: Option<usize>,
    ) -> Result<Vec<SearchResult>> {
        let mut results = Vec::new();
        let total_lines = reader.total_lines()?;
        let start = start_line.unwrap_or(0);

        // Type alias for search results
        type SearchFn = Box<dyn Fn(&str) -> Vec<(usize, usize)>>;

        // Prepare search pattern
        let searcher: SearchFn = if options.use_regex {
            let regex = self.get_or_compile_regex(pattern, options.case_sensitive)?;
            Box::new(move |line| {
                regex
                    .find_iter(line)
                    .map(|m| (m.start(), m.end()))
                    .collect()
            })
        } else {
            let case_sensitive = options.case_sensitive;
            let pattern = if case_sensitive {
                pattern.to_string()
            } else {
                pattern.to_lowercase()
            };
            Box::new(move |line| {
                let search_line = if case_sensitive {
                    line.to_string()
                } else {
                    line.to_lowercase()
                };

                let mut positions = Vec::new();
                let mut start = 0;
                while let Some(pos) = search_line[start..].find(&pattern) {
                    let abs_pos = start + pos;
                    positions.push((abs_pos, abs_pos + pattern.len()));
                    start = abs_pos + 1;
                }
                positions
            })
        };

        // Search through lines
        let mut i = start;
        while i < total_lines && results.len() < options.max_results {
            if let Some(line) = reader.read_line(i)? {
                let positions = searcher(&line);

                if !positions.is_empty() {
                    // Get context lines
                    let before_start = i.saturating_sub(options.before_context);
                    let before_context = if before_start < i {
                        reader.read_lines(before_start, i - before_start)?
                    } else {
                        Vec::new()
                    };

                    let after_start = i + 1;
                    let after_count = options
                        .after_context
                        .min(total_lines.saturating_sub(after_start));
                    let after_context = if after_count > 0 {
                        reader.read_lines(after_start, after_count)?
                    } else {
                        Vec::new()
                    };

                    results.push(SearchResult {
                        line_number: i,
                        line,
                        before_context,
                        after_context,
                        match_positions: positions,
                    });

                    // Skip context lines to avoid overlapping results
                    i += options.after_context;
                }
            }

            i += 1;
        }

        Ok(results)
    }

    /// Get or compile a regex pattern
    fn get_or_compile_regex(&self, pattern: &str, case_sensitive: bool) -> Result<Regex> {
        let mut cache = self.regex_cache.write();

        // Check if we have this pattern cached
        if let Some((cached_pattern, regex)) = &*cache
            && cached_pattern == pattern
        {
            return Ok(regex.clone());
        }

        // Compile new regex
        let mut regex_builder = regex::RegexBuilder::new(pattern);
        let regex = if case_sensitive {
            regex_builder.build()?
        } else {
            regex_builder.case_insensitive(true).build()?
        };

        // Cache it
        *cache = Some((pattern.to_string(), regex.clone()));

        Ok(regex)
    }
}

impl Default for SearchEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ViewerConfigBuilder;
    use crate::multi_reader::MultiFileReader;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn test_search_plain_text() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let log_file = temp_dir.path().join("test.log");

        let mut file = std::fs::File::create(&log_file)?;
        writeln!(file, "Line 1: Hello world")?;
        writeln!(file, "Line 2: Foo bar")?;
        writeln!(file, "Line 3: Hello again")?;
        writeln!(file, "Line 4: Another line")?;
        file.flush()?;

        let config = ViewerConfigBuilder::new()
            .log_dir(temp_dir.path().to_path_buf())
            .build();
        let reader = MultiFileReader::new(config)?;

        let engine = SearchEngine::new();
        let options = SearchOptions {
            case_sensitive: false,
            use_regex: false,
            max_results: 10,
            before_context: 1,
            after_context: 1,
        };

        let results = engine.search(&reader, "hello", &options, None)?;
        assert_eq!(results.len(), 2);

        // First match
        assert_eq!(results[0].line_number, 0);
        assert_eq!(results[0].line, "Line 1: Hello world");
        assert_eq!(results[0].before_context.len(), 0); // No line before
        assert_eq!(results[0].after_context.len(), 1);
        assert_eq!(results[0].after_context[0], "Line 2: Foo bar");
        assert_eq!(results[0].match_positions, vec![(8, 13)]);

        // Second match
        assert_eq!(results[1].line_number, 2);
        assert_eq!(results[1].line, "Line 3: Hello again");

        Ok(())
    }

    #[test]
    fn test_search_regex() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let log_file = temp_dir.path().join("test.log");

        let mut file = std::fs::File::create(&log_file)?;
        writeln!(file, "Error: Connection failed")?;
        writeln!(file, "Info: Connected successfully")?;
        writeln!(file, "Warning: Connection slow")?;
        file.flush()?;

        let config = ViewerConfigBuilder::new()
            .log_dir(temp_dir.path().to_path_buf())
            .build();
        let reader = MultiFileReader::new(config)?;

        let engine = SearchEngine::new();
        let options = SearchOptions {
            case_sensitive: true,
            use_regex: true,
            ..Default::default()
        };

        // Search for lines starting with Error or Warning
        let results = engine.search(&reader, r"^(Error|Warning):", &options, None)?;
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].line_number, 0);
        assert_eq!(results[1].line_number, 2);

        Ok(())
    }
}
