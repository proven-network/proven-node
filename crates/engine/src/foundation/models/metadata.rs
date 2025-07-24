//! Metadata structures

/// Group metadata
#[derive(Debug, Clone)]
pub struct GroupMetadata {
    /// Group creation time
    pub created_at: u64,
    /// Number of streams
    pub stream_count: usize,
    /// Total messages across all streams
    pub total_messages: u64,
    /// Total storage used
    pub total_bytes: u64,
}
