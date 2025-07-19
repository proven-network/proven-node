//! Common validation helpers for the engine

use crate::error::{ConsensusResult, Error, ErrorKind};

/// Validate that a collection is not empty
pub fn not_empty<T>(items: &[T], item_type: &str) -> ConsensusResult<()> {
    if items.is_empty() {
        return Err(Error::with_context(
            ErrorKind::Validation,
            format!("{item_type} must not be empty"),
        ));
    }
    Ok(())
}

/// Validate that a value is greater than zero
pub fn greater_than_zero(value: u64, field_name: &str) -> ConsensusResult<()> {
    if value == 0 {
        return Err(Error::with_context(
            ErrorKind::Validation,
            format!("{field_name} must be greater than 0"),
        ));
    }
    Ok(())
}

/// Validate string length
pub fn string_length(s: &str, min: usize, max: usize, field_name: &str) -> ConsensusResult<()> {
    let len = s.len();
    if len < min || len > max {
        return Err(Error::with_context(
            ErrorKind::Validation,
            format!("{field_name} length must be between {min} and {max} characters"),
        ));
    }
    Ok(())
}
