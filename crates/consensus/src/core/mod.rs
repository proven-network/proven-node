//! Core consensus functionality
//!
//! This module contains the core consensus implementation including:
//! - Main Consensus struct
//! - Unified consensus manager
//! - Global and local consensus layers
//! - Type-safe operations and requests

pub mod engine;
pub mod global;
pub mod group;
pub mod monitoring;
pub mod state_machine;
pub mod stream;
pub mod types;

// Re-export main types

// Re-export state machines from centralized location
