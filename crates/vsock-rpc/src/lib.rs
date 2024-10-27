#[cfg(target_os = "linux")]
mod client;
#[cfg(not(target_os = "linux"))]
mod client_mock;
mod common;
mod error;

#[cfg(target_os = "linux")]
mod server;
#[cfg(not(target_os = "linux"))]
mod server_mock;

#[cfg(target_os = "linux")]
pub use client::RpcClient;
#[cfg(not(target_os = "linux"))]
pub use client_mock::RpcClient;

pub use common::*;
pub use error::{Error, Result};

#[cfg(target_os = "linux")]
pub use server::{RpcCall, RpcServer};
#[cfg(not(target_os = "linux"))]
pub use server_mock::{RpcCall, RpcServer};
