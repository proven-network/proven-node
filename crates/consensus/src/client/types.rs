//! Type traits and helpers for strongly-typed consensus client operations
//!
//! This module defines traits that user types must implement to be used
//! with the typed client API. Users have full control over serialization.

use bytes::Bytes;

/// Trait for types that can be used as messages in streams and pubsub
///
/// This trait gives users complete control over serialization format.
/// Types must be convertible to and from `Bytes`.
///
/// # Examples
///
/// Using bincode serialization:
/// ```ignore
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct MyMessage {
///     id: u64,
///     data: String,
/// }
///
/// impl TryFrom<Bytes> for MyMessage {
///     type Error = bincode::Error;
///     
///     fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
///         bincode::deserialize(&bytes)
///     }
/// }
///
/// impl TryInto<Bytes> for MyMessage {
///     type Error = bincode::Error;
///     
///     fn try_into(self) -> Result<Bytes, Self::Error> {
///         Ok(Bytes::from(bincode::serialize(&self)?))
///     }
/// }
/// ```
///
/// Using custom serialization:
/// ```ignore
/// struct CustomMessage {
///     version: u8,
///     payload: Vec<u8>,
/// }
///
/// impl TryFrom<Bytes> for CustomMessage {
///     type Error = std::io::Error;
///     
///     fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
///         if bytes.is_empty() {
///             return Err(std::io::Error::new(
///                 std::io::ErrorKind::InvalidData,
///                 "Empty message"
///             ));
///         }
///         
///         Ok(CustomMessage {
///             version: bytes[0],
///             payload: bytes[1..].to_vec(),
///         })
///     }
/// }
///
/// impl TryInto<Bytes> for CustomMessage {
///     type Error = std::io::Error;
///     
///     fn try_into(self) -> Result<Bytes, Self::Error> {
///         let mut bytes = Vec::with_capacity(1 + self.payload.len());
///         bytes.push(self.version);
///         bytes.extend_from_slice(&self.payload);
///         Ok(Bytes::from(bytes))
///     }
/// }
/// ```
pub trait MessageType: Send + Sync + 'static
where
    Self: TryInto<Bytes, Error = Self::SerializeError>,
    Self: TryFrom<Bytes, Error = Self::DeserializeError>,
{
    /// Error type for serialization
    type SerializeError: std::error::Error + Send + Sync + 'static;

    /// Error type for deserialization  
    type DeserializeError: std::error::Error + Send + Sync + 'static;
}

// Blanket implementation for all types that meet the requirements
impl<T> MessageType for T
where
    T: Send + Sync + 'static,
    T: TryInto<Bytes>,
    T: TryFrom<Bytes>,
    <T as TryInto<Bytes>>::Error: std::error::Error + Send + Sync + 'static,
    <T as TryFrom<Bytes>>::Error: std::error::Error + Send + Sync + 'static,
{
    type SerializeError = <T as TryInto<Bytes>>::Error;
    type DeserializeError = <T as TryFrom<Bytes>>::Error;
}

// The serde helpers are commented out for now since the features aren't in Cargo.toml
// These can be uncommented when serde features are added

// /// Helper trait for types that use serde for serialization
// ///
// /// This is provided as a convenience but is not required.
// /// Users can implement a similar helper for their preferred serialization.
// #[cfg(feature = "serde")]
// pub trait SerdeMessage:
//     serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static
// {
//     /// The serialization format to use
//     type Format: SerdeFormat;
// }

// /// Serialization formats for serde-based messages
// #[cfg(feature = "serde")]
// pub trait SerdeFormat {
//     /// Serialize a value to bytes
//     fn serialize<T: serde::Serialize>(
//         value: &T,
//     ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>;

//     /// Deserialize bytes to a value
//     fn deserialize<T: for<'de> serde::Deserialize<'de>>(
//         bytes: &[u8],
//     ) -> Result<T, Box<dyn std::error::Error + Send + Sync>>;
// }

// #[cfg(feature = "serde")]
// /// Bincode format implementation
// pub struct BincodeFormat;

// #[cfg(all(feature = "serde", feature = "bincode"))]
// impl SerdeFormat for BincodeFormat {
//     fn serialize<T: serde::Serialize>(
//         value: &T,
//     ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
//         bincode::serialize(value)
//             .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
//     }

//     fn deserialize<T: for<'de> serde::Deserialize<'de>>(
//         bytes: &[u8],
//     ) -> Result<T, Box<dyn std::error::Error + Send + Sync>> {
//         bincode::deserialize(bytes)
//             .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
//     }
// }

// #[cfg(feature = "serde")]
// /// JSON format implementation
// pub struct JsonFormat;

// #[cfg(all(feature = "serde", feature = "json"))]
// impl SerdeFormat for JsonFormat {
//     fn serialize<T: serde::Serialize>(
//         value: &T,
//     ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
//         serde_json::to_vec(value)
//             .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
//     }

//     fn deserialize<T: for<'de> serde::Deserialize<'de>>(
//         bytes: &[u8],
//     ) -> Result<T, Box<dyn std::error::Error + Send + Sync>> {
//         serde_json::from_slice(bytes)
//             .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
//     }
// }

// /// Example implementation helper for serde types
// #[cfg(feature = "serde")]
// pub mod serde_impl {
//     use super::*;
//     use std::marker::PhantomData;

//     /// Wrapper to implement MessageType for serde types
//     pub struct SerdeWrapper<T, F> {
//         value: T,
//         _format: PhantomData<F>,
//     }

//     impl<T, F> SerdeWrapper<T, F> {
//         pub fn new(value: T) -> Self {
//             Self {
//                 value,
//                 _format: PhantomData,
//             }
//         }

//         pub fn into_inner(self) -> T {
//             self.value
//         }
//     }

//     impl<T, F> TryFrom<Bytes> for SerdeWrapper<T, F>
//     where
//         T: for<'de> serde::Deserialize<'de>,
//         F: SerdeFormat,
//     {
//         type Error = Box<dyn std::error::Error + Send + Sync>;

//         fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
//             Ok(Self::new(F::deserialize(&bytes)?))
//         }
//     }

//     impl<T, F> TryInto<Bytes> for SerdeWrapper<T, F>
//     where
//         T: serde::Serialize,
//         F: SerdeFormat,
//     {
//         type Error = Box<dyn std::error::Error + Send + Sync>;

//         fn try_into(self) -> Result<Bytes, Self::Error> {
//             Ok(Bytes::from(F::serialize(&self.value)?))
//         }
//     }
// }
