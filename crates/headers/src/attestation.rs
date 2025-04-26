use bytes::Bytes;
use headers::{Error as HeaderError, Header, HeaderName, HeaderValue};

// Define the HeaderName for X-Attestation
static X_ATTESTATION: HeaderName = HeaderName::from_static("x-attestation");

/// Represents the raw bytes of an `X-Attestation` header.
/// Assumes the header value is Base64 encoded.
#[derive(Clone, Debug)]
pub struct Attestation(pub Bytes);

impl Header for Attestation {
    fn name() -> &'static HeaderName {
        &X_ATTESTATION
    }

    // Decode from Base64
    fn decode<'i, I>(values: &mut I) -> Result<Self, HeaderError>
    where
        Self: Sized,
        I: Iterator<Item = &'i HeaderValue>,
    {
        use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};

        let value = values.next().ok_or_else(HeaderError::invalid)?;
        let bytes = STANDARD_NO_PAD
            .decode(value.as_bytes())
            .map_err(|_| HeaderError::invalid())?;
        Ok(Attestation(Bytes::from(bytes)))
    }

    // Encode to Base64
    fn encode<E: Extend<HeaderValue>>(&self, values: &mut E) {
        use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};

        let base64_encoded = STANDARD_NO_PAD.encode(&self.0);
        // HeaderValue::from_str can fail if the string is not valid ASCII,
        // but base64 output is always valid ASCII.
        let header_value = HeaderValue::from_str(&base64_encoded)
            .expect("Base64 encoded string should form a valid HeaderValue");
        values.extend(std::iter::once(header_value));
    }
}
