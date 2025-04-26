use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use bytes::Bytes;
use headers::{Error as HeaderError, Header, HeaderName, HeaderValue};

// Define the HeaderName for X-Nonce
static X_NONCE: HeaderName = HeaderName::from_static("x-nonce"); // Using lowercase as per HTTP convention

#[derive(Clone, Debug)]
pub struct Nonce(pub Bytes);

impl Header for Nonce {
    fn name() -> &'static HeaderName {
        &X_NONCE
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, HeaderError>
    where
        Self: Sized,
        I: Iterator<Item = &'i HeaderValue>,
    {
        let value = values.next().ok_or_else(HeaderError::invalid)?;
        let bytes = STANDARD_NO_PAD
            .decode(value.as_bytes())
            .map_err(|_| HeaderError::invalid())?; // Return HeaderError on decode failure
        Ok(Nonce(Bytes::from(bytes)))
    }

    fn encode<E: Extend<HeaderValue>>(&self, values: &mut E) {
        let base64_encoded = STANDARD_NO_PAD.encode(&self.0);
        // HeaderValue::from_str can fail if the string is not valid ASCII,
        // but base64 output is always valid ASCII. Using unwrap is likely safe,
        // but prefer expect for clarity or handle the Result properly in production.
        let header_value = HeaderValue::from_str(&base64_encoded)
            .expect("Base64 encoded string should form a valid HeaderValue");
        values.extend(std::iter::once(header_value));
    }
}
