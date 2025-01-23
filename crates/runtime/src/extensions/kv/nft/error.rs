use deno_error::JsError;
use thiserror::Error;

#[derive(Clone, Debug, Error, JsError)]
pub enum Error {
    #[class(generic)]
    #[error("store error: {0}")]
    Store(String),

    #[class(generic)]
    #[error("verifier error: {0}")]
    Verification(String),
}
