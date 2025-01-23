use deno_error::JsError;
use thiserror::Error;

#[derive(Clone, Debug, Error, JsError)]
pub enum Error {
    #[class(generic)]
    #[error("db error: {0}")]
    SqlStore(String),

    #[class(generic)]
    #[error("verifier error: {0}")]
    Verification(String),
}
