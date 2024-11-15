use deno_core::extension;

mod console;
mod kv;
mod run;
mod sessions;
mod sql;

pub use console::*;
pub use kv::*;
pub use run::*;
pub use sessions::*;
pub use sql::*;

extension!(
    radixdlt_babylon_gateway_api_ext,
    esm_entry_point = "proven:radixdlt_babylon_gateway_api",
    esm = [dir "node_modules/@radixdlt/babylon-gateway-api-sdk",
        "proven:radixdlt_babylon_gateway_api" = "dist/babylon-gateway-api-sdk.mjs"
    ],
);
