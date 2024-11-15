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
    openai_ext,
    esm_entry_point = "proven:openai",
    esm = [dir "vendor/openai",
        "proven:openai" = "index.mjs"
    ],
);

extension!(
    radixdlt_babylon_gateway_api_ext,
    esm_entry_point = "proven:radixdlt_babylon_gateway_api",
    esm = [dir "node_modules/@radixdlt/babylon-gateway-api-sdk",
        "proven:radixdlt_babylon_gateway_api" = "dist/babylon-gateway-api-sdk.mjs"
    ],
);

extension!(
    uuid_ext,
    esm_entry_point = "proven:uuid",
    esm = [dir "vendor/uuid",
        "proven:uuid" = "index.mjs"
    ],
);

extension!(
    zod_ext,
    esm_entry_point = "proven:zod",
    esm = [dir "vendor/zod",
        "proven:zod" = "index.mjs"
    ],
);
