use deno_core::extension;

mod console;
mod gateway_api_sdk;
mod kv;
mod run;
mod sessions;
mod sql;

pub use console::*;
pub use gateway_api_sdk::*;
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
    radixdlt_radix_engine_toolkit_ext,
    esm_entry_point = "proven:radixdlt_radix_engine_toolkit",
    esm = [dir "vendor/@radixdlt/radix-engine-toolkit",
        "proven:radixdlt_radix_engine_toolkit" = "index.mjs"
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
