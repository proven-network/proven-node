mod application;
mod nft;
mod personal;

pub use application::kv_application_ext;
pub use nft::kv_nft_ext;
pub use personal::kv_personal_ext;

use deno_core::extension;

extension!(
    kv_ext,
    esm_entry_point = "proven:kv",
    esm = [ dir "src/extensions/kv", "proven:kv" = "kv.js" ],
    docs = "Functions for accessing secure storage"
);
