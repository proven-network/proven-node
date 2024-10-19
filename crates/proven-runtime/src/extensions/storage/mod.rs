mod application;
mod personal;

pub use application::storage_application_ext;
pub use personal::storage_personal_ext;

use deno_core::extension;

extension!(
    storage_ext,
    esm_entry_point = "proven:storage",
    esm = [ dir "src/extensions/storage", "proven:storage" = "storage.js" ],
    docs = "Functions for accessing secure storage"
);
