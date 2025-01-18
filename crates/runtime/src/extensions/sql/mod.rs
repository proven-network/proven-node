mod application;
mod migrations;
mod nft;
mod personal;

pub use application::*;
pub use migrations::sql_migrations_ext;
pub use nft::*;
pub use personal::*;

use deno_core::extension;

extension!(
    sql_options_parser_ext,
    esm_entry_point = "proven:sql",
    esm = [
        dir "src/extensions/sql",
        "proven:sql" = "sql.js",
        "proven:sql-application" = "application/options-parser.js",
        "proven:sql-nft" = "nft/options-parser.js",
        "proven:sql-personal" = "personal/options-parser.js",
        "proven:sql-template-tag" = "template-tag.js",
    ],
    docs = "Functions for accessing sqlite"
);

extension!(
    sql_runtime_ext,
    esm_entry_point = "proven:sql",
    esm = [
        dir "src/extensions/sql",
        "proven:sql" = "sql.js",
        "proven:sql-application" = "application/runtime.js",
        "proven:sql-nft" = "nft/runtime.js",
        "proven:sql-personal" = "personal/runtime.js",
        "proven:sql-template-tag" = "template-tag.js",
    ],
    docs = "Functions for accessing sqlite"
);
