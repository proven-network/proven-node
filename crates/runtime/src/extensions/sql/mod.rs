mod application;
mod migrations;

pub use application::sql_application_ext;
pub use migrations::sql_migrations_ext;

use deno_core::extension;

extension!(
    sql_options_parser_ext,
    esm_entry_point = "proven:sql",
    esm = [
        dir "src/extensions/sql",
        "proven:sql" = "sql-options-parser.js",
        "proven:sql-template-tag" = "template-tag.js",
    ],
    docs = "Functions for accessing sqlite"
);

extension!(
    sql_runtime_ext,
    esm_entry_point = "proven:sql",
    esm = [
        dir "src/extensions/sql",
        "proven:sql" = "sql-runtime.js",
        "proven:sql-template-tag" = "template-tag.js",
    ],
    docs = "Functions for accessing sqlite"
);
