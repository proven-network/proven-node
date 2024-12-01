mod application;
mod connection_manager;
mod migrations;
mod nft;
mod personal;

pub use application::{sql_application_ext, ApplicationSqlParamListManager};
pub use connection_manager::*;
pub use migrations::sql_migrations_ext;
pub use personal::{sql_personal_ext, PersonalSqlParamListManager};

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
