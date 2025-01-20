mod application;
mod migrations;
mod nft;
mod param_list_manager;
mod personal;

pub use application::*;
pub use migrations::sql_migrations_ext;
pub use nft::*;
pub use param_list_manager::*;
pub use personal::*;

use deno_core::extension;

extension!(
    sql_options_parser_ext,
    esm_entry_point = "proven:sql",
    esm = [
        dir "src/extensions/sql",
        "proven:sql" = "sql-options-parser.js",
    ],
    docs = "Functions for accessing sqlite"
);

extension!(
    sql_runtime_ext,
    ops = [
        op_add_blob_param,
        op_add_integer_param,
        op_add_null_param,
        op_add_real_param,
        op_add_text_param,
        op_create_params_list,
    ],
    esm_entry_point = "proven:sql",
    esm = [
        dir "src/extensions/sql",
        "proven:sql" = "sql-runtime.js",
    ],
    docs = "Functions for accessing sqlite"
);
