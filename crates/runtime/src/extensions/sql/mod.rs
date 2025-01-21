mod application;
mod migrations;
mod nft;
mod param_list_manager;
mod personal;
mod query_results_manager;

pub use application::*;
pub use migrations::sql_migrations_ext;
pub use nft::*;
pub use param_list_manager::*;
pub use personal::*;
pub use query_results_manager::*;

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
        op_get_row_batch,
    ],
    esm_entry_point = "proven:sql",
    esm = [
        dir "src/extensions/sql",
        "proven:sql" = "sql-runtime.js",
    ],
    docs = "Functions for accessing sqlite"
);
