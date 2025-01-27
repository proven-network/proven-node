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
use proven_radix_nft_verifier::RadixNftVerifier;
use proven_sql::{SqlStore1, SqlStore2};

extension!(
    sql_options_ext,
    esm_entry_point = "proven:sql",
    esm = [
        dir "src/extensions/sql",
        "proven:sql" = "sql-options.js",
    ],
    docs = "Functions for accessing sqlite"
);

extension!(
    sql_runtime_ext,
    parameters = [ ASS: SqlStore1, PSS: SqlStore1, NSS: SqlStore2, RNV: RadixNftVerifier ],
    ops = [
        op_add_blob_param,
        op_add_integer_param,
        op_add_null_param,
        op_add_real_param,
        op_add_text_param,
        op_create_params_list,
        op_get_row_batch,
        op_execute_application_sql<ASS>,
        op_query_application_sql<ASS>,
        op_execute_nft_sql<NSS, RNV>,
        op_query_nft_sql<NSS, RNV>,
        op_execute_personal_sql<PSS>,
        op_query_personal_sql<PSS>,
    ],
    esm_entry_point = "proven:sql",
    esm = [
        dir "src/extensions/sql",
        "proven:sql" = "sql-runtime.js",
    ],
    docs = "Functions for accessing sqlite"
);
