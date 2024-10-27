use deno_core::extension;

extension!(
    sql_ext,
    esm_entry_point = "proven:sql",
    esm = [ dir "src/extensions/sql", "proven:sql" = "sql.js" ],
    docs = "Functions for accessing sqlite"
);
