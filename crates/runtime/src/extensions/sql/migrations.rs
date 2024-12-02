#![allow(clippy::inline_always)]
#![allow(clippy::significant_drop_tightening)]

use crate::options::SqlMigrations;

use deno_core::{extension, op2};

#[op2(fast)]
pub fn op_migrate_application_sql(
    #[state] state: &mut SqlMigrations,
    #[string] db_name: String,
    #[string] query: String,
) {
    state.application.entry(db_name).or_default().push(query);
}

#[op2(fast)]
pub fn op_migrate_nft_sql(
    #[state] state: &mut SqlMigrations,
    #[string] db_name: String,
    #[string] query: String,
) {
    state.nft.entry(db_name).or_default().push(query);
}

#[op2(fast)]
pub fn op_migrate_personal_sql(
    #[state] state: &mut SqlMigrations,
    #[string] db_name: String,
    #[string] query: String,
) {
    state.personal.entry(db_name).or_default().push(query);
}

extension!(
    sql_migrations_ext,
    ops = [
        op_migrate_application_sql,
        op_migrate_nft_sql,
        op_migrate_personal_sql
    ],
);
