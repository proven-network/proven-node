//! Request handling logic for the SQL service.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use libsql::{Transaction as LibsqlTransaction, Value};
use proven_sql::SqlParam;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    libsql_error::{LibsqlError, StatementType},
    pool::ConnectionPool,
    request::Request,
    response::Response,
};

static RESERVED_TABLE_PREFIX: &str = "__proven_";

/// Handle a SQL request.
pub async fn handle_request(
    pool: &Arc<ConnectionPool>,
    transactions: &Arc<Mutex<HashMap<Uuid, LibsqlTransaction>>>,
    request: Request,
) -> Response {
    match request {
        Request::Execute(sql, params) => handle_execute(pool, sql, params).await,
        Request::ExecuteBatch(sql, params) => handle_execute_batch(pool, sql, params).await,
        Request::Migrate(sql) => handle_migrate(pool, sql).await,
        Request::Query(sql, params) => handle_query(pool, sql, params).await,
        Request::BeginTransaction => handle_begin_transaction(pool, transactions).await,
        Request::TransactionExecute(tx_id, sql, params) => {
            handle_transaction_execute(transactions, tx_id, sql, params).await
        }
        Request::TransactionQuery(tx_id, sql, params) => {
            handle_transaction_query(transactions, tx_id, sql, params).await
        }
        Request::TransactionCommit(tx_id) => handle_transaction_commit(transactions, tx_id).await,
        Request::TransactionRollback(tx_id) => {
            handle_transaction_rollback(transactions, tx_id).await
        }
    }
}

async fn handle_execute(
    pool: &Arc<ConnectionPool>,
    sql: String,
    params: Vec<SqlParam>,
) -> Response {
    if sql.contains(RESERVED_TABLE_PREFIX) {
        return Response::Failed(LibsqlError::UsedReservedTablePrefix);
    }

    match classify_sql(&sql) {
        StatementType::Migration => Response::Failed(LibsqlError::IncorrectSqlType(
            StatementType::Mutation,
            StatementType::Migration,
        )),
        StatementType::Mutation => match pool.get_write_connection().await {
            Ok(pooled_conn) => match pooled_conn.get() {
                Ok(conn) => {
                    let libsql_params = convert_params(params);
                    match conn.execute(&sql, libsql_params).await {
                        Ok(affected_rows) => Response::Execute(affected_rows),
                        Err(e) => Response::Failed(LibsqlError::Libsql(e.to_string())),
                    }
                }
                Err(e) => Response::Failed(LibsqlError::Unknown(e.to_string())),
            },
            Err(e) => Response::Failed(LibsqlError::Unknown(e.to_string())),
        },
        StatementType::Query => Response::Failed(LibsqlError::IncorrectSqlType(
            StatementType::Mutation,
            StatementType::Query,
        )),
        StatementType::Unknown => Response::Failed(LibsqlError::IncorrectSqlType(
            StatementType::Mutation,
            StatementType::Unknown,
        )),
    }
}

async fn handle_execute_batch(
    pool: &Arc<ConnectionPool>,
    sql: String,
    params: Vec<Vec<SqlParam>>,
) -> Response {
    if sql.contains(RESERVED_TABLE_PREFIX) {
        return Response::Failed(LibsqlError::UsedReservedTablePrefix);
    }

    match classify_sql(&sql) {
        StatementType::Migration => Response::Failed(LibsqlError::IncorrectSqlType(
            StatementType::Mutation,
            StatementType::Migration,
        )),
        StatementType::Mutation => match pool.get_write_connection().await {
            Ok(pooled_conn) => match pooled_conn.get() {
                Ok(conn) => {
                    let mut total: u64 = 0;
                    for param_set in params {
                        let libsql_params = convert_params(param_set);
                        match conn.execute(&sql, libsql_params).await {
                            Ok(affected_rows) => total += affected_rows,
                            Err(e) => return Response::Failed(LibsqlError::Libsql(e.to_string())),
                        }
                    }
                    Response::ExecuteBatch(total)
                }
                Err(e) => Response::Failed(LibsqlError::Unknown(e.to_string())),
            },
            Err(e) => Response::Failed(LibsqlError::Unknown(e.to_string())),
        },
        StatementType::Query => Response::Failed(LibsqlError::IncorrectSqlType(
            StatementType::Mutation,
            StatementType::Query,
        )),
        StatementType::Unknown => Response::Failed(LibsqlError::IncorrectSqlType(
            StatementType::Mutation,
            StatementType::Unknown,
        )),
    }
}

async fn handle_migrate(pool: &Arc<ConnectionPool>, sql: String) -> Response {
    if sql.contains(RESERVED_TABLE_PREFIX) {
        return Response::Failed(LibsqlError::UsedReservedTablePrefix);
    }

    match classify_sql(&sql) {
        StatementType::Migration => {
            // Check for schema names in table operations
            let upper_query = sql.trim_start().to_uppercase();
            let mut words = upper_query.split_whitespace();
            while let Some(word) = words.next() {
                if word == "TABLE" {
                    if let Some(next_word) = words.next()
                        && next_word.contains('.')
                    {
                        return Response::Failed(LibsqlError::SchemaNameNotAllowed);
                    }
                    break;
                }
            }

            match pool.get_write_connection().await {
                Ok(pooled_conn) => match pooled_conn.get() {
                    Ok(conn) => {
                        let mut hasher = Sha256::new();
                        hasher.update(&sql);
                        let hash = format!("{:x}", hasher.finalize());

                        // Check if migration already applied
                        match conn
                            .query(
                                "SELECT COUNT(*) FROM __proven_migrations WHERE query_hash = ?1",
                                vec![Value::Text(hash.clone())],
                            )
                            .await
                        {
                            Ok(mut rows) => {
                                if let Ok(Some(row)) = rows.next().await
                                    && let Ok(Value::Integer(count)) = row.get(0)
                                    && count > 0
                                {
                                    return Response::Migrate(false);
                                }

                                // Apply migration in transactions
                                match conn.transaction().await {
                                    Ok(tx) => {
                                        // Execute the migration
                                        if let Err(e) = tx.execute(&sql, ()).await {
                                            return Response::Failed(LibsqlError::Libsql(
                                                e.to_string(),
                                            ));
                                        }

                                        // Record the migration
                                        let params = vec![Value::Text(hash), Value::Text(sql)];
                                        if let Err(e) = tx
                                            .execute(
                                                "INSERT INTO __proven_migrations (query_hash, query) VALUES (?1, ?2)",
                                                params,
                                            )
                                            .await
                                        {
                                            return Response::Failed(LibsqlError::Libsql(
                                                e.to_string(),
                                            ));
                                        }

                                        // Commit transaction
                                        if let Err(e) = tx.commit().await {
                                            return Response::Failed(LibsqlError::Libsql(
                                                e.to_string(),
                                            ));
                                        }

                                        Response::Migrate(true)
                                    }
                                    Err(e) => Response::Failed(LibsqlError::Libsql(e.to_string())),
                                }
                            }
                            Err(e) => Response::Failed(LibsqlError::Libsql(e.to_string())),
                        }
                    }
                    Err(e) => Response::Failed(LibsqlError::Unknown(e.to_string())),
                },
                Err(e) => Response::Failed(LibsqlError::Unknown(e.to_string())),
            }
        }
        StatementType::Mutation => Response::Failed(LibsqlError::IncorrectSqlType(
            StatementType::Migration,
            StatementType::Mutation,
        )),
        StatementType::Query => Response::Failed(LibsqlError::IncorrectSqlType(
            StatementType::Migration,
            StatementType::Query,
        )),
        StatementType::Unknown => Response::Failed(LibsqlError::IncorrectSqlType(
            StatementType::Migration,
            StatementType::Unknown,
        )),
    }
}

async fn handle_query(pool: &Arc<ConnectionPool>, sql: String, params: Vec<SqlParam>) -> Response {
    if sql.contains(RESERVED_TABLE_PREFIX) {
        return Response::Failed(LibsqlError::UsedReservedTablePrefix);
    }

    match classify_sql(&sql) {
        StatementType::Query => {
            // Use read connection for queries
            match pool.get_read_connection().await {
                Ok(pooled_conn) => match pooled_conn.get() {
                    Ok(conn) => {
                        let libsql_params = convert_params(params);
                        match conn.query(&sql, libsql_params).await {
                            Ok(mut rows) => {
                                let mut all_rows = Vec::new();

                                // Get column count from the first row
                                let mut column_count = 0;
                                let mut has_column_names = true;

                                while let Ok(Some(row)) = rows.next().await {
                                    if column_count == 0 {
                                        column_count = rows.column_count();
                                    }

                                    let sql_params = (0..column_count)
                                        .filter_map(|i| {
                                            if has_column_names {
                                                let column_name = rows
                                                    .column_name(i)
                                                    .unwrap_or_default()
                                                    .to_string();
                                                row.get_value(i).ok().map(|value| match value {
                                                    Value::Blob(b) => SqlParam::BlobWithName(
                                                        column_name,
                                                        Bytes::from(b),
                                                    ),
                                                    Value::Integer(n) => {
                                                        SqlParam::IntegerWithName(column_name, n)
                                                    }
                                                    Value::Null => {
                                                        SqlParam::NullWithName(column_name)
                                                    }
                                                    Value::Real(r) => {
                                                        SqlParam::RealWithName(column_name, r)
                                                    }
                                                    Value::Text(s) => {
                                                        SqlParam::TextWithName(column_name, s)
                                                    }
                                                })
                                            } else {
                                                row.get_value(i).ok().map(|value| match value {
                                                    Value::Blob(b) => {
                                                        SqlParam::Blob(Bytes::from(b))
                                                    }
                                                    Value::Integer(n) => SqlParam::Integer(n),
                                                    Value::Null => SqlParam::Null,
                                                    Value::Real(r) => SqlParam::Real(r),
                                                    Value::Text(s) => SqlParam::Text(s),
                                                })
                                            }
                                        })
                                        .collect::<Vec<SqlParam>>();

                                    all_rows.push(sql_params);
                                    has_column_names = false; // Only first row gets column names
                                }

                                Response::Rows(all_rows)
                            }
                            Err(e) => Response::Failed(LibsqlError::Libsql(e.to_string())),
                        }
                    }
                    Err(e) => Response::Failed(LibsqlError::Unknown(e.to_string())),
                },
                Err(e) => Response::Failed(LibsqlError::Unknown(e.to_string())),
            }
        }
        StatementType::Migration => Response::Failed(LibsqlError::IncorrectSqlType(
            StatementType::Query,
            StatementType::Migration,
        )),
        StatementType::Mutation => Response::Failed(LibsqlError::IncorrectSqlType(
            StatementType::Query,
            StatementType::Mutation,
        )),
        StatementType::Unknown => Response::Failed(LibsqlError::IncorrectSqlType(
            StatementType::Query,
            StatementType::Unknown,
        )),
    }
}

async fn handle_begin_transaction(
    pool: &Arc<ConnectionPool>,
    transactions: &Arc<Mutex<HashMap<Uuid, LibsqlTransaction>>>,
) -> Response {
    match pool.get_transaction_connection().await {
        Ok(pooled_conn) => {
            // Take the connection out of the pool for the transaction duration
            if let Some(conn) = pooled_conn.take() {
                match conn.transaction().await {
                    Ok(tx) => {
                        let tx_id = Uuid::new_v4();
                        transactions.lock().await.insert(tx_id, tx);
                        Response::TransactionBegun(tx_id)
                    }
                    Err(e) => Response::Failed(LibsqlError::Libsql(e.to_string())),
                }
            } else {
                Response::Failed(LibsqlError::Unknown("Failed to get connection".to_string()))
            }
        }
        Err(e) => Response::Failed(LibsqlError::Unknown(e.to_string())),
    }
}

async fn handle_transaction_execute(
    transactions: &Arc<Mutex<HashMap<Uuid, LibsqlTransaction>>>,
    tx_id: Uuid,
    sql: String,
    params: Vec<SqlParam>,
) -> Response {
    let txs = transactions.lock().await;
    if let Some(tx) = txs.get(&tx_id) {
        let libsql_params = convert_params(params);
        match tx.execute(&sql, libsql_params).await {
            Ok(affected_rows) => Response::TransactionExecute(affected_rows),
            Err(e) => Response::Failed(LibsqlError::Unknown(e.to_string())),
        }
    } else {
        Response::Failed(LibsqlError::Unknown("Transaction not found".to_string()))
    }
}

async fn handle_transaction_query(
    transactions: &Arc<Mutex<HashMap<Uuid, LibsqlTransaction>>>,
    tx_id: Uuid,
    sql: String,
    params: Vec<SqlParam>,
) -> Response {
    let txs = transactions.lock().await;
    if let Some(tx) = txs.get(&tx_id) {
        let libsql_params = convert_params(params);
        match tx.query(&sql, libsql_params).await {
            Ok(mut rows) => {
                // Convert first row to SqlParam format
                if let Ok(Some(row)) = rows.next().await {
                    let column_count = rows.column_count();
                    let sql_params = (0..column_count)
                        .filter_map(|i| {
                            let column_name = rows.column_name(i).unwrap_or_default().to_string();
                            row.get_value(i).ok().map(|value| match value {
                                Value::Blob(b) => {
                                    SqlParam::BlobWithName(column_name, Bytes::from(b))
                                }
                                Value::Integer(n) => SqlParam::IntegerWithName(column_name, n),
                                Value::Null => SqlParam::NullWithName(column_name),
                                Value::Real(r) => SqlParam::RealWithName(column_name, r),
                                Value::Text(s) => SqlParam::TextWithName(column_name, s),
                            })
                        })
                        .collect::<Vec<SqlParam>>();
                    Response::TransactionRow(sql_params)
                } else {
                    Response::TransactionRow(vec![])
                }
            }
            Err(e) => Response::Failed(LibsqlError::Unknown(e.to_string())),
        }
    } else {
        Response::Failed(LibsqlError::Unknown("Transaction not found".to_string()))
    }
}

async fn handle_transaction_commit(
    transactions: &Arc<Mutex<HashMap<Uuid, LibsqlTransaction>>>,
    tx_id: Uuid,
) -> Response {
    let mut txs = transactions.lock().await;
    if let Some(tx) = txs.remove(&tx_id) {
        match tx.commit().await {
            Ok(()) => Response::TransactionCommitted,
            Err(e) => Response::Failed(LibsqlError::Unknown(e.to_string())),
        }
    } else {
        Response::Failed(LibsqlError::Unknown("Transaction not found".to_string()))
    }
}

async fn handle_transaction_rollback(
    transactions: &Arc<Mutex<HashMap<Uuid, LibsqlTransaction>>>,
    tx_id: Uuid,
) -> Response {
    let mut txs = transactions.lock().await;
    if let Some(tx) = txs.remove(&tx_id) {
        match tx.rollback().await {
            Ok(()) => Response::TransactionRolledBack,
            Err(e) => Response::Failed(LibsqlError::Unknown(e.to_string())),
        }
    } else {
        Response::Failed(LibsqlError::Unknown("Transaction not found".to_string()))
    }
}

fn convert_params(params: Vec<SqlParam>) -> Vec<Value> {
    params
        .into_iter()
        .map(|p| match p {
            SqlParam::Blob(b) | SqlParam::BlobWithName(_, b) => Value::Blob(b.to_vec()),
            SqlParam::Integer(i) | SqlParam::IntegerWithName(_, i) => Value::Integer(i),
            SqlParam::Null | SqlParam::NullWithName(_) => Value::Null,
            SqlParam::Real(r) | SqlParam::RealWithName(_, r) => Value::Real(r),
            SqlParam::Text(s) | SqlParam::TextWithName(_, s) => Value::Text(s),
        })
        .collect()
}

fn classify_sql(sql: &str) -> StatementType {
    let sql = sql.trim_start().to_uppercase();

    if sql.starts_with("SELECT") {
        StatementType::Query
    } else if sql.starts_with("CREATE TABLE")
        || sql.starts_with("ALTER TABLE")
        || sql.starts_with("DROP TABLE")
        || sql.starts_with("CREATE INDEX")
        || sql.starts_with("DROP INDEX")
    {
        StatementType::Migration
    } else if sql.starts_with("INSERT")
        || sql.starts_with("REPLACE")
        || sql.starts_with("UPDATE")
        || sql.starts_with("DELETE")
        || sql.starts_with("UPSERT")
    {
        StatementType::Mutation
    } else {
        StatementType::Unknown
    }
}
