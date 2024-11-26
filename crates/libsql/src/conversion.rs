use bytes::Bytes;
use libsql::{Rows as LibsqlRows, Value, ValueType};
use proven_sql::{Rows, SqlParam};

use crate::Error;

pub async fn convert_libsql_rows(mut libsql_rows: LibsqlRows) -> Result<Rows, Error> {
    let column_count = libsql_rows.column_count();
    let column_names = get_column_names(&libsql_rows, column_count);
    let column_types = get_column_types(&libsql_rows, column_count);
    let rows = get_row_values(&mut libsql_rows, column_count).await?;

    Ok(Rows {
        column_count: column_count as u16,
        column_names,
        column_types,
        rows,
    })
}

fn get_column_names(rows: &LibsqlRows, column_count: i32) -> Vec<String> {
    (0..column_count)
        .map(|i| rows.column_name(i).unwrap().to_string())
        .collect()
}

fn get_column_types(rows: &LibsqlRows, column_count: i32) -> Vec<String> {
    (0..column_count)
        .map(|i| match rows.column_type(i).unwrap() {
            ValueType::Text => "TEXT".to_string(),
            ValueType::Integer => "INTEGER".to_string(),
            ValueType::Real => "REAL".to_string(),
            ValueType::Blob => "BLOB".to_string(),
            ValueType::Null => "NULL".to_string(),
        })
        .collect()
}

async fn get_row_values(
    rows: &mut LibsqlRows,
    column_count: i32,
) -> Result<Vec<Vec<SqlParam>>, Error> {
    let mut rows_vec = Vec::new();
    while let Some(row) = rows.next().await? {
        let row_vec = (0..column_count)
            .map(|i| match row.get_value(i).unwrap() {
                Value::Null => SqlParam::Null,
                Value::Integer(i) => SqlParam::Integer(i),
                Value::Real(r) => SqlParam::Real(r),
                Value::Text(s) => SqlParam::Text(s),
                Value::Blob(b) => SqlParam::Blob(Bytes::from(b)),
            })
            .collect();
        rows_vec.push(row_vec);
    }
    Ok(rows_vec)
}
