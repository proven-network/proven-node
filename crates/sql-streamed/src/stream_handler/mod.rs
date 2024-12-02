mod error;

use crate::request::Request;
use crate::response::Response;
pub use error::{Error, Result};

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use proven_libsql::Database;
use proven_stream::{HandlerResponse, StreamHandler};
use tokio::sync::{oneshot, Mutex};

#[derive(Clone, Debug)]
pub struct SqlStreamHandler {
    applied_migrations: Arc<Mutex<Vec<String>>>,
    caught_up_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    database: Database,
}

pub struct SqlStreamHandlerOptions {
    pub caught_up_tx: oneshot::Sender<()>,
    pub database: Database,
}

impl SqlStreamHandler {
    pub(crate) fn new(
        SqlStreamHandlerOptions {
            caught_up_tx,
            database,
        }: SqlStreamHandlerOptions,
    ) -> Self {
        Self {
            applied_migrations: Arc::new(Mutex::new(Vec::new())),
            caught_up_tx: Arc::new(Mutex::new(Some(caught_up_tx))),
            database,
        }
    }

    pub async fn applied_migrations(&self) -> Vec<String> {
        self.applied_migrations.lock().await.clone()
    }
}

#[async_trait]
impl StreamHandler for SqlStreamHandler {
    type HandlerError = Error;

    async fn handle(&self, bytes: Bytes) -> Result<HandlerResponse> {
        let request: Request = bytes.try_into()?;
        println!("Request: {:?}", request);

        let mut headers = HashMap::with_capacity(1);

        let response = match request {
            Request::Execute(sql, params) => {
                let affected_rows = self.database.execute(&sql, params).await?;
                Response::Execute(affected_rows)
            }
            Request::ExecuteBatch(sql, params) => {
                let affected_rows = self.database.execute_batch(&sql, params).await?;
                Response::ExecuteBatch(affected_rows)
            }
            Request::Migrate(sql) => {
                let needed_to_run = self.database.migrate(&sql).await?;
                if needed_to_run {
                    self.applied_migrations.lock().await.push(sql);
                }
                Response::Migrate(needed_to_run)
            }
            Request::Query(sql, params) => {
                // Used in relevent stream implemenetations to remove queries from append-only logs
                headers.insert(
                    "Request-Message-Should-Persist".to_string(),
                    "false".to_string(),
                );

                let rows = self.database.query(&sql, params).await?;
                Response::Query(rows)
            }
        };

        let data = response
            .try_into()
            .map_err(|e| Error::CborSerialize(Arc::new(e)))?;

        Ok(HandlerResponse { headers, data })
    }

    async fn on_caught_up(&self) -> Result<()> {
        let value = self.caught_up_tx.lock().await.take();
        if let Some(tx) = value {
            let _ = tx.send(());
        }
        Ok(())
    }
}
