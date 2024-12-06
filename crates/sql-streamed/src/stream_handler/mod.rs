mod error;

use crate::request::Request;
use crate::response::Response;
pub use error::{Error, Result};

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use proven_libsql::Database;
use proven_stream::{HandlerResponse, StreamHandler};
use tokio::sync::{oneshot, Mutex};

/// A stream handler that executes SQL queries and migrations.
#[derive(Clone, Debug)]
pub struct SqlStreamHandler {
    applied_migrations: Arc<Mutex<Vec<String>>>,
    caught_up_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    database: Database,
}

/// Options for configuring a `SqlStreamHandler`.
pub(crate) struct SqlStreamHandlerOptions {
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

    /// Returns a list of applied migrations.
    pub async fn applied_migrations(&self) -> Vec<String> {
        self.applied_migrations.lock().await.clone()
    }
}

#[async_trait]
impl StreamHandler for SqlStreamHandler {
    type Error = Error;
    type Request = Request;
    type Response = Response;

    async fn handle(&self, request: Request) -> Result<HandlerResponse<Response>> {
        let mut headers = HashMap::with_capacity(1);

        let response = match request {
            Request::Execute(sql, params) => match self.database.execute(&sql, params).await {
                Ok(affected_rows) => Response::Execute(affected_rows),
                Err(e) => {
                    headers.insert("Request-Should-Persist".to_string(), "false".to_string());
                    Response::Failed(e)
                }
            },
            Request::ExecuteBatch(sql, params) => {
                match self.database.execute_batch(&sql, params).await {
                    Ok(affected_rows) => Response::ExecuteBatch(affected_rows),
                    Err(e) => {
                        headers.insert("Request-Should-Persist".to_string(), "false".to_string());
                        Response::Failed(e)
                    }
                }
            }
            Request::Migrate(sql) => match self.database.migrate(&sql).await {
                Ok(needed_to_run) => {
                    if needed_to_run {
                        self.applied_migrations.lock().await.push(sql);
                    } else {
                        // Don't persist migrations that don't need to run.
                        headers.insert(
                            "Request-Message-Should-Persist".to_string(),
                            "false".to_string(),
                        );
                    }
                    Response::Migrate(needed_to_run)
                }
                Err(e) => {
                    headers.insert("Request-Should-Persist".to_string(), "false".to_string());
                    Response::Failed(e)
                }
            },
            Request::Query(sql, params) => {
                // Never persist query responses.
                headers.insert("Request-Should-Persist".to_string(), "false".to_string());

                match self.database.query(&sql, params).await {
                    Ok(rows) => Response::Query(rows),
                    Err(e) => Response::Failed(e),
                }
            }
        };

        Ok(HandlerResponse {
            headers,
            data: response,
        })
    }

    async fn on_caught_up(&self) -> Result<()> {
        let value = self.caught_up_tx.lock().await.take();
        if let Some(tx) = value {
            let _ = tx.send(());
        }
        Ok(())
    }
}
