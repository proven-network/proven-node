mod error;

use crate::request::Request;
use crate::response::Response;
pub use error::{Error, Result};

use std::sync::Arc;

use async_trait::async_trait;
use proven_libsql::Database;
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::{HeaderMap, Message};
use tokio::sync::{oneshot, Mutex};

/// A stream handler that executes SQL queries and migrations.
#[derive(Clone, Debug)]
pub struct SqlStreamHandler {
    applied_migrations: Arc<Mutex<Vec<String>>>,
    caught_up_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    database: Database,
}

/// Options for configuring a `SqlStreamHandler`.
pub struct SqlStreamHandlerOptions {
    pub applied_migrations: Arc<Mutex<Vec<String>>>,
    pub caught_up_tx: oneshot::Sender<()>,
    pub database: Database,
}

impl SqlStreamHandler {
    pub(crate) fn new(
        SqlStreamHandlerOptions {
            applied_migrations,
            caught_up_tx,
            database,
        }: SqlStreamHandlerOptions,
    ) -> Self {
        Self {
            applied_migrations,
            caught_up_tx: Arc::new(Mutex::new(Some(caught_up_tx))),
            database,
        }
    }
}

#[async_trait]
impl ServiceHandler for SqlStreamHandler {
    type Error = Error;
    type Type = Request;
    type ResponseType = Response;

    async fn handle(
        &self,
        Message {
            headers: _,
            payload: request,
        }: Message<Request>,
    ) -> Result<Message<Response>> {
        let mut headers = HeaderMap::new();

        let response = match request {
            Request::Execute(sql, params) => match self.database.execute(&sql, params).await {
                Ok(affected_rows) => Response::Execute(affected_rows),
                Err(e) => {
                    headers.insert("Request-Should-Persist", "false");
                    Response::Failed(e)
                }
            },
            Request::ExecuteBatch(sql, params) => {
                match self.database.execute_batch(&sql, params).await {
                    Ok(affected_rows) => Response::ExecuteBatch(affected_rows),
                    Err(e) => {
                        headers.insert("Request-Should-Persist", "false");
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
                        headers.insert("Request-Should-Persist", "false");
                    }
                    Response::Migrate(needed_to_run)
                }
                Err(e) => {
                    headers.insert("Request-Should-Persist", "false");
                    Response::Failed(e)
                }
            },
            Request::Query(sql, params) => {
                // Never persist query responses.
                headers.insert("Request-Should-Persist", "false");

                match self.database.query(&sql, params).await {
                    Ok(rows) => Response::Query(rows),
                    Err(e) => Response::Failed(e),
                }
            }
        };

        Ok(Message {
            headers: Some(headers),
            payload: response,
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
