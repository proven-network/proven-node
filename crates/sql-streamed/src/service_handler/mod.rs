mod error;

use crate::request::Request;
use crate::response::Response;
pub use error::Error;

use std::sync::Arc;

use async_trait::async_trait;
use proven_libsql::Database;
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::service_responder::ServiceResponder;
use tokio::sync::{oneshot, Mutex};

/// A stream handler that executes SQL queries and migrations.
#[derive(Clone, Debug)]
pub struct SqlServiceHandler {
    applied_migrations: Arc<Mutex<Vec<String>>>,
    caught_up_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    database: Database,
}

impl SqlServiceHandler {
    pub(crate) fn new(
        applied_migrations: Arc<Mutex<Vec<String>>>,
        caught_up_tx: oneshot::Sender<()>,
        database: Database,
    ) -> Self {
        Self {
            applied_migrations,
            caught_up_tx: Arc::new(Mutex::new(Some(caught_up_tx))),
            database,
        }
    }
}

#[async_trait]
impl
    ServiceHandler<
        Request,
        ciborium::de::Error<std::io::Error>,
        ciborium::ser::Error<std::io::Error>,
    > for SqlServiceHandler
{
    type Error = Error;
    type ResponseType = Response;
    type ResponseDeserializationError = ciborium::de::Error<std::io::Error>;
    type ResponseSerializationError = ciborium::ser::Error<std::io::Error>;

    async fn handle<R>(&self, request: Request, responder: R) -> Result<R::UsedResponder, Error>
    where
        R: ServiceResponder<
            Request,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
            Self::ResponseType,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
    {
        Ok(match request {
            Request::Execute(sql, params) => match self.database.execute(&sql, params).await {
                Ok(affected_rows) => responder.reply(Response::Execute(affected_rows)).await,
                Err(e) => {
                    responder
                        .reply_and_delete_request(Response::Failed(e))
                        .await
                }
            },
            Request::ExecuteBatch(sql, params) => {
                match self.database.execute_batch(&sql, params).await {
                    Ok(affected_rows) => {
                        responder.reply(Response::ExecuteBatch(affected_rows)).await
                    }
                    Err(e) => {
                        responder
                            .reply_and_delete_request(Response::Failed(e))
                            .await
                    }
                }
            }
            Request::Migrate(sql) => match self.database.migrate(&sql).await {
                Ok(needed_to_run) => {
                    if needed_to_run {
                        self.applied_migrations.lock().await.push(sql);
                        responder.reply(Response::Migrate(needed_to_run)).await
                    } else {
                        responder
                            .reply_and_delete_request(Response::Migrate(needed_to_run))
                            .await
                    }
                }
                Err(e) => {
                    responder
                        .reply_and_delete_request(Response::Failed(e))
                        .await
                }
            },
            Request::Query(sql, params) => match self.database.query(&sql, params).await {
                Ok(rows) => responder.reply(Response::Query(rows)).await,
                Err(e) => {
                    responder
                        .reply_and_delete_request(Response::Failed(e))
                        .await
                }
            },
        })
    }

    async fn on_caught_up(&self) -> Result<(), Error> {
        let value = self.caught_up_tx.lock().await.take();
        if let Some(tx) = value {
            let _ = tx.send(());
        }
        Ok(())
    }
}
