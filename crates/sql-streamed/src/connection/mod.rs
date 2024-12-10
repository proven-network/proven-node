mod error;

pub use error::Error;

use crate::{response::Response, Request};

use async_trait::async_trait;
use proven_messaging::client::Client;
use proven_sql::{Rows, SqlConnection, SqlParam};

/// A connection to a streamed SQL store.
#[derive(Clone)]
pub struct Connection<C>
where
    C: Client<Type = Request, ResponseType = Response>,
{
    client: C,
}

impl<C> Connection<C>
where
    C: Client<Type = Request, ResponseType = Response>,
{
    /// Creates a new `Connection` with the specified stream.
    pub const fn new(client: C) -> Self {
        Self { client }
    }
}

#[async_trait]
impl<C> SqlConnection for Connection<C>
where
    C: Client<Type = Request, ResponseType = Response>,
{
    type Error = Error<C::Error>;

    async fn execute<Q: Clone + Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<SqlParam>,
    ) -> Result<u64, Self::Error> {
        let request = Request::Execute(query.into(), params);

        match self.client.request(request).await.map_err(Error::Client)? {
            Response::Execute(affected_rows) => Ok(affected_rows),
            Response::Failed(error) => Err(Error::Libsql(error)),
            _ => unreachable!(),
        }
    }

    async fn execute_batch<Q: Clone + Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<Vec<SqlParam>>,
    ) -> Result<u64, Self::Error> {
        let request = Request::ExecuteBatch(query.into(), params);

        match self.client.request(request).await.map_err(Error::Client)? {
            Response::ExecuteBatch(affected_rows) => Ok(affected_rows),
            Response::Failed(error) => Err(Error::Libsql(error)),
            _ => unreachable!(),
        }
    }

    async fn migrate<Q: Clone + Into<String> + Send>(&self, query: Q) -> Result<bool, Self::Error> {
        let request = Request::Migrate(query.into());

        match self.client.request(request).await.map_err(Error::Client)? {
            Response::Migrate(needed_migration) => Ok(needed_migration),
            Response::Failed(error) => Err(Error::Libsql(error)),
            _ => unreachable!(),
        }
    }

    async fn query<Q: Clone + Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<SqlParam>,
    ) -> Result<Rows, Self::Error> {
        let request = Request::Query(query.into(), params);

        match self.client.request(request).await.map_err(Error::Client)? {
            Response::Query(rows) => Ok(rows),
            Response::Failed(error) => Err(Error::Libsql(error)),
            _ => unreachable!(),
        }
    }
}
