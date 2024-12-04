use crate::stream_handler::SqlStreamHandler;
use crate::{Error, Request, Response};

use std::marker::PhantomData;

use async_trait::async_trait;
use bytes::Bytes;
use proven_sql::{Rows, SqlConnection, SqlParam};
use proven_store::Store;
use proven_stream::Stream;

/// A connection to a streamed SQL store.
#[derive(Clone, Debug)]
pub struct Connection<S: Stream<SqlStreamHandler>, LS: Store> {
    stream: S,
    _marker: PhantomData<LS>,
}

impl<S: Stream<SqlStreamHandler>, LS: Store> Connection<S, LS> {
    /// Creates a new `Connection` with the specified stream.
    pub const fn new(stream: S) -> Self {
        Self {
            stream,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<S: Stream<SqlStreamHandler> + 'static, LS: Store> SqlConnection for Connection<S, LS> {
    type Error = Error<S::Error, LS::Error>;

    async fn execute<Q: Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<SqlParam>,
    ) -> Result<u64, Self::Error> {
        let request = Request::Execute(query.into(), params);
        let bytes: Bytes = request.try_into()?;

        let raw_response = self.stream.request(bytes).await.map_err(Error::Stream)?;
        let response: Response = raw_response.try_into()?;

        match response {
            Response::Execute(affected_rows) => Ok(affected_rows),
            Response::Failed(error) => Err(Error::Libsql(error)),
            _ => unreachable!(),
        }
    }

    async fn execute_batch<Q: Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<Vec<SqlParam>>,
    ) -> Result<u64, Self::Error> {
        let request = Request::ExecuteBatch(query.into(), params);
        let bytes: Bytes = request.try_into()?;

        let raw_response = self.stream.request(bytes).await.map_err(Error::Stream)?;
        let response: Response = raw_response.try_into()?;

        match response {
            Response::ExecuteBatch(affected_rows) => Ok(affected_rows),
            Response::Failed(error) => Err(Error::Libsql(error)),
            _ => unreachable!(),
        }
    }

    async fn migrate<Q: Into<String> + Send>(&self, query: Q) -> Result<bool, Self::Error> {
        let request = Request::Migrate(query.into());
        let bytes: Bytes = request.try_into()?;

        let raw_response = self.stream.request(bytes).await.map_err(Error::Stream)?;
        let response: Response = raw_response.try_into()?;

        match response {
            Response::Migrate(needed_migration) => Ok(needed_migration),
            Response::Failed(error) => Err(Error::Libsql(error)),
            _ => unreachable!(),
        }
    }

    async fn query<Q: Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<SqlParam>,
    ) -> Result<Rows, Self::Error> {
        let request = Request::Query(query.into(), params);
        let bytes: Bytes = request.try_into()?;

        let raw_response = self.stream.request(bytes).await.map_err(Error::Stream)?;
        let response: Response = raw_response.try_into()?;

        match response {
            Response::Query(rows) => Ok(rows),
            Response::Failed(error) => Err(Error::Libsql(error)),
            _ => unreachable!(),
        }
    }
}
