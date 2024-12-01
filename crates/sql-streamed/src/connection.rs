use crate::{Error, Request, Response, Result, SqlStreamHandler};

use std::marker::PhantomData;

use async_trait::async_trait;
use bytes::Bytes;
use proven_sql::{Rows, SqlConnection, SqlParam};
use proven_store::Store;
use proven_stream::{Stream, StreamHandler};

#[derive(Clone)]
pub struct Connection<S: Stream<SqlStreamHandler>, LS: Store>
where
    SqlStreamHandler: StreamHandler<S>,
{
    stream: S,
    _marker: PhantomData<LS>,
}

impl<S: Stream<SqlStreamHandler>, LS: Store> Connection<S, LS>
where
    SqlStreamHandler: StreamHandler<S>,
{
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<S: Stream<SqlStreamHandler> + 'static, LS: Store> SqlConnection for Connection<S, LS>
where
    SqlStreamHandler: StreamHandler<S>,
    S::Request: From<Bytes> + Into<Bytes>,
    S::Response: From<Bytes> + Into<Bytes>,
{
    type Error = Error<S::Error, LS::Error>;

    async fn execute<Q: Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<SqlParam>,
    ) -> Result<u64, S::Error, LS::Error> {
        let request = Request::Execute(query.into(), params);
        let bytes: Bytes = request.try_into()?;

        let raw_response = self
            .stream
            .request(bytes.into())
            .await
            .map_err(Error::Stream)?;
        let bytes_response: Bytes = raw_response.into();
        let response: Response = bytes_response.try_into()?;

        match response {
            Response::Execute(affected_rows) => Ok(affected_rows),
            _ => unreachable!(),
        }
    }

    async fn execute_batch<Q: Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<Vec<SqlParam>>,
    ) -> Result<u64, S::Error, LS::Error> {
        let request = Request::ExecuteBatch(query.into(), params);
        let bytes: Bytes = request.try_into()?;

        let raw_response = self
            .stream
            .request(bytes.into())
            .await
            .map_err(Error::Stream)?;
        let bytes_response: Bytes = raw_response.into();
        let response: Response = bytes_response.try_into()?;

        match response {
            Response::ExecuteBatch(affected_rows) => Ok(affected_rows),
            _ => unreachable!(),
        }
    }

    async fn migrate<Q: Into<String> + Send>(&self, query: Q) -> Result<bool, S::Error, LS::Error> {
        let request = Request::Migrate(query.into());
        let bytes: Bytes = request.try_into()?;

        let raw_response = self
            .stream
            .request(bytes.into())
            .await
            .map_err(Error::Stream)?;
        let bytes_response: Bytes = raw_response.into();
        let response: Response = bytes_response.try_into()?;

        match response {
            Response::Migrate(needed_migration) => Ok(needed_migration),
            _ => unreachable!(),
        }
    }

    async fn query<Q: Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<SqlParam>,
    ) -> Result<Rows, S::Error, LS::Error> {
        let request = Request::Query(query.into(), params);
        let bytes: Bytes = request.try_into()?;

        let raw_response = self
            .stream
            .request(bytes.into())
            .await
            .map_err(Error::Stream)?;
        let bytes_response: Bytes = raw_response.into();
        let response: Response = bytes_response.try_into()?;

        match response {
            Response::Query(rows) => Ok(rows),
            _ => unreachable!(),
        }
    }
}
