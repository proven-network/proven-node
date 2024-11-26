use crate::{Error, HandlerError, Request, Response, Result};

use std::marker::PhantomData;

use async_trait::async_trait;
use bytes::Bytes;
use proven_sql::{Connection as SqlConnection, Rows, SqlParam};
use proven_store::Store;
use proven_stream::Stream;

#[derive(Clone)]
pub struct Connection<S: Stream<HandlerError>, LS: Store> {
    pub(crate) stream: S,
    pub(crate) _marker: PhantomData<LS>,
}

#[async_trait]
impl<S: Stream<HandlerError> + 'static, LS: Store> SqlConnection for Connection<S, LS> {
    type Error = Error<S::Error, LS::Error>;

    async fn execute<Q: Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<SqlParam>,
    ) -> Result<u64, S::Error, LS::Error> {
        let request = Request::Execute(query.into(), params);
        let bytes: Bytes = request.try_into()?;

        let raw_response = self.stream.request(bytes).await.map_err(Error::Stream)?;
        let response: Response = raw_response.try_into()?;

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

        let raw_response = self.stream.request(bytes).await.map_err(Error::Stream)?;
        let response: Response = raw_response.try_into()?;

        match response {
            Response::ExecuteBatch(affected_rows) => Ok(affected_rows),
            _ => unreachable!(),
        }
    }

    async fn migrate<Q: Into<String> + Send>(&self, query: Q) -> Result<bool, S::Error, LS::Error> {
        let request = Request::Migrate(query.into());
        let bytes: Bytes = request.try_into()?;

        let raw_response = self.stream.request(bytes).await.map_err(Error::Stream)?;
        let response: Response = raw_response.try_into()?;

        match response {
            Response::Migrate(needs_migration) => Ok(needs_migration),
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

        let raw_response = self.stream.request(bytes).await.map_err(Error::Stream)?;
        let response: Response = raw_response.try_into()?;

        match response {
            Response::Query(rows) => Ok(rows),
            _ => unreachable!(),
        }
    }
}
