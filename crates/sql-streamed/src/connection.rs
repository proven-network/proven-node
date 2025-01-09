use crate::{
    DeserializeError as D, Error, Request, Request as T, Response, SerializeError as S,
    SqlServiceHandler as X,
};

use std::convert::Infallible;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use proven_messaging::{
    client::{Client, ClientResponseType},
    stream::{InitializedStream, Stream},
};
use proven_sql::{SqlConnection, SqlParam};
use proven_store::Store;

/// A connection to a streamed SQL store.
#[derive(Clone)]
pub struct Connection<P, SS>
where
    P: Stream<T, D, S>,
    SS: Store<Bytes, Infallible, Infallible>,
{
    client: <P::Initialized as InitializedStream<T, D, S>>::Client<X<P::Initialized, SS>>,
}

impl<P, SS> Connection<P, SS>
where
    P: Stream<T, D, S>,
    SS: Store<Bytes, Infallible, Infallible>,
{
    /// Creates a new `Connection` with the specified client.
    pub const fn new(
        client: <P::Initialized as InitializedStream<T, D, S>>::Client<X<P::Initialized, SS>>,
    ) -> Self {
        Self { client }
    }
}

#[async_trait]
impl<P, SS> SqlConnection for Connection<P, SS>
where
    P: Stream<T, D, S>,
    SS: Store<Bytes, Infallible, Infallible>,
{
    type Error = Error<P, SS>;

    async fn execute<Q: Clone + Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<SqlParam>,
    ) -> Result<u64, Self::Error> {
        let request = Request::Execute(query.into(), params);

        match self.client.request(request).await.map_err(Error::Client)? {
            ClientResponseType::Response(Response::Execute(affected_rows)) => Ok(affected_rows),
            ClientResponseType::Response(Response::Failed(error)) => Err(Error::Libsql(error)),
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
            ClientResponseType::Response(Response::ExecuteBatch(affected_rows)) => {
                Ok(affected_rows)
            }
            ClientResponseType::Response(Response::Failed(error)) => Err(Error::Libsql(error)),
            _ => unreachable!(),
        }
    }

    async fn migrate<Q: Clone + Into<String> + Send>(&self, query: Q) -> Result<bool, Self::Error> {
        let request = Request::Migrate(query.into());

        match self.client.request(request).await.map_err(Error::Client)? {
            ClientResponseType::Response(Response::Migrate(needed_migration)) => {
                Ok(needed_migration)
            }
            ClientResponseType::Response(Response::Failed(error)) => Err(Error::Libsql(error)),
            _ => unreachable!(),
        }
    }

    async fn query<Q: Clone + Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<SqlParam>,
    ) -> Result<Box<dyn futures::Stream<Item = Vec<SqlParam>> + Send + Unpin>, Self::Error> {
        let request = Request::Query(query.into(), params);

        match self.client.request(request).await.map_err(Error::Client)? {
            ClientResponseType::Stream(stream) => Ok(Box::new(stream.map(|item| match item {
                Response::Row(row) => row,
                _ => unreachable!(),
            }))),
            ClientResponseType::Response(Response::Failed(error)) => Err(Error::Libsql(error)),
            ClientResponseType::Response(_) => unreachable!(),
        }
    }
}
