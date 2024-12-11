use crate::Error;

use crate::{Request, Response, SqlStreamHandler};

use async_trait::async_trait;
use proven_messaging::{
    client::Client,
    stream::{InitializedStream, Stream},
};
use proven_sql::{Rows, SqlConnection, SqlParam};

/// A connection to a streamed SQL store.
#[derive(Clone)]
pub struct Connection<S>
where
    S: Stream<Request, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
    S::Initialized: InitializedStream<
        Request,
        ciborium::de::Error<std::io::Error>,
        ciborium::ser::Error<std::io::Error>,
    >,
{
    client: <S::Initialized as InitializedStream<
        Request,
        ciborium::de::Error<std::io::Error>,
        ciborium::ser::Error<std::io::Error>,
    >>::ClientType<SqlStreamHandler>,
}

impl<S> Connection<S>
where
    S: Stream<Request, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
    S::Initialized: InitializedStream<
        Request,
        ciborium::de::Error<std::io::Error>,
        ciborium::ser::Error<std::io::Error>,
    >,
    <S::Initialized as InitializedStream<
        Request,
        ciborium::de::Error<std::io::Error>,
        ciborium::ser::Error<std::io::Error>,
    >>::ClientType<SqlStreamHandler>: Client<
        S::Initialized,
        SqlStreamHandler,
        Request,
        ciborium::de::Error<std::io::Error>,
        ciborium::ser::Error<std::io::Error>,
    >,
{
    /// Creates a new `Connection` with the specified client.
    pub const fn new(
        client: <S::Initialized as InitializedStream<
            Request,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >>::ClientType<SqlStreamHandler>,
    ) -> Self {
        Self { client }
    }
}

#[async_trait]
impl<S> SqlConnection for Connection<S>
where
    S: Stream<Request, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
    S::Initialized: InitializedStream<
        Request,
        ciborium::de::Error<std::io::Error>,
        ciborium::ser::Error<std::io::Error>,
    >,
    <S::Initialized as InitializedStream<
        Request,
        ciborium::de::Error<std::io::Error>,
        ciborium::ser::Error<std::io::Error>,
    >>::ClientType<SqlStreamHandler>: Client<
        S::Initialized,
        SqlStreamHandler,
        Request,
        ciborium::de::Error<std::io::Error>,
        ciborium::ser::Error<std::io::Error>,
    >,
{
    type Error =
        Error<S::Error<ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>>;

    async fn execute<Q: Clone + Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<SqlParam>,
    ) -> Result<u64, Self::Error> {
        let request = Request::Execute(query.into(), params);

        match self
            .client
            .request(request)
            .await
            .map_err(|_| Error::Client)?
        {
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

        match self
            .client
            .request(request)
            .await
            .map_err(|_| Error::Client)?
        {
            Response::ExecuteBatch(affected_rows) => Ok(affected_rows),
            Response::Failed(error) => Err(Error::Libsql(error)),
            _ => unreachable!(),
        }
    }

    async fn migrate<Q: Clone + Into<String> + Send>(&self, query: Q) -> Result<bool, Self::Error> {
        let request = Request::Migrate(query.into());

        match self
            .client
            .request(request)
            .await
            .map_err(|_| Error::Client)?
        {
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

        match self
            .client
            .request(request)
            .await
            .map_err(|_| Error::Client)?
        {
            Response::Query(rows) => Ok(rows),
            Response::Failed(error) => Err(Error::Libsql(error)),
            _ => unreachable!(),
        }
    }
}
