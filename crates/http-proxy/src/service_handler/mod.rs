mod error;

use error::Error;

use crate::request::Request;
use crate::response::Response;
use crate::{DeserializeError, SerializeError};

use async_trait::async_trait;
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::service_responder::ServiceResponder;
use proven_messaging::stream::InitializedStream;

/// A stream handler that executes SQL queries and migrations.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct HttpServiceHandler<S>
where
    S: InitializedStream<Request, DeserializeError, SerializeError>,
{
    stream: S,
}

#[async_trait]
impl<S> ServiceHandler<Request, DeserializeError, SerializeError> for HttpServiceHandler<S>
where
    S: InitializedStream<Request, DeserializeError, SerializeError>,
{
    type Error = Error;
    type ResponseType = Response;
    type ResponseDeserializationError = DeserializeError;
    type ResponseSerializationError = SerializeError;

    /// Handle a request.
    #[allow(unused_variables)]
    async fn handle<R>(
        &self,
        request: Request,
        responder: R,
    ) -> Result<R::UsedResponder, Self::Error>
    where
        R: ServiceResponder<
                Request,
                DeserializeError,
                SerializeError,
                Self::ResponseType,
                DeserializeError,
                SerializeError,
            >,
    {
        todo!()
    }
}
