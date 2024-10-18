use std::{future::Future, pin::Pin};

type RpcFuture = Pin<Box<dyn Future<Output = Result<(), ()>> + Send>>;
type RpcFn = Box<dyn FnOnce(()) -> RpcFuture + Send>;

pub enum RpcCall {
    Initialize((), RpcFn),
    AddPeer((), RpcFn),
    Shutdown(RpcFn),
}

pub struct RpcServer;

impl RpcServer {
    pub fn new(_vsock_addr: ()) -> Self {
        Self
    }

    pub async fn accept(&self) -> Result<RpcCall, ()> {
        Ok(RpcCall::Shutdown(Box::new(|_| Box::pin(async { Ok(()) }))))
    }
}
