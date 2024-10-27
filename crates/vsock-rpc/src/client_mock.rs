pub struct RpcClient;

impl RpcClient {
    pub fn new(_vsock_addr: ()) -> Self {
        Self
    }

    pub async fn initialize(&self, _args: ()) -> Result<(), ()> {
        Ok(())
    }

    pub async fn add_peer(&self, _args: ()) -> Result<(), ()> {
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), ()> {
        Ok(())
    }
}
