use std::sync::Arc;

use multi_key_map::MultiKeyMap;
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;

#[derive(Debug)]
pub struct MultiResolver {
    resolvers: MultiKeyMap<String, Arc<dyn ResolvesServerCert>>,
    default_resolver: Arc<dyn ResolvesServerCert>,
}

impl MultiResolver {
    pub fn new(default_resolver: Arc<dyn ResolvesServerCert>) -> Self {
        Self {
            resolvers: MultiKeyMap::new(),
            default_resolver,
        }
    }

    pub fn add_resolver(&mut self, domains: Vec<String>, resolver: Arc<dyn ResolvesServerCert>) {
        self.resolvers.insert_many(domains, resolver);
    }
}

impl ResolvesServerCert for MultiResolver {
    fn resolve(&self, client_hello: ClientHello) -> Option<Arc<CertifiedKey>> {
        if let Some(server_name) = client_hello.server_name() {
            let domain = server_name.to_string();
            // Try to find resolver by domain
            if let Some(resolver) = self.resolvers.get(&domain) {
                return resolver.resolve(client_hello);
            }
        }

        self.default_resolver.resolve(client_hello)
    }
}
