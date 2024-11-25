use std::sync::{Arc, RwLock};

use multi_key_map::MultiKeyMap;
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;

#[derive(Debug)]
pub struct MultiResolver {
    inner: Arc<RwLock<MultiResolverInner>>,
}

#[derive(Debug)]
struct MultiResolverInner {
    resolvers: MultiKeyMap<String, Arc<dyn ResolvesServerCert>>,
    default_resolver: Arc<dyn ResolvesServerCert>,
}

impl MultiResolver {
    pub fn new(default_resolver: Arc<dyn ResolvesServerCert>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(MultiResolverInner {
                resolvers: MultiKeyMap::new(),
                default_resolver,
            })),
        }
    }

    pub fn add_resolver(&self, domains: Vec<String>, resolver: Arc<dyn ResolvesServerCert>) {
        let mut inner = self.inner.write().expect("Failed to acquire write lock");
        inner.resolvers.insert_many(domains, resolver);
    }
}

impl ResolvesServerCert for MultiResolver {
    fn resolve(&self, client_hello: ClientHello) -> Option<Arc<CertifiedKey>> {
        let inner = self.inner.read().expect("Failed to acquire read lock");

        if let Some(server_name) = client_hello.server_name() {
            let domain = server_name.to_string();
            if let Some(resolver) = inner.resolvers.get(&domain) {
                return resolver.resolve(client_hello);
            }
        }

        inner.default_resolver.resolve(client_hello)
    }
}
