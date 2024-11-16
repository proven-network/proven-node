use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct HttpHandlerOptions {
    pub allowed_web_origins: HashSet<String>,
    pub max_heap_mbs: Option<u16>,
    pub path: Option<String>,
    pub timeout_millis: Option<u32>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RpcHandlerOptions {
    pub allowed_web_origins: HashSet<String>,
    pub max_heap_mbs: Option<u16>,
    pub timeout_millis: Option<u32>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum HandlerOptions {
    Http(HttpHandlerOptions),
    Rpc(RpcHandlerOptions),
}

// Maps each invokable export to its options.
pub type ModuleHandlerOptions = HashMap<String, HandlerOptions>;

#[derive(Clone, Debug, Default)]
#[allow(dead_code)] // TODO: Remove this once used
pub struct ModuleOptions {
    pub handler_options: ModuleHandlerOptions,
    pub module_hash: String,
    pub module_source: String,
}
