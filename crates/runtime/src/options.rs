use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(dead_code)] // TODO: Remove this once used
pub enum EventBinding {
    Emitter(String),
    Event(String),
    EventFromEmitter(String, String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HandlerOptions {
    Http {
        allowed_web_origins: HashSet<String>,
        max_heap_mbs: Option<u16>,
        path: Option<String>,
        timeout_millis: Option<u32>,
    },
    RadixEvent {
        allowed_web_origins: HashSet<String>,
        event_binding: Option<EventBinding>,
        max_heap_mbs: Option<u16>,
        timeout_millis: Option<u32>,
    },
    Rpc {
        allowed_web_origins: HashSet<String>,
        max_heap_mbs: Option<u16>,
        timeout_millis: Option<u32>,
    },
}

// Maps each invokable export to its options.
pub type ModuleHandlerOptions = HashMap<String, HandlerOptions>;

#[derive(Clone, Debug, Default)]
pub struct SqlMigrations {
    pub application: HashMap<String, Vec<String>>,
    pub nft: HashMap<String, Vec<String>>,
    pub personal: HashMap<String, Vec<String>>,
}

#[derive(Clone, Debug, Default)]
pub struct ModuleOptions {
    pub handler_options: ModuleHandlerOptions,
    pub sql_migrations: SqlMigrations,
}
