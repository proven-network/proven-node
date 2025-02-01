use crate::options_parser::OptionsParser;
use crate::{Error, HandlerSpecifier, ModuleLoader};

use std::collections::{HashMap, HashSet};

use deno_core::ModuleSpecifier;
use proven_code_package::CodePackage;

/// Defines which Radix transactions should be invoke a given handler.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EventBinding {
    /// Any event from a particular emitter.
    Emitter(String),

    /// Any event regardless of the emitter.
    Event(String),

    /// A specific event from a specific emitter.
    EventFromEmitter(String, String),
}

/// Options for a specific handler.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HandlerOptions {
    /// Options for an HTTP handler.
    Http {
        /// The allowed web origins for outgoing requests.
        allowed_web_origins: HashSet<String>,

        /// The maximum heap size in MBs.
        max_heap_mbs: Option<u16>,

        /// The HTTP method. If `None`, the handler should be invoked for any method.
        method: Option<String>,

        /// The path the handler should be mounted at.
        path: Option<String>,

        /// The timeout in milliseconds.
        timeout_millis: Option<u32>,
    },
    /// Options for a Radix event handler.
    RadixEvent {
        /// The allowed web origins for outgoing requests.
        allowed_web_origins: HashSet<String>,

        /// The Radix event binding.
        event_binding: Option<EventBinding>,

        /// The maximum heap size in MBs.
        max_heap_mbs: Option<u16>,

        /// The timeout in milliseconds.
        timeout_millis: Option<u32>,
    },
    /// Options for an RPC handler.
    Rpc {
        /// The allowed web origins for outgoing requests.
        allowed_web_origins: HashSet<String>,

        /// The maximum heap size in MBs.
        max_heap_mbs: Option<u16>,

        /// The timeout in milliseconds.
        timeout_millis: Option<u32>,
    },
}

/// Maps each invokable export to its options.
pub type ModuleHandlerOptions = HashMap<String, HandlerOptions>;

/// Contains a mapping of store names to their SQL migrations for each of the three scopes.
#[derive(Clone, Debug, Default)]
pub struct SqlMigrations {
    /// Migrations for the application scope.
    pub application: HashMap<String, Vec<String>>,

    /// Migrations for the NFT scope.
    pub nft: HashMap<String, Vec<String>>,

    /// Migrations for the personal scope.
    pub personal: HashMap<String, Vec<String>>,
}

/// Details about a declared HTTP endpoint.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct HttpEndpoint {
    /// The handler specifier to execute.
    pub handler_specifier: HandlerSpecifier,

    /// The HTTP path.
    pub path: String,

    /// The HTTP method. If `None`, the endpoint should be invoked for any method.
    pub method: Option<String>,
}

/// Options for a module (and any submodules it imports).
#[derive(Clone, Debug, Default)]
pub struct ModuleOptions {
    /// The options for each handler.
    pub handler_options: ModuleHandlerOptions,

    /// The HTTP endpoints declared by the module.
    pub http_endpoints: HashSet<HttpEndpoint>,

    /// The SQL migrations declared by the module.
    pub sql_migrations: SqlMigrations,
}

impl ModuleOptions {
    /// Creates a new `ModuleOptions` instance from a code package and module specifier.
    ///
    /// # Arguments
    /// * `code_package` - The code package containing the module
    /// * `module_specifier` - The specifier for the module to parse options from
    ///
    /// # Returns
    /// The parsed module options
    ///
    /// # Errors
    /// Returns an error if the module options cannot be parsed from the code package
    pub async fn from_code_package(
        code_package: &CodePackage,
        module_specifier: &ModuleSpecifier,
    ) -> Result<Self, Error> {
        OptionsParser::parse_async(
            ModuleLoader::new(code_package.clone()),
            module_specifier.clone(),
        )
        .await
    }
}
