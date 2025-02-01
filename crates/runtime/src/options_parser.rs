use crate::extensions::{
    babylon_gateway_api_ext, console_ext, crypto_ext, handler_options_ext, kv_options_ext,
    openai_ext, radix_engine_toolkit_ext, session_ext, sql_migrations_ext, sql_options_ext,
    uuid_ext, zod_ext, ConsoleState, GatewayDetailsState,
};
use crate::module_loader::{ModuleLoader, ProcessingMode};
use crate::options::{
    HandlerOptions, HttpEndpoint, ModuleHandlerOptions, ModuleOptions, SqlMigrations,
};
use crate::permissions::OriginAllowlistWebPermissions;
use crate::schema::SCHEMA_WHLIST;
use crate::{Error, HandlerSpecifier};

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use deno_core::ModuleSpecifier;
use rustyscript::{ExtensionOptions, WebOptions};
use tokio::sync::oneshot;

pub struct OptionsParser;

impl OptionsParser {
    pub fn parse(
        module_loader: &ModuleLoader,
        module_specifier: &ModuleSpecifier,
    ) -> Result<ModuleOptions, Error> {
        let mut runtime = rustyscript::Runtime::new(rustyscript::RuntimeOptions {
            import_provider: Some(Box::new(
                module_loader.import_provider(ProcessingMode::Options),
            )),
            timeout: Duration::from_millis(5000),
            schema_whlist: SCHEMA_WHLIST.clone(),
            extensions: vec![
                handler_options_ext::init_ops_and_esm(),
                console_ext::init_ops_and_esm(),
                crypto_ext::init_ops_and_esm(),
                session_ext::init_ops_and_esm(),
                kv_options_ext::init_ops_and_esm(),
                sql_options_ext::init_ops_and_esm(),
                sql_migrations_ext::init_ops(),
                // Vendered modules
                openai_ext::init_ops_and_esm(),
                babylon_gateway_api_ext::init_ops_and_esm(),
                radix_engine_toolkit_ext::init_ops_and_esm(),
                uuid_ext::init_ops_and_esm(),
                zod_ext::init_ops_and_esm(),
            ],
            extension_options: ExtensionOptions {
                web: WebOptions {
                    // No access to web during option extraction
                    permissions: Arc::new(OriginAllowlistWebPermissions::new(vec![])),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        })?;

        runtime.put(GatewayDetailsState::default())?;

        runtime.put(ConsoleState::default())?;
        runtime.put(ModuleHandlerOptions::default())?;
        runtime.put(SqlMigrations::default())?;

        let Some(module) = module_loader.get_module(module_specifier, ProcessingMode::Options)
        else {
            return Err(Error::SpecifierNotFoundInCodePackage(
                module_specifier.clone(),
            ));
        };

        runtime.load_module(&module)?;

        let handler_options: ModuleHandlerOptions = runtime.take().unwrap();
        let sql_migrations: SqlMigrations = runtime.take().unwrap();

        drop(runtime);

        let http_endpoints = handler_options
            .iter()
            .filter_map(
                |(handler_specifier, handler_options)| match handler_options {
                    HandlerOptions::Http { path, .. } if path.is_some() => Some(HttpEndpoint {
                        handler_specifier: HandlerSpecifier::parse(handler_specifier).unwrap(),
                        path: path.clone().unwrap(),
                        method: None, // TODO: Add handler options for specifying methods
                    }),
                    _ => None,
                },
            )
            .collect();

        Ok(ModuleOptions {
            handler_options,
            http_endpoints,
            sql_migrations,
        })
    }

    /// Asynchronous version of parse that runs in a separate thread to avoid blocking.
    ///
    /// # Parameters
    /// - `module_loader`: The module loader to use for parsing.
    /// - `module_specifier`: The specifier of the module to parse options from.
    ///
    /// # Returns
    /// A result containing the module options.
    ///
    /// # Errors
    /// This function will return an error if the parsing fails or if there are channel communication issues.
    pub async fn parse_async(
        module_loader: ModuleLoader,
        module_specifier: ModuleSpecifier,
    ) -> Result<ModuleOptions, Error> {
        let (sender, receiver) = oneshot::channel();

        thread::spawn(move || {
            let result = Self::parse(&module_loader, &module_specifier);
            if sender.send(result).is_err() {
                eprintln!("Failed to send parse result through channel");
            }
        });

        receiver.await.unwrap_or_else(|_| {
            eprintln!("Failed to receive parse result from channel");
            Err(Error::ChannelCommunicationError)
        })
    }
}
