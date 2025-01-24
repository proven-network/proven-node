use crate::extensions::{
    babylon_gateway_api_ext, console_ext, crypto_ext, handler_options_parser_ext,
    kv_options_parser_ext, openai_ext, radix_engine_toolkit_ext, session_ext, sql_migrations_ext,
    sql_options_parser_ext, uuid_ext, zod_ext, ConsoleState, GatewayDetailsState,
};
use crate::module_loader::{ModuleLoader, ProcessingMode};
use crate::options::{ModuleHandlerOptions, ModuleOptions, SqlMigrations};
use crate::permissions::OriginAllowlistWebPermissions;
use crate::schema::SCHEMA_WHLIST;
use crate::Error;

use std::sync::Arc;
use std::time::Duration;

use deno_core::ModuleSpecifier;
use rustyscript::{ExtensionOptions, WebOptions};

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
                handler_options_parser_ext::init_ops_and_esm(),
                console_ext::init_ops_and_esm(),
                crypto_ext::init_ops_and_esm(),
                session_ext::init_ops_and_esm(),
                kv_options_parser_ext::init_ops_and_esm(),
                sql_options_parser_ext::init_ops_and_esm(),
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
                    permissions: Arc::new(OriginAllowlistWebPermissions::new()),
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

        let Some(module) = module_loader.get_module(module_specifier, &ProcessingMode::Options)
        else {
            return Err(Error::SpecifierNotFoundInCodePackage);
        };

        runtime.load_module(&module)?;

        let handler_options: ModuleHandlerOptions = runtime.take().unwrap();
        let sql_migrations: SqlMigrations = runtime.take().unwrap();

        drop(runtime);

        Ok(ModuleOptions {
            handler_options,
            sql_migrations,
        })
    }
}
