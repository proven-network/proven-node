use crate::extensions::{
    console_ext, crypto_ext, handler_options_parser_ext, kv_options_parser_ext, openai_ext,
    radixdlt_babylon_gateway_api_ext, radixdlt_radix_engine_toolkit_ext, session_ext,
    sql_migrations_ext, sql_options_parser_ext, uuid_ext, zod_ext, ConsoleState,
    GatewayDetailsState,
};
use crate::import_replacements::replace_esm_imports;
use crate::options::{ModuleHandlerOptions, ModuleOptions, SqlMigrations};
use crate::permissions::OriginAllowlistWebPermissions;
use crate::schema::SCHEMA_WHLIST;
use crate::Error;

use std::sync::Arc;
use std::time::Duration;

use regex::Regex;
use rustyscript::{ExtensionOptions, Module, WebOptions};
use sha2::{Digest, Sha256};

pub struct OptionsParser {
    runtime: rustyscript::Runtime,
}

impl OptionsParser {
    pub fn new() -> Result<Self, Error> {
        let mut runtime = rustyscript::Runtime::new(rustyscript::RuntimeOptions {
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
                radixdlt_babylon_gateway_api_ext::init_ops_and_esm(),
                radixdlt_radix_engine_toolkit_ext::init_ops_and_esm(),
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

        Ok(Self { runtime })
    }

    pub fn parse<M: Into<String>>(&mut self, module_source: M) -> Result<ModuleOptions, Error> {
        let module_source = module_source.into();

        let module_hash = Self::hash_source(module_source.clone());

        let module = Module::new(
            format!("{module_hash}.ts"),
            Self::preprocess_source(&module_source).as_str(),
        );

        self.runtime.put(ConsoleState::default())?;
        self.runtime.put(ModuleHandlerOptions::default())?;
        self.runtime.put(SqlMigrations::default())?;

        self.runtime.load_module(&module)?;
        let handler_options: ModuleHandlerOptions = self.runtime.take().unwrap();
        let sql_migrations: SqlMigrations = self.runtime.take().unwrap();

        Ok(ModuleOptions {
            handler_options,
            module_hash,
            module_source,
            sql_migrations,
        })
    }

    fn preprocess_source(module_source: &str) -> String {
        let module_source = replace_esm_imports(module_source);
        let module_source = Self::strip_comments(&module_source);
        let module_source = Self::name_default_export(&module_source);
        Self::rewrite_run_functions(&module_source)
    }

    fn strip_comments(module_source: &str) -> String {
        let comment_re = Regex::new(r"(?m)^\s*//.*|/\*[\s\S]*?\*/").unwrap();
        comment_re.replace_all(module_source, "").to_string()
    }

    fn name_default_export(module_source: &str) -> String {
        module_source.replace("export default ", "export const __default__ = ")
    }

    fn rewrite_run_functions(module_source: &str) -> String {
        // Define the regex to match `export const/let` declarations with the specified functions
        let re = Regex::new(r"(?m)^(\s*)export\s+(const|let)\s+(\w+)\s*=\s*(runOnHttp|runOnProvenEvent|runOnRadixEvent|runOnSchedule|runWithOptions)\(").unwrap();

        // Replace the matched string with the modified version
        let result = re.replace_all(module_source, |caps: &regex::Captures| {
            format!(
                "{}export {} {} = {}('{}', ",
                &caps[1], &caps[2], &caps[3], &caps[4], &caps[3]
            )
        });

        result.to_string()
    }

    pub fn hash_source(module_source: String) -> String {
        let mut hasher = Sha256::new();

        hasher.update(module_source);
        let result = hasher.finalize();

        // Convert the hash result to a hexadecimal string
        let hash_string = format!("{result:x}");

        hash_string
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::{HandlerOptions, HttpHandlerOptions, RpcHandlerOptions};

    #[test]
    fn test_new_options_parser() {
        assert!(OptionsParser::new().is_ok());
    }

    #[test]
    fn test_parse_module() {
        let module_source = r#"
            export default function() {
                console.log("Hello, world!");
            }
        "#;
        let result = OptionsParser::new().unwrap().parse(module_source);
        assert!(result.is_ok());
        let options = result.unwrap();
        assert_eq!(options.module_source, module_source);
    }

    #[test]
    fn test_parse_module_handler_options() {
        let module_source = r"
            import { runOnHttp, runWithOptions } from '@proven-network/handler';
            import { getApplicationDb, sql } from '@proven-network/sql';

            const DB = getApplicationDb('main');
            DB.migrate('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY);');

            export const handler = runOnHttp((x,y) => {
                console.log(x, y);
            }, {
                path: '/hello',
                timeout: 5000
            });

            export default runWithOptions((x,y) => {
                console.log(x, y);
            }, {
                timeout: 2000
            });
        ";
        let result = OptionsParser::new().unwrap().parse(module_source);
        assert!(result.is_ok());

        let options = result.unwrap();
        assert_eq!(options.module_source, module_source);
        assert_eq!(options.handler_options.len(), 2);

        assert!(options.handler_options.contains_key("handler"));
        assert!(options.handler_options.contains_key("__default__"));

        assert!(options.sql_migrations.application.contains_key("main"));
        assert_eq!(
            options.sql_migrations.application.get("main").unwrap(),
            &vec!["CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY);".to_string()]
        );

        assert_eq!(
            options.handler_options.get("handler").unwrap(),
            &HandlerOptions::Http(HttpHandlerOptions {
                path: Some("/hello".to_string()),
                timeout_millis: Some(5000),
                ..Default::default()
            })
        );

        assert_eq!(
            options.handler_options.get("__default__").unwrap(),
            &HandlerOptions::Rpc(RpcHandlerOptions {
                timeout_millis: Some(2000),
                ..Default::default()
            })
        );
    }

    #[test]
    fn test_strip_comments() {
        let source = r"
            // This is a comment
            const x = 42;/* This is another comment */
            /* This is another comment */console.log(x);
        ";
        let expected = r"
            const x = 42;
            console.log(x);
        ";
        assert_eq!(OptionsParser::strip_comments(source), expected);
    }

    #[test]
    fn test_name_default_export() {
        let source = r#"
            export default function() {
                console.log("Hello, world!");
            }
        "#;
        let expected = r#"
            export const __default__ = function() {
                console.log("Hello, world!");
            }
        "#;
        assert_eq!(OptionsParser::name_default_export(source), expected);
    }

    #[test]
    fn test_rewrite_run_functions() {
        let source = r"
            export const handler = runWithOptions((x,y) => {
                console.log(x, y);
            }, {
                timeout: 5000
            });
        ";
        let expected = r"
            export const handler = runWithOptions('handler', (x,y) => {
                console.log(x, y);
            }, {
                timeout: 5000
            });
        ";
        assert_eq!(OptionsParser::rewrite_run_functions(source), expected);
    }

    #[test]
    fn test_hash_source() {
        let source = "console.log('Hello, world!');";
        let hash = OptionsParser::hash_source(source.to_string());
        assert_eq!(hash.len(), 64); // SHA-256 hash length in hexadecimal
    }
}
