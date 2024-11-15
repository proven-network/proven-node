use crate::extensions::*;
use crate::options::{ModuleHandlerOptions, ModuleOptions};
use crate::schema::SCHEMA_WHLIST;
use crate::vendor_replacements::replace_vendor_imports;
use crate::web_permissions::NoWebPermissions;
use crate::Error;

use std::rc::Rc;
use std::time::Duration;

use regex::Regex;
use rustyscript::{ExtensionOptions, Module, WebOptions};
use sha2::{Digest, Sha256};

pub struct OptionsParser {
    runtime: rustyscript::Runtime,
}

impl OptionsParser {
    pub fn new() -> Result<Self, Error> {
        let runtime = rustyscript::Runtime::new(rustyscript::RuntimeOptions {
            timeout: Duration::from_millis(5000),
            schema_whlist: SCHEMA_WHLIST.clone(),
            extensions: vec![
                run_ext::init_ops_and_esm(),
                console_ext::init_ops_and_esm(),
                sessions_ext::init_ops_and_esm(),
                kv_ext::init_ops_and_esm(),
                sql_ext::init_ops_and_esm(),
                // Vendered modules
                radixdlt_babylon_gateway_api_ext::init_ops_and_esm(),
                uuid_ext::init_ops_and_esm(),
                zod_ext::init_ops_and_esm(),
            ],
            extension_options: ExtensionOptions {
                web: WebOptions {
                    // No access to web during option extraction
                    permissions: Rc::new(NoWebPermissions::new()),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        })?;

        Ok(Self { runtime })
    }

    pub fn parse<M: Into<String>>(&mut self, module_source: M) -> Result<ModuleOptions, Error> {
        let module_source = module_source.into();

        let module_hash = Self::hash_source(module_source.clone());

        println!("{}", Self::preprocess_source(module_source.clone()));

        let module = Module::new(
            format!("{}.ts", module_hash),
            Self::preprocess_source(module_source.clone()).as_str(),
        );

        self.runtime.put(ConsoleState::default())?;
        self.runtime.put(ModuleHandlerOptions::default())?;

        self.runtime.load_module(&module)?;
        let handler_options: ModuleHandlerOptions = self.runtime.take().unwrap();

        Ok(ModuleOptions {
            handler_options,
            module_hash,
            module_source,
        })
    }

    fn preprocess_source(module_source: String) -> String {
        let module_source = replace_vendor_imports(module_source);
        let module_source = Self::strip_comments(module_source);
        let module_source = Self::name_default_export(module_source);
        Self::rewrite_run_functions(module_source)
    }

    fn strip_comments(module_source: String) -> String {
        let comment_re = Regex::new(r"(?m)^\s*//.*|/\*[\s\S]*?\*/").unwrap();
        comment_re
            .replace_all(module_source.as_str(), "")
            .to_string()
    }

    fn name_default_export(module_source: String) -> String {
        module_source.replace("export default ", "export const __default__ = ")
    }

    fn rewrite_run_functions(module_source: String) -> String {
        // Define the regex to match `export const/let` declarations with the specified functions
        let re = Regex::new(r"(?m)^(\s*)export\s+(const|let)\s+(\w+)\s*=\s*(runOnHttp|runOnProvenEvent|runOnRadixEvent|runOnSchedule|runWithOptions)\(").unwrap();

        // Replace the matched string with the modified version
        let result = re.replace_all(module_source.as_str(), |caps: &regex::Captures| {
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
        let hash_string = format!("{:x}", result);

        hash_string
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::{HandlerOptions, HttpHandlerOptions, RpcHandlerOptions};

    #[test]
    fn test_new_options_parser() {
        let parser = OptionsParser::new();
        assert!(parser.is_ok());
    }

    #[test]
    fn test_parse_module() {
        let mut parser = OptionsParser::new().unwrap();
        let module_source = r#"
            export default function() {
                console.log("Hello, world!");
            }
        "#;
        let result = parser.parse(module_source);
        assert!(result.is_ok());
        let options = result.unwrap();
        assert_eq!(options.module_source, module_source);
    }

    #[test]
    fn test_parse_module_handler_options() {
        let mut parser = OptionsParser::new().unwrap();
        let module_source = r#"
            import { runOnHttp, runWithOptions } from 'proven:run';

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
        "#;
        let result = parser.parse(module_source);
        assert!(result.is_ok());

        let options = result.unwrap();
        assert_eq!(options.module_source, module_source);
        assert_eq!(options.handler_options.len(), 2);

        assert!(options.handler_options.contains_key("handler"));
        assert!(options.handler_options.contains_key("__default__"));

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
        let source = r#"
            // This is a comment
            const x = 42;/* This is another comment */
            /* This is another comment */console.log(x);
        "#;
        let expected = r#"
            const x = 42;
            console.log(x);
        "#;
        assert_eq!(OptionsParser::strip_comments(source.to_string()), expected);
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
        assert_eq!(
            OptionsParser::name_default_export(source.to_string()),
            expected
        );
    }

    #[test]
    fn test_rewrite_run_functions() {
        let source = r#"
            export const handler = runWithOptions((x,y) => {
                console.log(x, y);
            }, {
                timeout: 5000
            });
        "#;
        let expected = r#"
            export const handler = runWithOptions('handler', (x,y) => {
                console.log(x, y);
            }, {
                timeout: 5000
            });
        "#;
        assert_eq!(
            OptionsParser::rewrite_run_functions(source.to_string()),
            expected
        );
    }

    #[test]
    fn test_hash_source() {
        let source = "console.log('Hello, world!');";
        let hash = OptionsParser::hash_source(source.to_string());
        assert_eq!(hash.len(), 64); // SHA-256 hash length in hexadecimal
    }
}
