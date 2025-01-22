use crate::extensions::typescript_ext;
use crate::schema::SCHEMA_WHLIST;
use crate::Error;

use rustyscript::Module;

pub struct Preprocessor {
    module_handle: rustyscript::ModuleHandle,
    runtime: rustyscript::Runtime,
}

impl Preprocessor {
    pub fn new() -> Result<Self, Error> {
        let mut runtime = rustyscript::Runtime::new(rustyscript::RuntimeOptions {
            schema_whlist: SCHEMA_WHLIST.clone(),
            extensions: vec![typescript_ext::init_ops_and_esm()],
            ..Default::default()
        })?;

        let module = Module::new(
            "preprocessor.ts",
            include_str!("extensions/preprocess/preprocess.ts")
                .replace("\"typescript\"", "'proven:typescript'"),
        );

        let module_handle = runtime.load_module(&module)?;

        Ok(Self {
            module_handle,
            runtime,
        })
    }

    pub fn process<M: Into<String>>(&mut self, module_source: M) -> Result<String, Error> {
        let module_source: String = module_source.into();
        let json_string = serde_json::json!(module_source);

        let args = vec![json_string];

        let result: serde_json::Value = self.runtime.call_entrypoint(&self.module_handle, &args)?;

        Ok(result.as_str().unwrap().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_preprocess_module() {
        let module_source = r#"
            export default function() {
                console.log("Hello, world!");
            }
        "#;
        let result = Preprocessor::new().unwrap().process(module_source).unwrap();

        assert_eq!(
            result.trim(),
            r#"
import { runWithOptions } from "proven:handler";
export const defaultName = runWithOptions(async (): Promise<any> => {
    console.log("Hello, world!");
}, {});
            "#
            .trim()
        );
    }
}
