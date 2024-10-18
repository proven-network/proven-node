use proven_runtime::{Context, ExecutionRequest, Runtime};
use rustyscript::Error;
use serde_json::json;

fn main() -> Result<(), Error> {
    let user_module = r#"
        import { getCurrentAccounts, getCurrentIdentity } from "proven:sessions";

        export const handler = async (a, b) => {
            const userId = getCurrentIdentity();
            const accounts = getCurrentAccounts();
            console.info("Current identity: " + userId);
            console.log("Current accounts: " + accounts);

            return a + b;
        }
    "#;

    let mut runtime = Runtime::new(user_module)?;

    let request = ExecutionRequest {
        context: Context {
            identity: Some("identity".to_string()),
            accounts: Some(vec!["account1".to_string(), "account2".to_string()]),
        },
        handler_name: "handler".to_string(),
        args: vec![json!(10), json!(20)],
    };

    let result = runtime.execute(request)?;

    assert!(result.output.is_number());
    let output = result.output.as_i64().unwrap();
    assert_eq!(output, 30);

    println!("Result: {:?}", result);

    Ok(())
}