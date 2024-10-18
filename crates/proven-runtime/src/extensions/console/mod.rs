use deno_core::{extension, op2};

#[derive(Default)]
pub struct ConsoleState {
    pub messages: Vec<String>,
}

#[op2(fast)]
pub fn op_console_log(
    #[state] state: &mut ConsoleState,
    #[string] msg: String,
    #[string] level: String,
) {
    state.messages.push(format!("[{}] {}", level, msg));
}

extension!(
    console_ext,
    ops = [op_console_log],
    esm_entry_point = "ext:console.js",
    esm = [ dir "src/extensions/console", "ext:console.js" = "console.js" ],
    docs = "Functions for logging"
);
