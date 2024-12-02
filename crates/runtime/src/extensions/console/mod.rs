#![allow(clippy::inline_always)]
#![allow(clippy::significant_drop_tightening)]

use deno_core::{extension, op2, v8};

#[derive(Default)]
pub struct ConsoleState {
    pub messages: Vec<LogMessage>,
}

#[derive(Debug)]
pub struct LogMessage {
    pub args: v8::Global<v8::Value>,
    pub level: String,
}

#[op2]
pub fn op_console_log(
    #[state] state: &mut ConsoleState,
    #[string] level: String,
    #[global] args: v8::Global<v8::Value>,
) {
    state.messages.push(LogMessage { args, level });
}

extension!(
    console_ext,
    ops = [op_console_log],
    esm_entry_point = "ext:console.js",
    esm = [ dir "src/extensions/console", "ext:console.js" = "console.js" ],
    docs = "Functions for logging"
);
