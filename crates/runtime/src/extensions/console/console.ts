const { op_console_log } = Deno.core.ops;

globalThis.console.debug = function debug(...args) {
  op_console_log("debug", args);
};

globalThis.console.error = function error(...args) {
  op_console_log("error", args);
};

globalThis.console.info = function info(...args) {
  op_console_log("info", args);
};

globalThis.console.log = function log(...args) {
  op_console_log("log", args);
};

globalThis.console.warn = function warn(...args) {
  op_console_log("warn", args);
};
