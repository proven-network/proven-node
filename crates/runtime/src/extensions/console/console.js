function logMessage(level, ...args) {
  const { op_console_log } = Deno.core.ops;
  op_console_log(level, args);
}

globalThis.console.debug = function debug(...args) {
  logMessage('debug', ...args);
}

globalThis.console.error = function error(...args) {
  logMessage('error', ...args);
}

globalThis.console.info = function info(...args) {
  logMessage('info', ...args);
}

globalThis.console.log = function log(...args) {
  logMessage('log', ...args);
}

globalThis.console.warn = function warn(...args) {
  logMessage('warn', ...args);
}
