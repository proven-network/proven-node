const validOptions = [
  'memory',
  'timeout'
]

// Handler name is dynamically inserted and should not be part of exported types.
export const runWithOptions = (handlerName, fn, options = {}) => {
  if (typeof handlerName !== 'string') {
    throw new Error('runWithOptions must be used in confunction with the export keyword')
  }

  if (typeof fn !== 'function') {
    throw new Error('No function passed to runWithOptions')
  }

  for (const key of Object.keys(options)) {
    if (!validOptions.includes(key)) {
      throw new Error(`Invalid option: ${key}`)
    }

    Deno.core.ops[`op_set_${key}_option`](handlerName, options[key]);
  }

  return fn
}
