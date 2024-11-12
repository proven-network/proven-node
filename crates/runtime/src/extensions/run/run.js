const validOptions = [
  'allowedHosts',
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

    if (key === 'allowedHosts') {
      if (!Array.isArray(options[key])) {
        throw new Error('allowedHosts must be an array')
      }

      for (const host of options[key]) {
        if (typeof host !== 'string') {
          throw new Error('allowedHosts must be an array of strings')
        }

        Deno.core.ops[`op_add_allowed_host`]('rpc', handlerName, host);
      }
    } else {
      Deno.core.ops[`op_set_${key}_option`]('rpc', handlerName, options[key]);
    }
  }

  return fn
}

const validHttpOptions = [
  'memory',
  'timeout',
  'path',
]

const requiredHttpOptions = [
  'path',
]

// Handler name is dynamically inserted and should not be part of exported types.
export const runOnHttp = (handlerName, fn, options = {}) => {
  if (typeof handlerName !== 'string') {
    throw new Error('runOnHttp must be used in confunction with the export keyword')
  }

  if (typeof fn !== 'function') {
    throw new Error('No function passed to runOnHttp')
  }

  for (const key of requiredHttpOptions) {
    if (!options[key]) {
      throw new Error(`Missing required option: ${key}`)
    }
  }

  for (const key of Object.keys(options)) {
    if (!validHttpOptions.includes(key)) {
      throw new Error(`Invalid option: ${key}`)
    }

    Deno.core.ops[`op_set_${key}_option`]('http', handlerName, options[key]);
  }

  return fn
}
