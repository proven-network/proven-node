# Handler Types for Proven Application Code

This package allows customization of exported functions in Proven application code.

## Installation

Install this package as a dev dependency:

```bash
npm install --save-dev @proven-network/handler
```

or

```bash
yarn add -D @proven-network/handler
```

## Usage

### RPC Handlers

```typescript
import { run, runWithOptions } from '@proven-network/handler';

// RPC handler with no options
export const add = run((a: number, b: number) => {
  return a + b;
});

// RPC handler with options
export const subtract = runWithOptions(
  (a: number, b: number) => {
    return a - b;
  },
  {
    allowedOrigins: ['https://example.com'],
    timeout: 30000,
  }
);
```

### HTTP Handlers

```typescript
import { runOnHttp } from '@proven-network/handler';

// Simple HTTP handler with path parameters
export const getUser = runOnHttp(
  {
    path: '/users/:userId',
  },
  (request) => {
    const { userId } = request.pathVariables;
    return { id: userId, name: 'John Doe' };
  }
);

// HTTP handler with additional options
export const createUser = runOnHttp(
  {
    path: '/organizations/:orgId/users',
    timeout: 2000,
  },
  (request) => {
    const { orgId } = request.pathVariables;
    return { success: true };
  }
);
```
