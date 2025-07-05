# @proven-network/sdk

TypeScript SDK for building applications on the Proven Network platform.

## Overview

This package provides a TypeScript SDK for interacting with the Proven Network platform from web applications. It handles authentication, script execution, and identity management.

## Installation

```bash
npm install @proven-network/sdk
```

## Usage

```typescript
import { ProvenSDK } from '@proven-network/sdk';

// Initialize the SDK
const sdk = ProvenSDK({
  authGatewayOrigin: 'https://auth.proven.network',
  applicationId: 'your-app-id',
  logger: console, // Optional logger
});

// Execute a script
const result = await sdk.execute('your-script', 'handlerFunction', [arg1, arg2]);

// Get user identity
const identity = await sdk.whoAmI();

// Initialize connect button
await sdk.initConnectButton('#connect-button');
```

## API Reference

### ProvenSDK

The main SDK class that provides methods for interacting with the Proven Network.

#### Constructor Options

- `authGatewayOrigin`: The origin URL of the auth gateway
- `applicationId`: Your application ID
- `logger`: Optional logger implementation

#### Methods

- `execute(script, handler, args)`: Execute a script with the given handler and arguments
- `whoAmI()`: Get the current user's identity information
- `initConnectButton(targetElement)`: Initialize the connect button in the specified element

## Development

```bash
# Build the package
npm run build

# Run tests
npm test

# Run linting
npm run lint
```
