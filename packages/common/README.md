# @proven-network/common

Common utilities and types for Proven Network applications.

## Overview

This package contains shared TypeScript utilities, types, and helper functions used across the Proven Network ecosystem.

## Contents

- **Common Types**: Shared type definitions for RPC commands and responses
- **Helper Functions**: Utility functions for cryptographic operations, session management, and more

## Installation

```bash
npm install @proven-network/common
```

## Usage

```typescript
import { WhoAmIResult, ExecuteOutput, generateWindowId } from '@proven-network/common';

// Use shared types and utilities
const result: WhoAmIResult = await someFunction();
const windowId = generateWindowId();
```

## Development

```bash
# Build the package
npm run build

# Run tests
npm test

# Run linting
npm run lint
``` 
