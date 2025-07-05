# Session Information for Proven Application Code

This package provides functions to access information about the active session inside Proven runtime.

## Installation

Install this package as a dev dependency:

```bash
npm install --save-dev @proven-network/session
```

or

```bash
yarn add -D @proven-network/session
```

## Usage

### Get Current Accounts

```typescript
import { getCurrentAccounts } from "@proven-network/session";

const accounts = getCurrentAccounts();
console.log(accounts);
```

### Get Current Identity

```typescript
import { getCurrentIdentity } from "@proven-network/session";

const identity = getCurrentIdentity();
console.log(identity);
```
