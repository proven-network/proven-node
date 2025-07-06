# Reactive Authentication with Preact Signals

This example demonstrates how to integrate the Proven SDK's reactive authentication state using Preact Signals with a proxy-based architecture.

## Architecture Overview

### Signal Proxy Pattern

The implementation uses a sophisticated proxy pattern:

1. **State Worker as Signal Registry**: The SharedWorker maintains a centralized registry of signals by key
2. **Bridge Security Layer**: The bridge iframe enforces whitelisting of allowed signal keys
3. **Proxy Signals in SDK**: Local proxy signals automatically sync with master signals
4. **Cross-Tab Synchronization**: State changes are shared across all browser tabs

### Key Components

1. **State Worker** - Maintains master signals (`auth.state`, `auth.userInfo`, `auth.isAuthenticated`)
2. **Bridge** - Enforces security with whitelisted signal keys
3. **State Iframe** - Bridges worker and bridge communication
4. **SDK Signal Proxy** - Creates local proxies that sync with master signals

## Key Features

### 1. Reactive State Management

The SDK provides reactive signals for authentication state:

- `sdk.authState` - Signal containing 'loading' | 'authenticated' | 'unauthenticated'
- `sdk.userInfo` - Signal containing user information when authenticated
- `sdk.isAuthenticated` - Computed signal that derives authentication status

### 2. Automatic UI Updates

Using Preact Signals' `effect()`, the UI automatically updates when authentication state changes:

```typescript
import { effect } from '@preact/signals-core';

effect(() => {
  const authState = sdk.authState.value;
  const userInfo = sdk.userInfo.value;
  const isAuthenticated = sdk.isAuthenticated.value;

  // Update UI based on authentication state
  updateAuthUI(isAuthenticated, userInfo);

  // Load todos when authenticated
  if (isAuthenticated) {
    loadTodos().catch(console.error);
    updateStats().catch(console.error);
  }
});
```

### 3. Security-First Design

Signal access is controlled by a whitelist in the bridge:

- Only approved signal keys can be accessed by the SDK
- Internal iframe-to-iframe signals remain private
- Bridge acts as the security gateway

### 4. Cross-Tab Synchronization

Authentication state is synchronized across browser tabs:

- SharedWorker is truly shared (no window ID in URL)
- State changes in one tab instantly update all other tabs
- Single source of truth for all authentication state

### 5. Extensible Signal System

The system supports requesting custom signals:

```typescript
// Request additional signals by key (if whitelisted)
const customSignal = await sdk.requestSignal('app.theme', 'light');
customSignal.value = 'dark'; // Syncs across tabs
```

## Security Model

The bridge enforces strict whitelisting:

```typescript
const ALLOWED_SIGNAL_KEYS = new Set(['auth.state', 'auth.userInfo', 'auth.isAuthenticated']);
```

Only signals in this whitelist can be accessed by the SDK, ensuring internal state remains secure.

## Benefits

1. **Security** - Whitelisted signal access prevents unauthorized state access
2. **Performance** - No polling, only reactive updates
3. **Cross-Tab Sync** - Instant state synchronization across tabs
4. **Type Safety** - Full TypeScript support with proxy types
5. **Extensibility** - Easy to add new signal types
6. **Clean Architecture** - Clear separation of concerns

## Usage

The system maintains backward compatibility while providing the new reactive capabilities. Simply use the SDK signals as before, and they'll automatically sync across tabs through the proxy system.
