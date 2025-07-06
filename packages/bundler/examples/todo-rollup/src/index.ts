// Main entry point for the todo app (Rollup version)
// This demonstrates how the rollup bundler discovers handlers across multiple modules
// Authentication is handled client-side via the SDK, not through handlers

export * from './todo-handlers';
export * from './types';
export * from './sdk-init';
export * from './app';

// Initialize the app when DOM is ready
import { initApp } from './app';
import { sdk } from './sdk-init';

// Make SDK and app available globally for the browser
(globalThis as any).ProvenTodoApp = { sdk, initApp };

// Auto-initialize if DOM is already loaded
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initApp);
} else {
  initApp();
}
