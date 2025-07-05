// Main entry point for the todo app (Rollup version)
// This demonstrates how the rollup bundler discovers handlers across multiple modules

export * from './todo-handlers';
export * from './auth-handlers';
export * from './types';
export * from './sdk-init';

// Make SDK available globally for the browser
import { sdk } from './sdk-init';
(globalThis as any).ProvenTodoApp = { sdk };