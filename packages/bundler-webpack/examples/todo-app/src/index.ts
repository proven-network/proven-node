// Main entry point for the todo app
// This demonstrates how the webpack bundler discovers handlers across multiple modules

export * from './todo-handlers';
export * from './auth-handlers';
export * from './types';
export * from './sdk-init';

// Make SDK available globally for the browser
import { sdk } from './sdk-init';
(globalThis as any).ProvenTodoApp = { sdk };