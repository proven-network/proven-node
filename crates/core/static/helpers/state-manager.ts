import { MessageBroker } from './broker';
import { StateAccessor } from './accessors';

/**
 * StateManager provides a simple interface for getting and setting state
 * through the message broker system with full TypeScript generic support.
 * It uses StateAccessor internally for consistent iframe communication.
 */
export class StateManager {
  private stateAccessor: StateAccessor;

  constructor(broker: MessageBroker, windowId: string) {
    this.stateAccessor = new StateAccessor(broker, windowId);
  }

  /**
   * Get a state value with type safety
   * @param key The state key to retrieve
   * @returns Promise resolving to the typed value or undefined if not found
   */
  async get<T = any>(key: string): Promise<T | undefined> {
    return this.stateAccessor.get<T>(key);
  }

  /**
   * Set a state value with type safety
   * @param key The state key to set
   * @param value The typed value to store
   * @returns Promise resolving to true if successful, false otherwise
   */
  async set<T = any>(key: string, value: T): Promise<boolean> {
    return this.stateAccessor.set<T>(key, value);
  }

  /**
   * Get a state value with a default fallback
   * @param key The state key to retrieve
   * @param defaultValue The default value to return if key doesn't exist
   * @returns Promise resolving to the typed value or the default
   */
  async getWithDefault<T>(key: string, defaultValue: T): Promise<T> {
    const value = await this.get<T>(key);
    return value !== undefined ? value : defaultValue;
  }

  /**
   * Check if a state key exists
   * @param key The state key to check
   * @returns Promise resolving to true if the key exists
   */
  async has(key: string): Promise<boolean> {
    return this.stateAccessor.has(key);
  }

  /**
   * Delete a state key
   * @param key The state key to delete
   * @returns Promise resolving to true if successful
   */
  async delete(key: string): Promise<boolean> {
    return this.stateAccessor.delete(key);
  }

  /**
   * Get multiple state values at once
   * @param keys Array of state keys to retrieve
   * @returns Promise resolving to a record of key-value pairs
   */
  async getMany<T = any>(keys: string[]): Promise<Record<string, T | undefined>> {
    const results: Record<string, T | undefined> = {};

    // Execute all requests in parallel
    const promises = keys.map(async (key) => {
      const value = await this.get<T>(key);
      return { key, value };
    });

    const responses = await Promise.all(promises);

    for (const { key, value } of responses) {
      results[key] = value;
    }

    return results;
  }

  /**
   * Set multiple state values at once
   * @param values Record of key-value pairs to set
   * @returns Promise resolving to true if all operations succeeded
   */
  async setMany<T = any>(values: Record<string, T>): Promise<boolean> {
    // Execute all requests in parallel
    const promises = Object.entries(values).map(([key, value]) => this.set(key, value));

    const results = await Promise.all(promises);

    // Return true only if all operations succeeded
    return results.every((result) => result === true);
  }

  /**
   * Clear all state keys that start with a given prefix
   * @param prefix The prefix to match
   * @returns Promise resolving to the number of keys cleared
   */
  async clearPrefix(prefix: string): Promise<number> {
    // Note: This is a simple implementation that requires getting all keys first
    // In a real implementation, you might want to add a dedicated broker method
    // for prefix-based operations for better performance

    // For now, we can't efficiently get all keys, so this is a placeholder
    console.warn(
      `StateManager: clearPrefix('${prefix}') is not yet implemented - requires broker support`
    );
    return 0;
  }
}

// Type-safe helpers for common state patterns

/**
 * Creates a typed state accessor for a specific key
 * @param stateManager The state manager instance
 * @param key The state key
 * @returns Object with get/set methods for the specific key
 */
export function createStateAccessor<T>(stateManager: StateManager, key: string) {
  return {
    get: (): Promise<T | undefined> => stateManager.get<T>(key),
    set: (value: T): Promise<boolean> => stateManager.set<T>(key, value),
    getWithDefault: (defaultValue: T): Promise<T> =>
      stateManager.getWithDefault<T>(key, defaultValue),
    has: (): Promise<boolean> => stateManager.has(key),
    delete: (): Promise<boolean> => stateManager.delete(key),
  };
}

/**
 * Creates a typed state accessor for a specific key using StateAccessor directly
 * @param stateAccessor The state accessor instance
 * @param key The state key
 * @returns Object with get/set methods for the specific key
 */
export function createDirectStateAccessor<T>(stateAccessor: StateAccessor, key: string) {
  return {
    get: (): Promise<T | undefined> => stateAccessor.get<T>(key),
    set: (value: T): Promise<boolean> => stateAccessor.set<T>(key, value),
    getWithDefault: async (defaultValue: T): Promise<T> => {
      const value = await stateAccessor.get<T>(key);
      return value !== undefined ? value : defaultValue;
    },
    has: (): Promise<boolean> => stateAccessor.has(key),
    delete: (): Promise<boolean> => stateAccessor.delete(key),
  };
}

/**
 * Creates typed state accessors for authentication-related state
 * @param stateManager The state manager instance
 * @returns Object with typed accessors for auth state
 */
export function createAuthStateAccessors(stateManager: StateManager) {
  return {
    authState: createStateAccessor<'loading' | 'authenticated' | 'unauthenticated'>(
      stateManager,
      'auth_state'
    ),
    userInfo: createStateAccessor<any>(stateManager, 'auth_user_info'),
    isAuthenticated: createStateAccessor<boolean>(stateManager, 'auth_is_authenticated'),
  };
}
