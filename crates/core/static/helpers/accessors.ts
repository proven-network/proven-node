import { MessageBroker } from './broker';
import type {
  WhoAmI,
  Execute,
  ExecuteHash,
  WhoAmIResult,
  ExecutionResult,
  ExecuteHashResult,
  RpcResponse,
  StateGetRequest,
  StateSetRequest,
} from '../types';

/**
 * Base class for iframe-specific broker accessors
 */
abstract class IframeAccessor {
  protected broker: MessageBroker;
  protected windowId: string;
  protected targetIframe: string;

  constructor(broker: MessageBroker, windowId: string, targetIframe: string) {
    this.broker = broker;
    this.windowId = windowId;
    this.targetIframe = targetIframe;
  }

  /**
   * Send a typed request to the target iframe
   */
  protected async request<TResponse = any, TRequest = any>(
    type: string,
    data: TRequest
  ): Promise<TResponse> {
    return this.broker.request<TResponse>(type, data, this.targetIframe);
  }
}

/**
 * Type-safe accessor for RPC iframe operations
 */
export class RpcAccessor extends IframeAccessor {
  constructor(broker: MessageBroker, windowId: string) {
    super(broker, windowId, 'rpc');
  }

  /**
   * Execute WhoAmI RPC call
   */
  async whoAmI(): Promise<WhoAmIResult> {
    const rpcCall: WhoAmI = { type: 'WhoAmI', data: null };

    const response = await this.request<{
      success: boolean;
      data?: RpcResponse<WhoAmIResult>;
      error?: string;
    }>('rpc_request', rpcCall);

    if (response.success && response.data?.type === 'WhoAmI') {
      return response.data.data;
    } else {
      throw new Error(response.error || 'WhoAmI request failed');
    }
  }

  /**
   * Execute a handler with full manifest
   */
  async execute(
    manifest: any,
    handlerSpecifier: string,
    args: any[] = []
  ): Promise<{ executionResult: ExecutionResult; codePackageHash?: string }> {
    const rpcCall: Execute = {
      type: 'Execute',
      data: {
        manifest,
        handler_specifier: handlerSpecifier,
        args,
      },
    };

    const response = await this.request<{
      success: boolean;
      data?: RpcResponse<any>;
      error?: string;
    }>('rpc_request', rpcCall);

    if (response.success && response.data?.type === 'Execute') {
      const executeResult = response.data.data;

      if (executeResult.result === 'success') {
        const successData = executeResult.data as any;

        // Handle both new format (with code_package_hash) and legacy format
        if (successData.execution_result && successData.code_package_hash) {
          return {
            executionResult: successData.execution_result,
            codePackageHash: successData.code_package_hash,
          };
        } else {
          return { executionResult: successData };
        }
      } else {
        throw new Error(executeResult.data as string);
      }
    } else {
      throw new Error(response.error || 'Execute request failed');
    }
  }

  /**
   * Execute a handler by hash
   */
  async executeHash(
    moduleHash: string,
    handlerSpecifier: string,
    args: any[] = []
  ): Promise<ExecutionResult | 'HashUnknown'> {
    const rpcCall: ExecuteHash = {
      type: 'ExecuteHash',
      data: {
        module_hash: moduleHash,
        handler_specifier: handlerSpecifier,
        args,
      },
    };

    const response = await this.request<{
      success: boolean;
      data?: RpcResponse<ExecuteHashResult>;
      error?: string;
    }>('rpc_request', rpcCall);

    if (response.success && response.data?.type === 'ExecuteHash') {
      const executeHashResult = response.data.data;

      if (executeHashResult.result === 'success') {
        return executeHashResult.data as ExecutionResult;
      } else if (executeHashResult.result === 'failure') {
        throw new Error(executeHashResult.data as string);
      } else {
        // result === 'error' means HashUnknown
        return 'HashUnknown';
      }
    } else {
      throw new Error(response.error || 'ExecuteHash request failed');
    }
  }
}

/**
 * Type-safe accessor for State iframe operations
 */
export class StateAccessor extends IframeAccessor {
  constructor(broker: MessageBroker, windowId: string) {
    super(broker, windowId, 'state');
  }

  /**
   * Get a state value with type safety
   */
  async get<T = any>(key: string): Promise<T | undefined> {
    const request: StateGetRequest = {
      key,
      tabId: this.windowId,
    };

    const response = await this.request<{
      success: boolean;
      data?: T;
      error?: string;
    }>('get_state', request);

    if (response.success) {
      return response.data;
    } else {
      console.warn(`StateAccessor: Failed to get state for key '${key}':`, response.error);
      return undefined;
    }
  }

  /**
   * Set a state value with type safety
   */
  async set<T = any>(key: string, value: T): Promise<boolean> {
    const request: StateSetRequest = {
      key,
      value,
      tabId: this.windowId,
    };

    const response = await this.request<{
      success: boolean;
      data?: any;
      error?: string;
    }>('set_state', request);

    if (response.success) {
      return true;
    } else {
      console.warn(`StateAccessor: Failed to set state for key '${key}':`, response.error);
      return false;
    }
  }

  /**
   * Check if a state key exists
   */
  async has(key: string): Promise<boolean> {
    const value = await this.get(key);
    return value !== undefined;
  }

  /**
   * Delete a state key
   */
  async delete(key: string): Promise<boolean> {
    return this.set(key, undefined);
  }
}

/**
 * Type-safe accessor for Connect iframe operations
 */
export class ConnectAccessor extends IframeAccessor {
  constructor(broker: MessageBroker, windowId: string) {
    super(broker, windowId, 'connect');
  }

  /**
   * Request authentication (this would trigger the connect flow)
   */
  async requestAuth(): Promise<void> {
    // This would send a message to the connect iframe to start auth flow
    // The exact implementation depends on how the connect iframe is designed
    await this.request('request_auth', { tabId: this.windowId });
  }

  /**
   * Check authentication status
   */
  async checkAuth(): Promise<boolean> {
    const response = await this.request<{
      success: boolean;
      authenticated: boolean;
    }>('check_auth', { tabId: this.windowId });

    return response.success && response.authenticated;
  }
}

/**
 * Factory class for creating all iframe accessors
 */
export class BrokerAccessorFactory {
  private broker: MessageBroker;
  private windowId: string;

  constructor(broker: MessageBroker, windowId: string) {
    this.broker = broker;
    this.windowId = windowId;
  }

  /**
   * Get a type-safe RPC accessor
   */
  rpc(): RpcAccessor {
    return new RpcAccessor(this.broker, this.windowId);
  }

  /**
   * Get a type-safe State accessor
   */
  state(): StateAccessor {
    return new StateAccessor(this.broker, this.windowId);
  }

  /**
   * Get a type-safe Connect accessor
   */
  connect(): ConnectAccessor {
    return new ConnectAccessor(this.broker, this.windowId);
  }
}

/**
 * Convenience function to create broker accessors
 */
export function createBrokerAccessors(
  broker: MessageBroker,
  windowId: string
): BrokerAccessorFactory {
  return new BrokerAccessorFactory(broker, windowId);
}

// Type definitions for better IDE support
export type BrokerAccessors = {
  rpc: RpcAccessor;
  state: StateAccessor;
  connect: ConnectAccessor;
};

/**
 * Create all accessor instances at once
 */
export function createAllAccessors(broker: MessageBroker, windowId: string): BrokerAccessors {
  const factory = new BrokerAccessorFactory(broker, windowId);
  return {
    rpc: factory.rpc(),
    state: factory.state(),
    connect: factory.connect(),
  };
}
