/// <reference lib="DOM" />
import { MessageBroker, getWindowIdFromUrl } from '../../helpers/broker';
import { StateManager, createAuthStateAccessors } from '../../helpers/state-manager';
import { createAllAccessors, type BrokerAccessors } from '../../helpers/accessors';
import {
  BridgeRequest,
  BridgeResponse,
  ExecuteRequest,
  RequestAuthSignalRequest,
  UpdateAuthSignalRequest,
  WhoAmIRequest,
  SuccessResponse,
  ErrorResponse,
  // Type-safe postMessage imports
  SdkToBridgeMessage,
  TypeSafeBridgeMessenger,
  isWhoAmIMessage,
  isExecuteMessage,
  isRequestAuthSignalMessage,
  isUpdateAuthSignalMessage,
  createAuthSignalUpdateMessage,
  createOpenModalMessage,
  createCloseModalMessage,
  createIframeReadyMessage,
  createIframeErrorMessage,
} from '@proven-network/common';
import { bytesToHex } from '@noble/curves/abstract/utils';
import type { ExecuteLog, ExecutionResult, ExecuteResult, RpcResponse } from '../../types';

// Use the types from common package - no local types needed

// Whitelist of signals that can be accessed by SDK
const ALLOWED_SIGNAL_KEYS = new Set(['auth.state', 'auth.userInfo', 'auth.isAuthenticated']);

function isSignalKeyAllowed(key: string): boolean {
  return ALLOWED_SIGNAL_KEYS.has(key);
}

class BridgeClient {
  broker: MessageBroker;
  windowId: string;
  stateManager: StateManager;
  authState: ReturnType<typeof createAuthStateAccessors>;
  brokerAccessors: BrokerAccessors;
  typeSafeMessenger: TypeSafeBridgeMessenger;
  manifestCache: Map<string, any>; // Cache for manifests
  hashMapping: Map<string, string>; // Map from manifest hash to CodePackage hash

  constructor() {
    // Extract window ID from URL fragment
    this.windowId = getWindowIdFromUrl() || 'unknown';

    // Initialize broker synchronously - will throw if it fails
    this.broker = new MessageBroker(this.windowId, 'bridge');

    // Initialize state manager and broker accessors
    this.stateManager = new StateManager(this.broker, this.windowId);
    this.authState = createAuthStateAccessors(this.stateManager);
    this.brokerAccessors = createAllAccessors(this.broker, this.windowId);

    // Initialize type-safe messenger for postMessage communication
    this.typeSafeMessenger = new TypeSafeBridgeMessenger(this.handleTypedParentMessage.bind(this));

    // Initialize caches
    this.manifestCache = new Map();
    this.hashMapping = new Map();

    // Load hash mapping from sessionStorage
    this.loadHashMappingFromStorage();

    this.initializeBroker();
    this.setupParentListener();
  }

  async initializeBroker() {
    try {
      await this.broker.connect();

      // Set up message handlers for modal events
      this.broker.on('open_registration_modal', (_message) => {
        this.typeSafeMessenger.sendMessage(createOpenModalMessage());
      });

      this.broker.on('close_registration_modal', (_message) => {
        this.typeSafeMessenger.sendMessage(createCloseModalMessage());
      });

      // Set up state update listener for auth state changes
      this.broker.on('state_updated', (message) => {
        this.handleStateUpdate(message.data.key, message.data.value);
      });

      console.debug('Bridge: Broker initialized successfully');

      // Notify parent that bridge is ready using type-safe messaging
      this.typeSafeMessenger.sendMessage(createIframeReadyMessage('bridge'));
    } catch (error) {
      console.error('Bridge: Failed to initialize broker:', error);

      // Notify parent of initialization error using type-safe messaging
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      this.typeSafeMessenger.sendMessage(createIframeErrorMessage('bridge', errorMessage));

      throw new Error(
        `Bridge: Failed to initialize broker: ${error instanceof Error ? error.message : 'Unknown error'}`
      );
    }
  }

  setupParentListener() {
    // Note: Type-safe messaging is now handled by TypeSafeBridgeMessenger
    // This method is kept for compatibility but could be removed
    console.debug('Bridge: Parent listener setup (using TypeSafeBridgeMessenger)');
  }

  /**
   * Handle typed messages from parent SDK using the new type-safe system
   */
  async handleTypedParentMessage(message: SdkToBridgeMessage): Promise<any> {
    console.debug('Bridge: Received typed message from parent:', message);

    if (isWhoAmIMessage(message)) {
      return await this.handleTypedWhoAmI(message);
    } else if (isExecuteMessage(message)) {
      return await this.handleTypedExecute(message);
    } else if (isRequestAuthSignalMessage(message)) {
      return await this.handleTypedRequestAuthSignal(message);
    } else if (isUpdateAuthSignalMessage(message)) {
      return await this.handleTypedUpdateAuthSignal(message);
    } else {
      throw new Error(`Unknown message type: ${(message as any).type}`);
    }
  }

  /**
   * Typed WhoAmI handler - returns data directly (response creation handled by messenger)
   */
  async handleTypedWhoAmI(_message: SdkToBridgeMessage & { type: 'whoAmI' }): Promise<any> {
    return await this.brokerAccessors.rpc.whoAmI();
  }

  /**
   * Typed Execute handler - returns data directly
   */
  async handleTypedExecute(message: SdkToBridgeMessage & { type: 'execute' }): Promise<any> {
    const { manifestId, manifest, handler, args } = message.data;

    if (!manifestId || !handler) {
      throw new Error('ManifestId and handler are required for execute');
    }

    // Cache manifest if provided
    if (manifest) {
      console.debug('Bridge: Caching manifest', manifestId);
      this.manifestCache.set(manifestId, manifest);
    }

    // Get manifest from cache
    const cachedManifest = this.manifestCache.get(manifestId);
    if (!cachedManifest) {
      throw new Error(`Manifest ${manifestId} not found in cache`);
    }

    // Calculate manifest hash for hash mapping lookup
    const manifestHash = await this.hashManifest(cachedManifest);
    const storedCodePackageHash = this.getCodePackageHash(manifestHash);

    // Try ExecuteHash first if we have a stored CodePackage hash
    if (storedCodePackageHash) {
      console.debug('Bridge: Trying ExecuteHash with stored CodePackage hash');

      const executionResult = await this.brokerAccessors.rpc.executeHash(
        storedCodePackageHash,
        handler,
        args || []
      );

      if (executionResult === 'HashUnknown') {
        console.debug('Bridge: ExecuteHash returned HashUnknown, falling back to Execute');
      } else {
        // Success - return the ExecutionResult directly
        return executionResult;
      }
    } else {
      console.debug('Bridge: No stored CodePackage hash, using Execute directly');
    }

    // Fall back to full Execute
    return await this.executeTypedWithFullManifest(
      cachedManifest,
      handler,
      args || [],
      manifestHash
    );
  }

  /**
   * Execute with full manifest for typed handlers - returns result directly
   */
  async executeTypedWithFullManifest(
    manifest: any,
    handler: string,
    args: any[],
    manifestHash: string
  ): Promise<any> {
    const result = await this.brokerAccessors.rpc.execute(manifest, handler, args);

    // Store the hash mapping for future ExecuteHash calls if available
    if (result.codePackageHash) {
      this.storeHashMapping(manifestHash, result.codePackageHash);
      console.debug('Bridge: Stored hash mapping', {
        manifestHash,
        codePackageHash: result.codePackageHash,
      });
    }

    return result.executionResult;
  }

  /**
   * Typed auth signal request handler - returns data directly
   */
  async handleTypedRequestAuthSignal(
    message: SdkToBridgeMessage & { type: 'request_auth_signal' }
  ): Promise<any> {
    // Check if signal key is whitelisted
    if (!isSignalKeyAllowed(message.signalKey)) {
      throw new Error(`Auth signal key '${message.signalKey}' is not allowed`);
    }

    // Get state using the StateManager
    const stateKey = this.authSignalToStateKey(message.signalKey);
    const value = await this.stateManager.get(stateKey);

    return { value };
  }

  /**
   * Typed auth signal update handler - returns data directly
   */
  async handleTypedUpdateAuthSignal(
    message: SdkToBridgeMessage & { type: 'update_auth_signal' }
  ): Promise<any> {
    // Check if signal key is whitelisted
    if (!isSignalKeyAllowed(message.signalKey)) {
      throw new Error(`Auth signal key '${message.signalKey}' is not allowed`);
    }

    // Set state using the StateManager
    const stateKey = this.authSignalToStateKey(message.signalKey);
    const success = await this.stateManager.set(stateKey, message.signalValue);

    if (!success) {
      throw new Error('Failed to update state');
    }

    return null; // Success with no data
  }

  async handleParentMessage(message: BridgeRequest) {
    try {
      console.debug('Bridge: Received message from parent:', message);

      if (message.type === 'whoAmI') {
        await this.handleWhoAmI(message);
      } else if (message.type === 'execute') {
        await this.handleExecute(message);
      } else if (message.type === 'request_auth_signal') {
        await this.handleRequestAuthSignal(message);
      } else if (message.type === 'update_auth_signal') {
        await this.handleUpdateAuthSignal(message);
      } else {
        // This should never happen due to our type guard, but TypeScript requires it
        const exhaustiveCheck: never = message;
        throw new Error(`Unknown message type: ${(exhaustiveCheck as any).type}`);
      }
    } catch (error) {
      console.error('Bridge: Error handling parent message:', error);
      const errorResponse: ErrorResponse = {
        type: 'response',
        nonce: message.nonce,
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error',
      };
      this.forwardToParent(errorResponse);
    }
  }

  /**
   * Translate auth signal keys to generic state keys
   */
  private authSignalToStateKey(authKey: string): string {
    switch (authKey) {
      case 'auth.state':
        return 'auth_state';
      case 'auth.userInfo':
        return 'auth_user_info';
      case 'auth.isAuthenticated':
        return 'auth_is_authenticated';
      default:
        throw new Error(`Unknown auth signal key: ${authKey}`);
    }
  }

  /**
   * Translate generic state keys back to auth signal keys
   */
  private stateKeyToAuthSignal(stateKey: string): string {
    switch (stateKey) {
      case 'auth_state':
        return 'auth.state';
      case 'auth_user_info':
        return 'auth.userInfo';
      case 'auth_is_authenticated':
        return 'auth.isAuthenticated';
      default:
        throw new Error(`Unknown state key: ${stateKey}`);
    }
  }

  /**
   * Handle state updates from state iframe and forward relevant ones to SDK
   */
  private handleStateUpdate(stateKey: string, value: any): void {
    // Check if this is an auth-related state update
    if (stateKey.startsWith('auth_')) {
      try {
        const authSignalKey = this.stateKeyToAuthSignal(stateKey);

        // Forward auth state update to SDK using type-safe messaging
        this.typeSafeMessenger.sendMessage(createAuthSignalUpdateMessage(authSignalKey, value));
      } catch {
        console.warn('Bridge: Unknown auth state key:', stateKey);
      }
    }
  }

  async handleRequestAuthSignal(message: RequestAuthSignalRequest) {
    console.debug('Bridge: Handling auth signal request:', message.signalKey);

    // Check if signal key is whitelisted
    if (!isSignalKeyAllowed(message.signalKey)) {
      console.warn('Bridge: Auth signal key not allowed:', message.signalKey);
      const errorResponse: ErrorResponse = {
        type: 'response',
        nonce: message.nonce,
        success: false,
        error: `Auth signal key '${message.signalKey}' is not allowed`,
      };
      this.forwardToParent(errorResponse);
      return;
    }

    try {
      // Get state using the StateManager
      const stateKey = this.authSignalToStateKey(message.signalKey);
      const value = await this.stateManager.get(stateKey);

      const successResponse: SuccessResponse = {
        type: 'response',
        nonce: message.nonce,
        success: true,
        data: { value },
      };
      this.forwardToParent(successResponse);
    } catch (error) {
      console.error('Bridge: Error requesting signal:', error);
      const errorResponse: ErrorResponse = {
        type: 'response',
        nonce: message.nonce,
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error',
      };
      this.forwardToParent(errorResponse);
    }
  }

  async handleUpdateAuthSignal(message: UpdateAuthSignalRequest) {
    console.debug('Bridge: Handling auth signal update:', message.signalKey, message.signalValue);

    // Check if signal key is whitelisted
    if (!isSignalKeyAllowed(message.signalKey)) {
      console.warn('Bridge: Auth signal key not allowed:', message.signalKey);
      const errorResponse: ErrorResponse = {
        type: 'response',
        nonce: message.nonce,
        success: false,
        error: `Auth signal key '${message.signalKey}' is not allowed`,
      };
      this.forwardToParent(errorResponse);
      return;
    }

    try {
      // Set state using the StateManager
      const stateKey = this.authSignalToStateKey(message.signalKey);
      const success = await this.stateManager.set(stateKey, message.signalValue);

      if (success) {
        const successResponse: SuccessResponse = {
          type: 'response',
          nonce: message.nonce,
          success: true,
          data: null,
        };
        this.forwardToParent(successResponse);
      } else {
        const errorResponse: ErrorResponse = {
          type: 'response',
          nonce: message.nonce,
          success: false,
          error: 'Failed to update state',
        };
        this.forwardToParent(errorResponse);
      }
    } catch (error) {
      console.error('Bridge: Error updating signal:', error);
      const errorResponse: ErrorResponse = {
        type: 'response',
        nonce: message.nonce,
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error',
      };
      this.forwardToParent(errorResponse);
    }
  }

  async handleWhoAmI(message: WhoAmIRequest) {
    try {
      // Use the typed RPC accessor for cleaner code
      const whoAmIResult = await this.brokerAccessors.rpc.whoAmI();

      const successResponse: SuccessResponse = {
        type: 'response',
        nonce: message.nonce,
        success: true,
        data: whoAmIResult,
      };
      this.forwardToParent(successResponse);
    } catch (error) {
      const errorResponse: ErrorResponse = {
        type: 'response',
        nonce: message.nonce,
        success: false,
        error: error instanceof Error ? error.message : 'Failed to execute WhoAmI',
      };
      this.forwardToParent(errorResponse);
    }
  }

  async handleExecute(message: ExecuteRequest) {
    try {
      const { manifestId, manifest, handler, args } = message.data;

      if (!manifestId || !handler) {
        throw new Error('ManifestId and handler are required for execute');
      }

      // Cache manifest if provided
      if (manifest) {
        console.debug('Bridge: Caching manifest', manifestId);
        this.manifestCache.set(manifestId, manifest);
      }

      // Get manifest from cache
      const cachedManifest = this.manifestCache.get(manifestId);
      if (!cachedManifest) {
        throw new Error(`Manifest ${manifestId} not found in cache`);
      }

      // Calculate manifest hash for hash mapping lookup
      const manifestHash = await this.hashManifest(cachedManifest);
      const storedCodePackageHash = this.getCodePackageHash(manifestHash);

      // Try ExecuteHash first if we have a stored CodePackage hash
      if (storedCodePackageHash) {
        console.debug('Bridge: Trying ExecuteHash with stored CodePackage hash');

        try {
          const executionResult = await this.brokerAccessors.rpc.executeHash(
            storedCodePackageHash,
            handler,
            args || []
          );

          if (executionResult === 'HashUnknown') {
            console.debug('Bridge: ExecuteHash returned HashUnknown, falling back to Execute');
          } else {
            // Success - we got an ExecutionResult
            this.handleExecutionResult(executionResult, message.nonce);
            return;
          }
        } catch (error) {
          // ExecuteHash failed with an error
          const errorResponse: ErrorResponse = {
            type: 'response',
            nonce: message.nonce,
            success: false,
            error: error instanceof Error ? error.message : 'ExecuteHash failed',
          };
          this.forwardToParent(errorResponse);
          return;
        }
      } else {
        console.debug('Bridge: No stored CodePackage hash, using Execute directly');
      }

      // Fall back to full Execute
      await this.executeWithFullManifest(
        message,
        cachedManifest,
        handler,
        args || [],
        manifestHash
      );
    } catch (error) {
      const errorResponse: ErrorResponse = {
        type: 'response',
        nonce: message.nonce,
        success: false,
        error: error instanceof Error ? error.message : 'Failed to execute handler',
      };
      this.forwardToParent(errorResponse);
    }
  }

  async executeWithFullManifest(
    originalMessage: BridgeRequest,
    manifest: any,
    handler: string,
    args: any[],
    manifestHash: string
  ) {
    try {
      // Log the manifest structure being sent to RPC
      console.debug('Bridge: Sending manifest to RPC:', {
        id: manifest.id,
        version: manifest.version,
        modules: manifest.modules?.length || 0,
        dependencies: manifest.dependencies,
        metadata: manifest.metadata,
        fullManifest: manifest,
      });

      const fullRpcCall = {
        type: 'Execute',
        data: {
          manifest: manifest,
          handler_specifier: handler,
          args: args,
        },
      } as any;

      console.debug('Bridge: Full RPC call structure:', fullRpcCall);

      const response = await this.broker.request<{
        success: boolean;
        data?: RpcResponse<ExecuteResult>;
        error?: string;
      }>('rpc_request', fullRpcCall, 'rpc');

      if (!response.success) {
        throw new Error(response.error || 'Failed to execute full manifest');
      }

      if (!response.data || response.data.type !== 'Execute') {
        throw new Error('Invalid response format from Execute');
      }

      const executeResult = response.data.data;

      if (executeResult.result === 'success') {
        // Handle new response format with CodePackage hash
        const successData = executeResult.data as any;

        if (successData.execution_result && successData.code_package_hash) {
          // Store the hash mapping for future ExecuteHash calls
          this.storeHashMapping(manifestHash, successData.code_package_hash);
          console.debug('Bridge: Stored hash mapping', {
            manifestHash,
            codePackageHash: successData.code_package_hash,
          });

          // Forward the execution result
          this.handleExecutionResult(successData.execution_result, originalMessage.nonce);
        } else {
          // Fallback for legacy format
          this.handleExecutionResult(successData, originalMessage.nonce);
        }
      } else if (executeResult.result === 'failure' || executeResult.result === 'error') {
        const errorResponse: ErrorResponse = {
          type: 'response',
          nonce: originalMessage.nonce,
          success: false,
          error: executeResult.data as string,
        };
        this.forwardToParent(errorResponse);
      } else {
        const errorResponse: ErrorResponse = {
          type: 'response',
          nonce: originalMessage.nonce,
          success: false,
          error: 'Unexpected response format from Execute',
        };
        this.forwardToParent(errorResponse);
      }
    } catch (error) {
      const errorResponse: ErrorResponse = {
        type: 'response',
        nonce: originalMessage.nonce,
        success: false,
        error: error instanceof Error ? error.message : 'Failed to retry with full script',
      };
      this.forwardToParent(errorResponse);
    }
  }

  handleExecutionResult(result: ExecutionResult, nonce: number) {
    // Send the full ExecutionResult back to the SDK to handle
    const successResponse: SuccessResponse = {
      type: 'response',
      nonce: nonce,
      success: true,
      data: result,
    };
    this.forwardToParent(successResponse);
  }

  async hashManifest(manifest: any): Promise<string> {
    const manifestString = JSON.stringify(manifest);
    const rawHash = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(manifestString));
    return bytesToHex(new Uint8Array(rawHash));
  }

  /**
   * Load hash mapping from sessionStorage
   */
  loadHashMappingFromStorage(): void {
    try {
      const stored = sessionStorage.getItem('proven_hash_mapping');
      if (stored) {
        const parsed = JSON.parse(stored);
        this.hashMapping = new Map(Object.entries(parsed));
        console.debug('Bridge: Loaded hash mapping from storage', this.hashMapping.size, 'entries');
      }
    } catch (error) {
      console.warn('Bridge: Failed to load hash mapping from storage:', error);
    }
  }

  /**
   * Save hash mapping to sessionStorage
   */
  saveHashMappingToStorage(): void {
    try {
      const obj = Object.fromEntries(this.hashMapping);
      sessionStorage.setItem('proven_hash_mapping', JSON.stringify(obj));
      console.debug('Bridge: Saved hash mapping to storage', this.hashMapping.size, 'entries');
    } catch (error) {
      console.warn('Bridge: Failed to save hash mapping to storage:', error);
    }
  }

  /**
   * Store manifest hash to CodePackage hash mapping
   */
  storeHashMapping(manifestHash: string, codePackageHash: string): void {
    this.hashMapping.set(manifestHash, codePackageHash);
    this.saveHashMappingToStorage();
  }

  /**
   * Get CodePackage hash for a manifest hash
   */
  getCodePackageHash(manifestHash: string): string | undefined {
    return this.hashMapping.get(manifestHash);
  }

  processExecuteLogs(logs: ExecuteLog[]) {
    logs.forEach((log) => {
      if (log.level === 'log') {
        console.log(...log.args);
      } else if (log.level === 'error') {
        console.error(...log.args);
      } else if (log.level === 'warn') {
        console.warn(...log.args);
      } else if (log.level === 'debug') {
        console.debug(...log.args);
      } else if (log.level === 'info') {
        console.info(...log.args);
      }
    });
  }

  forwardToParent(message: BridgeResponse) {
    console.debug('Bridge: Forwarding to parent:', message);
    parent.postMessage(message, '*');
  }

  // Initialize the client when the page loads
  static init() {
    const client = new BridgeClient();

    // Make client available globally for debugging
    (globalThis as any).bridgeClient = client;
  }
}

// Initialize when the page loads
if (globalThis.addEventListener) {
  globalThis.addEventListener('DOMContentLoaded', BridgeClient.init);
} else {
  // Fallback for cases where DOMContentLoaded has already fired
  BridgeClient.init();
}
