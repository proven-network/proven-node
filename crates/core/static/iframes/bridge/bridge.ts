/// <reference lib="DOM" />
import {
  MessageBroker,
  getWindowIdFromUrl,
  ExecuteMessage,
  ParentToBridgeMessage,
  BridgeToParentMessage,
} from '@proven-network/common';
import { bytesToHex } from '@noble/curves/abstract/utils';
import type {
  WhoAmI,
  WhoAmIResult,
  ExecuteHash,
  ExecuteLog,
  ExecutionResult,
  ExecuteResult,
  ExecuteHashResult,
  RpcResponse,
} from '@proven-network/common';

// WhoAmIMessage is still local to the bridge
export type WhoAmIMessage = {
  type: 'whoAmI';
  nonce: number;
};

// Local type that combines imported ParentToBridgeMessage with local WhoAmIMessage
type LocalParentToBridgeMessage = ParentToBridgeMessage | WhoAmIMessage;

function isParentMessage(data: unknown): data is LocalParentToBridgeMessage {
  if (
    typeof data !== 'object' ||
    data === null ||
    !('type' in data) ||
    !('nonce' in data) ||
    typeof (data as any).nonce !== 'number'
  ) {
    return false;
  }

  const message = data as any;

  if (message.type === 'whoAmI') {
    return true;
  }

  if (message.type === 'execute') {
    return (
      'data' in message &&
      typeof message.data === 'object' &&
      message.data !== null &&
      typeof message.data.manifestId === 'string' &&
      typeof message.data.handler === 'string'
    );
  }

  return false;
}

class BridgeClient {
  broker: MessageBroker;
  windowId: string;
  manifestCache: Map<string, any>; // Cache for manifests
  hashMapping: Map<string, string>; // Map from manifest hash to CodePackage hash

  constructor() {
    // Extract window ID from URL fragment
    this.windowId = getWindowIdFromUrl() || 'unknown';

    // Initialize broker synchronously - will throw if it fails
    this.broker = new MessageBroker(this.windowId, 'sdk');

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
        this.forwardToParent({
          type: 'open_registration_modal',
        });
      });

      this.broker.on('close_registration_modal', (_message) => {
        this.forwardToParent({
          type: 'close_registration_modal',
        });
      });

      console.debug('Bridge: Broker initialized successfully');

      // Notify parent that bridge is ready
      this.forwardToParent({
        type: 'iframe_ready',
        iframeType: 'bridge',
      });
    } catch (error) {
      console.error('Bridge: Failed to initialize broker:', error);

      // Notify parent of initialization error
      this.forwardToParent({
        type: 'iframe_error',
        iframeType: 'bridge',
        error: error instanceof Error ? error.message : 'Unknown error',
      });

      throw new Error(
        `Bridge: Failed to initialize broker: ${error instanceof Error ? error.message : 'Unknown error'}`
      );
    }
  }

  setupParentListener() {
    // Listen for messages from parent SDK
    window.addEventListener('message', (event: MessageEvent) => {
      // Only handle messages from parent window
      if (event.source === parent && isParentMessage(event.data)) {
        this.handleParentMessage(event.data);
      }
    });
  }

  async handleParentMessage(message: ParentToBridgeMessage) {
    try {
      console.debug('Bridge: Received message from parent:', message);

      if (message.type === 'whoAmI') {
        await this.handleWhoAmI(message);
      } else if (message.type === 'execute') {
        await this.handleExecute(message);
      } else {
        // This should never happen due to our type guard, but TypeScript requires it
        const exhaustiveCheck: never = message;
        throw new Error(`Unknown message type: ${(exhaustiveCheck as any).type}`);
      }
    } catch (error) {
      console.error('Bridge: Error handling parent message:', error);
      this.forwardToParent({
        type: 'response',
        nonce: message.nonce,
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
    }
  }

  async handleWhoAmI(message: WhoAmIMessage) {
    try {
      const rpcCall: WhoAmI = { type: 'WhoAmI', data: null };

      const response = await this.broker.request<{
        success: boolean;
        data?: RpcResponse<WhoAmIResult>;
        error?: string;
      }>('rpc_request', rpcCall, 'rpc');

      if (response.success && response.data?.type === 'WhoAmI') {
        this.forwardToParent({
          type: 'response',
          nonce: message.nonce,
          success: true,
          data: response.data.data, // Extract the WhoAmIResult from the tagged response
        });
      } else {
        this.forwardToParent({
          type: 'response',
          nonce: message.nonce,
          success: false,
          error: response.error || 'WhoAmI response is missing or malformed',
        });
      }
    } catch (error) {
      this.forwardToParent({
        type: 'response',
        nonce: message.nonce,
        success: false,
        error: error instanceof Error ? error.message : 'Failed to execute WhoAmI',
      });
    }
  }

  async handleExecute(message: ExecuteMessage) {
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

        const hashRpcCall: ExecuteHash = {
          type: 'ExecuteHash',
          data: {
            args: args || [],
            handler_specifier: handler,
            module_hash: storedCodePackageHash,
          },
        };

        const hashResponse = await this.broker.request<{
          success: boolean;
          data?: RpcResponse<ExecuteHashResult>;
          error?: string;
        }>('rpc_request', hashRpcCall, 'rpc');

        if (hashResponse.success && hashResponse.data?.type === 'ExecuteHash') {
          const executeHashResult = hashResponse.data.data;

          if (executeHashResult.result === 'success') {
            const executionResult = executeHashResult.data as ExecutionResult;
            this.handleExecutionResult(executionResult, message.nonce);
            return;
          } else if (executeHashResult.result === 'failure') {
            this.forwardToParent({
              type: 'response',
              nonce: message.nonce,
              success: false,
              error: executeHashResult.data as string,
            });
            return;
          }
          // If result is "error" (HashUnknown), fall through to Execute
        }

        console.debug('Bridge: ExecuteHash failed or unknown, falling back to Execute');
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
      this.forwardToParent({
        type: 'response',
        nonce: message.nonce,
        success: false,
        error: error instanceof Error ? error.message : 'Failed to execute handler',
      });
    }
  }

  async executeWithFullManifest(
    originalMessage: LocalParentToBridgeMessage,
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
        this.forwardToParent({
          type: 'response',
          nonce: originalMessage.nonce,
          success: false,
          error: executeResult.data as string,
        });
      } else {
        this.forwardToParent({
          type: 'response',
          nonce: originalMessage.nonce,
          success: false,
          error: 'Unexpected response format from Execute',
        });
      }
    } catch (error) {
      this.forwardToParent({
        type: 'response',
        nonce: originalMessage.nonce,
        success: false,
        error: error instanceof Error ? error.message : 'Failed to retry with full script',
      });
    }
  }

  handleExecutionResult(result: ExecutionResult, nonce: number) {
    // Send the full ExecutionResult back to the SDK to handle
    this.forwardToParent({
      type: 'response',
      nonce: nonce,
      success: true,
      data: result,
    });
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

  forwardToParent(message: BridgeToParentMessage) {
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
