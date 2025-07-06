import {
  generateWindowId,
  ExecuteOutput,
  WhoAmIResult,
  ExecutionResult,
  ExecuteError,
  ParentToBridgeMessage,
  OpenModalMessage,
  CloseModalMessage,
  BundleManifest,
  QueuedHandler,
  ExecuteMessage,
} from '@proven-network/common';

export type ProvenSDK = {
  execute: (manifestIdOrScript: string, handler: string, args?: any[]) => Promise<ExecuteOutput>;
  whoAmI: () => Promise<WhoAmIResult>;
  isAuthenticated: () => Promise<boolean>;
  initConnectButton: (targetElement?: HTMLElement | string) => Promise<void>;
  registerManifest: (manifest: BundleManifest) => void;
  updateManifest: (manifest: BundleManifest) => void;
};

export type Logger = {
  debug: (message: string, data?: any) => void;
  log: (message: string, data?: any) => void;
  error: (message: string, data?: any) => void;
  info: (message: string, data?: any) => void;
  warn: (message: string, data?: any) => void;
};

export const ProvenSDK = (options: {
  logger?: Logger;
  authGatewayOrigin: string;
  applicationId: string;
}): ProvenSDK => {
  const { logger, authGatewayOrigin, applicationId } = options;

  // Build iframe URLs from well-known paths
  const bridgeIframeUrl = `${authGatewayOrigin}/app/${applicationId}/iframes/bridge.html`;
  const connectIframeUrl = `${authGatewayOrigin}/app/${applicationId}/iframes/connect.html`;
  const registerIframeUrl = `${authGatewayOrigin}/app/${applicationId}/iframes/register.html`;
  const rpcIframeUrl = `${authGatewayOrigin}/app/${applicationId}/iframes/rpc.html`;

  // Generate unique window ID for this SDK instance
  const windowId = generateWindowId();
  logger?.debug('SDK: Generated window ID:', windowId);

  let bridgeIframe: HTMLIFrameElement | null = null;
  let connectIframe: HTMLIFrameElement | null = null;
  let rpcIframe: HTMLIFrameElement | null = null;
  let modalIframe: HTMLIFrameElement | null = null;
  let modalOverlay: HTMLDivElement | null = null;
  let bridgeIframeReady = false;
  let rpcIframeReady = false;
  let connectIframeReady = false;
  let nonce = 0;
  const pendingCallbacks = new Map<
    number,
    { resolve: (data: any) => void; reject: (error: Error) => void }
  >();

  // Manifest management
  const manifests = new Map<string, BundleManifest>();
  const sentManifests = new Set<string>(); // Track which manifests have been sent to bridge

  /**
   * Register a manifest with the SDK
   */
  const registerManifest = (manifest: BundleManifest): void => {
    logger?.debug('SDK: Registering manifest', manifest.id);
    manifests.set(manifest.id, manifest);

    // Process any queued handlers waiting for this manifest
    processQueuedHandlers();
  };

  /**
   * Update an existing manifest (for hot-reload)
   */
  const updateManifest = (manifest: BundleManifest): void => {
    logger?.debug('SDK: Updating manifest', manifest.id);
    manifests.set(manifest.id, manifest);

    // Mark as not sent so it gets resent to bridge
    sentManifests.delete(manifest.id);
  };

  /**
   * Connect to the global handler queue and process queued calls
   */
  const connectHandlerQueue = (): void => {
    logger?.debug('SDK: Attempting to connect to handler queue...');

    // Get reference to the global queue
    const existingQueue = (window as any).__ProvenHandlerQueue__;
    logger?.debug('SDK: Found existing queue:', existingQueue);

    if (!existingQueue) {
      logger?.debug('SDK: Creating new global queue object with push method');
      // Create queue object directly with push method
      (window as any).__ProvenHandlerQueue__ = {
        push: (handler: QueuedHandler) => {
          logger?.debug('SDK: Executing handler directly:', handler);
          executeHandler(handler);
          return 0; // Not meaningful for object, but maintaining array-like interface
        },
      };
    } else if (Array.isArray(existingQueue)) {
      logger?.debug(`SDK: Processing ${existingQueue.length} existing queued calls...`);

      // Process existing queued calls
      while (existingQueue.length > 0) {
        const handler = existingQueue.shift();
        if (handler) {
          logger?.debug('SDK: Processing queued handler:', handler);
          executeHandler(handler);
        }
      }

      // Replace the push method to execute handlers directly
      existingQueue.push = (handler: QueuedHandler) => {
        logger?.debug('SDK: Executing handler directly:', handler);
        executeHandler(handler);
        return existingQueue.length;
      };
    } else {
      logger?.warn('SDK: Global queue exists but is not an array:', existingQueue);
      // Replace with queue object
      (window as any).__ProvenHandlerQueue__ = {
        push: (handler: QueuedHandler) => {
          logger?.debug('SDK: Executing handler directly:', handler);
          executeHandler(handler);
          return 0;
        },
      };
    }

    logger?.debug('SDK: Successfully connected to handler queue');
  };

  /**
   * Process queued handlers that are waiting for manifests
   */
  const processQueuedHandlers = (): void => {
    // This would be called when a new manifest is registered
    // For now, just connecting to the global queue is sufficient
  };

  /**
   * Execute a handler from the queue (simplified approach)
   */
  const executeHandler = async (handler: QueuedHandler): Promise<void> => {
    try {
      logger?.debug('SDK: Executing handler from queue', {
        manifestId: handler.manifestId,
        handler: handler.handler,
        args: handler.args,
        hasManifest: !!handler.manifest,
      });

      const executeMessage: ExecuteMessage = {
        type: 'execute',
        nonce: 0, // Will be set by sendMessage
        data: {
          manifestId: handler.manifestId,
          ...(handler.manifest && { manifest: handler.manifest }),
          handler: handler.handler, // This is now the complete handler specifier from bundler
          args: handler.args,
        },
      };

      const response = await sendMessage(executeMessage);

      // Process the response same as before
      let result: ExecutionResult;
      if (response && typeof response === 'object' && 'execution_result' in response) {
        result = response.execution_result as ExecutionResult;
      } else {
        result = response as ExecutionResult;
      }

      if ('Ok' in result) {
        const successResult = result.Ok;
        processExecuteLogs(successResult.logs);
        handler.resolve(successResult.output as ExecuteOutput);
      } else if ('Error' in result) {
        const errorResult = result.Error;
        processExecuteLogs(errorResult.logs);
        const jsError = createErrorFromExecuteError(errorResult);
        handler.reject(jsError);
      } else {
        handler.reject(new Error('Invalid execution result format'));
      }
    } catch (error) {
      handler.reject(error instanceof Error ? error : new Error(String(error)));
    }
  };

  const createModalOverlay = (): HTMLDivElement => {
    const overlay = document.createElement('div');
    overlay.style.cssText = `
      position: fixed;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      z-index: 10000;
    `;

    return overlay;
  };

  const openRegistrationModal = (): void => {
    if (modalIframe && modalOverlay) {
      // Modal already open
      return;
    }

    logger?.debug('SDK: Opening registration modal');

    // Create overlay
    modalOverlay = createModalOverlay();

    // Create modal iframe
    modalIframe = document.createElement('iframe');
    modalIframe.src = `${registerIframeUrl}?app=${applicationId}#window=${windowId}`;
    modalIframe.setAttribute('sandbox', 'allow-scripts allow-same-origin allow-forms');
    modalIframe.setAttribute('allow', 'publickey-credentials-create *;');

    // Style the modal iframe to fill the screen
    modalIframe.style.cssText = `
      width: 100%;
      height: 100%;
      border: none;
      background: transparent;
    `;

    modalOverlay.appendChild(modalIframe);
    document.body.appendChild(modalOverlay);

    // Send init message to modal after it loads
    modalIframe.onload = () => {
      setTimeout(() => {
        if (modalIframe && modalIframe.contentWindow) {
          modalIframe.contentWindow.postMessage(
            {
              type: 'init_registration',
            },
            '*'
          );
        }
      }, 100);
    };
  };

  const closeRegistrationModal = (): void => {
    logger?.debug('SDK: Closing registration modal');

    if (modalOverlay && modalOverlay.parentNode) {
      modalOverlay.parentNode.removeChild(modalOverlay);
    }

    modalIframe = null;
    modalOverlay = null;
  };

  const createBridgeIframe = (): Promise<void> => {
    return new Promise((resolve, reject) => {
      if (bridgeIframe && bridgeIframeReady) {
        resolve();
        return;
      }

      bridgeIframe = document.createElement('iframe');
      bridgeIframe.src = `${bridgeIframeUrl}?app=${applicationId}#window=${windowId}`;
      bridgeIframe.setAttribute('sandbox', 'allow-scripts allow-same-origin');
      bridgeIframe.setAttribute(
        'allow',
        'publickey-credentials-create *; publickey-credentials-get *'
      );

      // Hide the bridge iframe as it's only for communication
      bridgeIframe.style.cssText = `
        position: absolute;
        width: 1px;
        height: 1px;
        top: -1000px;
        left: -1000px;
        border: none;
        visibility: hidden;
      `;

      bridgeIframe.onload = () => {
        // Wait a bit for iframe to initialize
        setTimeout(() => {
          bridgeIframeReady = true;
          resolve();
        }, 100);
      };

      bridgeIframe.onerror = () => {
        reject(new Error('Failed to load bridge iframe'));
      };

      // Append bridge iframe to document body (hidden)
      document.body.appendChild(bridgeIframe);
    });
  };

  const createRpcIframe = (): Promise<void> => {
    return new Promise((resolve, reject) => {
      if (rpcIframe && rpcIframeReady) {
        resolve();
        return;
      }

      rpcIframe = document.createElement('iframe');
      rpcIframe.src = `${rpcIframeUrl}?app=${applicationId}#window=${windowId}`;
      rpcIframe.setAttribute('sandbox', 'allow-scripts allow-same-origin');

      // Hide the RPC iframe as it's only for communication
      rpcIframe.style.cssText = `
        position: absolute;
        width: 1px;
        height: 1px;
        top: -1000px;
        left: -1000px;
        border: none;
        visibility: hidden;
      `;

      rpcIframe.onload = () => {
        // Wait a bit for iframe to initialize
        setTimeout(() => {
          rpcIframeReady = true;
          resolve();
        }, 100);
      };

      rpcIframe.onerror = () => {
        reject(new Error('Failed to load RPC iframe'));
      };

      // Append RPC iframe to document body (hidden)
      document.body.appendChild(rpcIframe);
    });
  };

  const createConnectIframe = (targetElement?: HTMLElement | string): Promise<void> => {
    return new Promise((resolve, reject) => {
      if (connectIframe && connectIframeReady) {
        resolve();
        return;
      }

      connectIframe = document.createElement('iframe');
      connectIframe.src = `${connectIframeUrl}?app=${applicationId}#window=${windowId}`;
      connectIframe.setAttribute('sandbox', 'allow-scripts allow-same-origin');
      connectIframe.setAttribute('allow', 'publickey-credentials-get *');

      // Set iframe dimensions for the smart auth button
      connectIframe.style.width = '180px';
      connectIframe.style.height = '65px';
      connectIframe.style.border = 'none';
      connectIframe.style.background = 'transparent';

      connectIframe.onload = () => {
        // Wait a bit for iframe to initialize
        setTimeout(() => {
          connectIframeReady = true;
          resolve();
        }, 100);
      };

      connectIframe.onerror = () => {
        reject(new Error('Failed to load connect iframe'));
      };

      // Append iframe to target element or document body
      let target = document.body;
      if (targetElement) {
        if (typeof targetElement === 'string') {
          const element = document.querySelector(targetElement);
          if (element) {
            target = element as HTMLElement;
          }
        } else {
          target = targetElement;
        }
      }
      target.appendChild(connectIframe);
    });
  };

  const handleIframeMessage = (event: MessageEvent) => {
    // Handle messages from bridge iframe
    if (bridgeIframe && event.source === bridgeIframe.contentWindow) {
      const message = event.data;

      if (message.type === 'response') {
        // Handle API responses
        const callback = pendingCallbacks.get(message.nonce);
        if (callback) {
          pendingCallbacks.delete(message.nonce);

          if (message.success) {
            callback.resolve(message.data);
          } else {
            callback.reject(new Error(message.error || 'Unknown error'));
          }
        }
      } else if (message.type === 'open_registration_modal') {
        // Handle modal open requests from button iframe (via bridge)
        openRegistrationModal();
      } else if (message.type === 'close_registration_modal') {
        // Handle modal close requests from registration iframe (via bridge)
        closeRegistrationModal();
      }
      return;
    }

    // Handle messages from button iframe (for backwards compatibility during transition)
    if (connectIframe && event.source === connectIframe.contentWindow) {
      const message = event.data as OpenModalMessage;

      if (message.type === 'open_registration_modal') {
        openRegistrationModal();
      }
      return;
    }

    // Handle messages from modal iframe (for backwards compatibility during transition)
    if (modalIframe && event.source === modalIframe.contentWindow) {
      const message = event.data as CloseModalMessage;

      if (message.type === 'close_registration_modal') {
        closeRegistrationModal();
      }
      return;
    }
  };

  const sendMessage = async (message: ParentToBridgeMessage): Promise<any> => {
    // Wait for bridge iframe to be ready if it's not already
    if (!bridgeIframeReady) {
      await createBridgeIframe();
    }

    return new Promise((resolve, reject) => {
      const currentNonce = nonce++;
      message.nonce = currentNonce;

      pendingCallbacks.set(currentNonce, { resolve, reject });

      // Set timeout for the request
      setTimeout(() => {
        if (pendingCallbacks.has(currentNonce)) {
          pendingCallbacks.delete(currentNonce);
          reject(new Error('Request timeout'));
        }
      }, 30000); // 30 second timeout

      bridgeIframe!.contentWindow!.postMessage(message, '*');
    });
  };

  /**
   * Creates a proper JavaScript Error object from ExecuteError details.
   * This error can be thrown and will behave like a normal browser error.
   */
  const createErrorFromExecuteError = (executeError: ExecuteError): Error => {
    const error = new Error(executeError.error.message);
    error.name = executeError.error.name;

    // Set the stack trace if available
    if (executeError.error.stack) {
      error.stack = executeError.error.stack;
    }

    // Add duration as a custom property for debugging
    (error as any).executionDuration = executeError.duration;

    return error;
  };

  /**
   * Processes execution logs and outputs them to the console
   */
  const processExecuteLogs = (logs: any[]) => {
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
  };

  const execute = async (
    manifestIdOrScript: string,
    handler: string,
    args: any[] = []
  ): Promise<ExecuteOutput> => {
    logger?.debug('SDK: Executing handler', { manifestIdOrScript, handler, args });

    // Check if this is a manifest ID
    const manifest = manifests.get(manifestIdOrScript);

    let executeMessage: any;

    if (manifest) {
      // Manifest-based execution
      const shouldSendManifest = !sentManifests.has(manifest.id);

      // Log the manifest structure before sending
      if (shouldSendManifest) {
        logger?.debug('SDK: Sending manifest structure:', {
          id: manifest.id,
          version: manifest.version,
          modules: manifest.modules?.length || 0,
          dependencies: manifest.dependencies,
          metadata: manifest.metadata,
          fullManifest: manifest,
        });
      }

      executeMessage = {
        type: 'execute',
        nonce: 0, // Will be set by sendMessage
        data: {
          manifestId: manifest.id,
          manifest: shouldSendManifest ? manifest : undefined,
          handler,
          args,
        },
      };

      // Mark manifest as sent
      if (shouldSendManifest) {
        sentManifests.add(manifest.id);
      }
    } else {
      // Fallback to script-based execution (backward compatibility)
      executeMessage = {
        type: 'execute',
        nonce: 0,
        data: {
          manifestId: 'legacy-script',
          manifest: {
            id: 'legacy-script',
            version: '1.0.0',
            modules: [
              {
                path: 'script.js',
                content: manifestIdOrScript,
                handlers: [],
                dependencies: [],
              },
            ],
            entrypoints: [],
            dependencies: { production: {}, development: {}, all: {} },
            metadata: {
              createdAt: new Date().toISOString(),
              mode: 'development' as const,
              pluginVersion: '1.0.0',
              fileCount: 1,
              bundleSize: manifestIdOrScript.length,
              sourceMaps: false,
            },
          },
          handler,
          args,
        },
      };
    }

    const response = await sendMessage(executeMessage);

    // The response now contains either the full ExecutionResult (legacy) or ExecuteSuccessResponse (new)
    let result: ExecutionResult;

    // Check if this is the new format with code_package_hash
    if (response && typeof response === 'object' && 'execution_result' in response) {
      // New format: { execution_result: ExecutionResult, code_package_hash: string }
      result = response.execution_result as ExecutionResult;
      // The bridge already handles storing the hash mapping, so we don't need to do anything with it here
    } else {
      // Legacy format: direct ExecutionResult
      result = response as ExecutionResult;
    }

    if ('Ok' in result) {
      // Handle successful execution
      const successResult = result.Ok;
      processExecuteLogs(successResult.logs);
      return successResult.output as ExecuteOutput;
    } else if ('Error' in result) {
      // Handle runtime error - process logs then throw error
      const errorResult = result.Error;
      processExecuteLogs(errorResult.logs);

      // Create and throw a proper JavaScript Error
      const jsError = createErrorFromExecuteError(errorResult);
      throw jsError;
    } else {
      // This should never happen, but handle it gracefully
      throw new Error('Invalid execution result format');
    }
  };

  const whoAmI = async (): Promise<WhoAmIResult> => {
    logger?.debug('SDK: Getting identity');

    const response = await sendMessage({
      type: 'whoAmI',
      nonce: 0, // Will be set by sendMessage
    });

    return response;
  };

  const isAuthenticated = async (): Promise<boolean> => {
    try {
      const whoAmIResult = await whoAmI();
      return whoAmIResult.result === 'identified';
    } catch (error) {
      logger?.error('SDK: Error checking authentication status:', error);
      return false;
    }
  };

  const initConnectButton = async (targetElement?: HTMLElement | string): Promise<void> => {
    logger?.debug('SDK: Initializing button iframe');
    await createConnectIframe(targetElement);
  };

  // Initialize bridge and RPC iframes immediately
  Promise.all([
    createBridgeIframe().catch((error) => {
      logger?.error('Failed to initialize bridge iframe:', error);
    }),
    createRpcIframe().catch((error) => {
      logger?.error('Failed to initialize RPC iframe:', error);
    }),
  ]);

  // Listen for messages from iframes
  window.addEventListener('message', handleIframeMessage);

  // Handle ESC key to close modal
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && modalOverlay) {
      closeRegistrationModal();
    }
  });

  // Initialize manifest system
  const initializeManifestSystem = () => {
    // Process any pre-registered manifest
    const preRegisteredManifest = (window as any).__ProvenManifest__;
    if (preRegisteredManifest) {
      registerManifest(preRegisteredManifest);
    }

    // Connect to the handler queue
    connectHandlerQueue();
  };

  // Initialize after a short delay to allow for bundler initialization
  setTimeout(initializeManifestSystem, 10);

  return {
    execute,
    whoAmI,
    isAuthenticated,
    initConnectButton,
    registerManifest,
    updateManifest,
  };
};

// Make ProvenSDK available globally when bundled as IIFE
if (typeof window !== 'undefined') {
  (window as any).ProvenSDK = ProvenSDK;
}
