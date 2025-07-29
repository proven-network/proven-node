import {
  SdkExecutionResult,
  SdkExecuteError,
  BundleManifest,
  QueuedHandler,
  // Type-safe messaging imports
  TypeSafeSdkMessenger,
  SdkToBridgeMessage,
  BridgeToSdkMessage,
  addSdkMessageListener,
  isAuthSignalUpdateMessage,
  isSuccessResponse,
  isErrorResponse,
  isIframeReadyMessage,
  isIframeErrorMessage,
} from '@proven-network/common';
import { AuthSignalManager, type AuthStateSignals } from './signals';

// Local type definitions for SDK
export type ExecuteOutput = string | number | boolean | null | undefined;

export type ProvenSDK = {
  execute: (manifestIdOrScript: string, handler: string, args?: any[]) => Promise<ExecuteOutput>;
  checkAuthentication: () => Promise<boolean>;
  initConnectButton: (targetElement?: HTMLElement | string) => Promise<void>;
  registerManifest: (manifest: BundleManifest) => void;
  updateManifest: (manifest: BundleManifest) => void;
  destroy: () => void;
} & AuthStateSignals;

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

  // Initialize signal manager for reactive authentication state
  const authSignalManager = new AuthSignalManager();

  // Build iframe URLs from well-known paths
  const bridgeIframeUrl = `${authGatewayOrigin}/app/${applicationId}/iframes/bridge.html`;
  const connectIframeUrl = `${authGatewayOrigin}/app/${applicationId}/iframes/connect.html`;
  const registerIframeUrl = `${authGatewayOrigin}/app/${applicationId}/iframes/register.html`;
  const rpcIframeUrl = `${authGatewayOrigin}/app/${applicationId}/iframes/rpc.html`;
  const stateIframeUrl = `${authGatewayOrigin}/app/${applicationId}/iframes/state.html`;

  // Generate unique window ID for this SDK instance
  const windowId = crypto.randomUUID();

  let bridgeIframe: HTMLIFrameElement | null = null;
  let connectIframe: HTMLIFrameElement | null = null;
  let rpcIframe: HTMLIFrameElement | null = null;
  let stateIframe: HTMLIFrameElement | null = null;
  let modalIframe: HTMLIFrameElement | null = null;
  let modalOverlay: HTMLDivElement | null = null;
  let bridgeIframeReady = false;
  let rpcIframeReady = false;
  let connectIframeReady = false;
  let stateIframeReady = false;
  let typeSafeMessenger: TypeSafeSdkMessenger | null = null;
  let messageListenerCleanup: (() => void) | null = null;

  // Promises for iframe readiness
  let bridgeReadyPromise: Promise<void> | null = null;
  let rpcReadyPromise: Promise<void> | null = null;
  let connectReadyPromise: Promise<void> | null = null;
  let stateReadyPromise: Promise<void> | null = null;
  let bridgeReadyResolve: (() => void) | null = null;
  let rpcReadyResolve: (() => void) | null = null;
  let connectReadyResolve: (() => void) | null = null;
  let stateReadyResolve: (() => void) | null = null;
  const pendingCallbacks = new Map<
    number,
    { resolve: (data: any) => void; reject: (error: Error) => void }
  >();

  // Setup type-safe message listener
  const setupTypeSafeMessageListener = (): void => {
    if (messageListenerCleanup) {
      messageListenerCleanup();
    }

    messageListenerCleanup = addSdkMessageListener((message) => {
      handleTypeSafeMessage(message);
    });
  };

  const handleTypeSafeMessage = (message: BridgeToSdkMessage): void => {
    if (isIframeReadyMessage(message)) {
      handleIframeReady(message);
    } else if (isIframeErrorMessage(message)) {
      handleIframeError(message);
    } else if (isAuthSignalUpdateMessage(message)) {
      authSignalManager.handleAuthSignalUpdate(message.signalKey, message.signalValue);
    } else if (message.type === 'open_registration_modal') {
      openRegistrationModal();
    } else if (message.type === 'close_registration_modal') {
      closeRegistrationModal();
    } else if (isSuccessResponse(message) || isErrorResponse(message)) {
      // Response messages are handled by TypeSafeSdkMessenger
      // No need to handle them here
    }
  };

  const handleIframeReady = (message: BridgeToSdkMessage & { type: 'iframe_ready' }): void => {
    if (message.iframeType === 'bridge' && bridgeReadyResolve) {
      bridgeIframeReady = true;

      // Initialize type-safe messenger
      if (bridgeIframe) {
        typeSafeMessenger = new TypeSafeSdkMessenger(bridgeIframe);
        // Initialize signal manager with type-safe messenger
        authSignalManager.setSendMessage(sendTypeSafeMessage);
      }

      bridgeReadyResolve();
      bridgeReadyResolve = null;
    } else if (message.iframeType === 'rpc' && rpcReadyResolve) {
      rpcIframeReady = true;
      rpcReadyResolve();
      rpcReadyResolve = null;
    } else if (message.iframeType === 'connect' && connectReadyResolve) {
      connectIframeReady = true;
      connectReadyResolve();
      connectReadyResolve = null;
    } else if (message.iframeType === 'state' && stateReadyResolve) {
      stateIframeReady = true;
      stateReadyResolve();
      stateReadyResolve = null;
    }
  };

  const handleIframeError = (message: BridgeToSdkMessage & { type: 'iframe_error' }): void => {
    logger?.error('SDK: Iframe initialization error:', message);

    if (message.iframeType === 'bridge' && bridgeReadyResolve) {
      bridgeReadyPromise = null;
      bridgeReadyResolve = null;
    } else if (message.iframeType === 'rpc' && rpcReadyResolve) {
      rpcReadyPromise = null;
      rpcReadyResolve = null;
    } else if (message.iframeType === 'connect' && connectReadyResolve) {
      connectReadyPromise = null;
      connectReadyResolve = null;
    } else if (message.iframeType === 'state' && stateReadyResolve) {
      stateReadyPromise = null;
      stateReadyResolve = null;
    }
  };

  // Manifest management
  const manifests = new Map<string, BundleManifest>();
  const sentManifests = new Set<string>(); // Track which manifests have been sent to bridge

  /**
   * Register a manifest with the SDK
   */
  const registerManifest = (manifest: BundleManifest): void => {
    manifests.set(manifest.id, manifest);

    // Process any queued handlers waiting for this manifest
    processQueuedHandlers();
  };

  /**
   * Update an existing manifest (for hot-reload)
   */
  const updateManifest = (manifest: BundleManifest): void => {
    manifests.set(manifest.id, manifest);

    // Mark as not sent so it gets resent to bridge
    sentManifests.delete(manifest.id);
  };

  /**
   * Connect to the global handler queue and process queued calls
   */
  const connectHandlerQueue = (): void => {
    // Get reference to the global queue
    const existingQueue = (window as any).__ProvenHandlerQueue__;

    if (!existingQueue) {
      // Create queue object directly with push method
      (window as any).__ProvenHandlerQueue__ = {
        push: (handler: QueuedHandler) => {
          executeHandler(handler);
          return 0; // Not meaningful for object, but maintaining array-like interface
        },
      };
    } else if (Array.isArray(existingQueue)) {
      // Process existing queued calls
      while (existingQueue.length > 0) {
        const handler = existingQueue.shift();
        if (handler) {
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
          executeHandler(handler);
          return 0;
        },
      };
    }
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
      const executeMessage = {
        type: 'execute' as const,
        data: {
          manifestId: handler.manifestId,
          ...(handler.manifest && { manifest: handler.manifest }),
          handler: handler.handler, // This is now the complete handler specifier from bundler
          args: handler.args,
        },
      };

      const response = await sendTypeSafeMessage(executeMessage);

      // Process the response same as before
      let result: SdkExecutionResult;
      if (response && typeof response === 'object' && 'data' in response) {
        result = response.data as SdkExecutionResult;
      } else {
        result = response as SdkExecutionResult;
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
    if (modalOverlay && modalOverlay.parentNode) {
      modalOverlay.parentNode.removeChild(modalOverlay);
    }

    modalIframe = null;
    modalOverlay = null;
  };

  const createBridgeIframe = (): Promise<void> => {
    if (bridgeIframe && bridgeIframeReady) {
      return Promise.resolve();
    }

    if (bridgeReadyPromise) {
      return bridgeReadyPromise;
    }

    bridgeReadyPromise = new Promise((resolve, reject) => {
      bridgeReadyResolve = resolve;

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

      bridgeIframe.onerror = () => {
        bridgeReadyPromise = null;
        bridgeReadyResolve = null;
        reject(new Error('Failed to load bridge iframe'));
      };

      // Set timeout as backup
      setTimeout(() => {
        if (!bridgeIframeReady) {
          bridgeReadyPromise = null;
          bridgeReadyResolve = null;
          reject(new Error('Bridge iframe initialization timeout'));
        }
      }, 10000); // 10 second timeout

      // Append bridge iframe to document body (hidden)
      document.body.appendChild(bridgeIframe);
    });

    return bridgeReadyPromise;
  };

  const createRpcIframe = (): Promise<void> => {
    if (rpcIframe && rpcIframeReady) {
      return Promise.resolve();
    }

    if (rpcReadyPromise) {
      return rpcReadyPromise;
    }

    rpcReadyPromise = new Promise((resolve, reject) => {
      rpcReadyResolve = resolve;

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

      rpcIframe.onerror = () => {
        rpcReadyPromise = null;
        rpcReadyResolve = null;
        reject(new Error('Failed to load RPC iframe'));
      };

      // Set timeout as backup
      setTimeout(() => {
        if (!rpcIframeReady) {
          rpcReadyPromise = null;
          rpcReadyResolve = null;
          reject(new Error('RPC iframe initialization timeout'));
        }
      }, 10000); // 10 second timeout

      // Append RPC iframe to document body (hidden)
      document.body.appendChild(rpcIframe);
    });

    return rpcReadyPromise;
  };

  const createConnectIframe = (targetElement?: HTMLElement | string): Promise<void> => {
    if (connectIframe && connectIframeReady) {
      return Promise.resolve();
    }

    if (connectReadyPromise) {
      return connectReadyPromise;
    }

    connectReadyPromise = new Promise((resolve, reject) => {
      connectReadyResolve = resolve;

      connectIframe = document.createElement('iframe');
      connectIframe.src = `${connectIframeUrl}?app=${applicationId}#window=${windowId}`;
      connectIframe.setAttribute('sandbox', 'allow-scripts allow-same-origin');
      connectIframe.setAttribute('allow', 'publickey-credentials-get *');

      // Set iframe dimensions for the smart auth button
      connectIframe.style.width = '180px';
      connectIframe.style.height = '65px';
      connectIframe.style.border = 'none';
      connectIframe.style.background = 'transparent';

      connectIframe.onerror = () => {
        connectReadyPromise = null;
        connectReadyResolve = null;
        reject(new Error('Failed to load connect iframe'));
      };

      // Set timeout as backup
      setTimeout(() => {
        if (!connectIframeReady) {
          connectReadyPromise = null;
          connectReadyResolve = null;
          reject(new Error('Connect iframe initialization timeout'));
        }
      }, 10000); // 10 second timeout

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

    return connectReadyPromise;
  };

  const createStateIframe = (): Promise<void> => {
    if (stateIframe && stateIframeReady) {
      return Promise.resolve();
    }

    if (stateReadyPromise) {
      return stateReadyPromise;
    }

    stateReadyPromise = new Promise((resolve, reject) => {
      stateReadyResolve = resolve;

      stateIframe = document.createElement('iframe');
      stateIframe.src = `${stateIframeUrl}?app=${applicationId}#window=${windowId}`;
      stateIframe.setAttribute('sandbox', 'allow-scripts allow-same-origin');

      // Hide the state iframe as it's only for communication
      stateIframe.style.cssText = `
        position: absolute;
        width: 1px;
        height: 1px;
        top: -1000px;
        left: -1000px;
        border: none;
        visibility: hidden;
      `;

      stateIframe.onerror = () => {
        stateReadyPromise = null;
        stateReadyResolve = null;
        reject(new Error('Failed to load state iframe'));
      };

      // Set timeout as backup
      setTimeout(() => {
        if (!stateIframeReady) {
          stateReadyPromise = null;
          stateReadyResolve = null;
          reject(new Error('State iframe initialization timeout'));
        }
      }, 10000); // 10 second timeout

      // Append state iframe to document body (hidden)
      document.body.appendChild(stateIframe);
    });

    return stateReadyPromise;
  };

  const sendTypeSafeMessage = async (message: Omit<SdkToBridgeMessage, 'nonce'>): Promise<any> => {
    // Wait for bridge iframe to be ready if it's not already
    if (!bridgeIframeReady) {
      await createBridgeIframe();
    }

    if (!typeSafeMessenger) {
      throw new Error('Type-safe messenger not initialized');
    }

    return typeSafeMessenger.sendRequest(message);
  };

  /**
   * Creates a proper JavaScript Error object from ExecuteError details.
   * This error can be thrown and will behave like a normal browser error.
   */
  const createErrorFromExecuteError = (executeError: SdkExecuteError): Error => {
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
    // Check if this is a manifest ID
    const manifest = manifests.get(manifestIdOrScript);

    let executeMessage: any;

    if (manifest) {
      // Manifest-based execution
      const shouldSendManifest = !sentManifests.has(manifest.id);

      executeMessage = {
        type: 'execute' as const,
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
    }

    const response = await sendTypeSafeMessage(executeMessage);

    // The response now contains either the full SdkExecutionResult (legacy) or ExecuteSuccessResponse (new)
    let result: SdkExecutionResult;

    // Check if this is the new format with response envelope
    if (response && typeof response === 'object' && 'data' in response) {
      // New format: { type: "response", nonce: X, success: true, data: SdkExecutionResult }
      result = response.data as SdkExecutionResult;
    } else {
      // Legacy format: direct SdkExecutionResult
      result = response as SdkExecutionResult;
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

  const checkAuthentication = async (): Promise<boolean> => {
    try {
      // Use the reactive signal to check authentication state
      const signals = authSignalManager.getSignals();
      return signals.isAuthenticated.value;
    } catch (error) {
      logger?.error('SDK: Error checking authentication status:', error);
      return false;
    }
  };

  const initConnectButton = async (targetElement?: HTMLElement | string): Promise<void> => {
    await createConnectIframe(targetElement);
  };

  // Initialization is now handled by initializeManifestSystem() to ensure proper coordination

  // Setup type-safe message listener
  setupTypeSafeMessageListener();

  // Handle ESC key to close modal
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && modalOverlay) {
      closeRegistrationModal();
    }
  });

  // Initialize manifest system
  const initializeManifestSystem = async () => {
    try {
      // Initialize iframes in order of dependencies:
      // 1. State iframe (no dependencies)
      // 2. RPC iframe (no dependencies on other iframes)
      // 3. Bridge iframe (depends on state iframe)
      logger?.debug('SDK: Initializing state iframe...');
      await createStateIframe();

      logger?.debug('SDK: Initializing RPC iframe...');
      await createRpcIframe();

      logger?.debug('SDK: Initializing bridge iframe...');
      await createBridgeIframe();

      // Process any pre-registered manifest
      const preRegisteredManifest = (window as any).__ProvenManifest__;
      if (preRegisteredManifest) {
        registerManifest(preRegisteredManifest);
      }

      // Connect to the handler queue
      connectHandlerQueue();
    } catch (error) {
      logger?.error('SDK: Failed to initialize manifest system:', error);
    }
  };

  // Initialize after a short delay to allow for bundler initialization, then wait for iframes
  setTimeout(() => {
    initializeManifestSystem().catch((error) => {
      logger?.error('SDK: Manifest system initialization failed:', error);
    });
  }, 10);

  /**
   * Destroy the SDK and clean up all resources
   */
  const destroy = (): void => {
    // Clean up signal manager
    authSignalManager.destroy();

    // Clean up type-safe messenger
    if (typeSafeMessenger) {
      typeSafeMessenger.destroy();
      typeSafeMessenger = null;
    }

    // Clean up message listener
    if (messageListenerCleanup) {
      messageListenerCleanup();
      messageListenerCleanup = null;
    }

    // Clean up iframes
    if (bridgeIframe && bridgeIframe.parentNode) {
      bridgeIframe.parentNode.removeChild(bridgeIframe);
      bridgeIframe = null;
    }
    if (rpcIframe && rpcIframe.parentNode) {
      rpcIframe.parentNode.removeChild(rpcIframe);
      rpcIframe = null;
    }
    if (stateIframe && stateIframe.parentNode) {
      stateIframe.parentNode.removeChild(stateIframe);
      stateIframe = null;
    }
    if (connectIframe && connectIframe.parentNode) {
      connectIframe.parentNode.removeChild(connectIframe);
      connectIframe = null;
    }
    if (modalOverlay && modalOverlay.parentNode) {
      modalOverlay.parentNode.removeChild(modalOverlay);
      modalOverlay = null;
      modalIframe = null;
    }

    // Clear pending callbacks
    pendingCallbacks.clear();
  };

  // Get the signals object for destructuring
  const signals = authSignalManager.getSignals();

  return {
    execute,
    checkAuthentication,
    initConnectButton,
    registerManifest,
    updateManifest,
    destroy,
    // Expose the actual signal objects directly
    authState: signals.authState,
    userInfo: signals.userInfo,
    isAuthenticated: signals.isAuthenticated,
  };
};

// Make ProvenSDK available globally when bundled as IIFE
if (typeof window !== 'undefined') {
  (window as any).ProvenSDK = ProvenSDK;
}
