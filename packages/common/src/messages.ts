// Type-safe postMessage communication between SDK and Bridge
// This file defines all message types that can be sent between parent (SDK) and child (Bridge) windows

// =============================================================================
// SDK -> Bridge Messages (Parent -> Child)
// =============================================================================

export interface SdkToBridgeWhoAmIMessage {
  type: 'whoAmI';
  nonce: number;
}

export interface SdkToBridgeExecuteMessage {
  type: 'execute';
  nonce: number;
  data: {
    manifestId: string;
    manifest?: any; // BundleManifest
    handler: string;
    args?: any[];
  };
}

export interface SdkToBridgeRequestAuthSignalMessage {
  type: 'request_auth_signal';
  nonce: number;
  signalKey: string;
}

export interface SdkToBridgeUpdateAuthSignalMessage {
  type: 'update_auth_signal';
  nonce: number;
  signalKey: string;
  signalValue: any;
}

export type SdkToBridgeMessage =
  | SdkToBridgeWhoAmIMessage
  | SdkToBridgeExecuteMessage
  | SdkToBridgeRequestAuthSignalMessage
  | SdkToBridgeUpdateAuthSignalMessage;

// =============================================================================
// Bridge -> SDK Messages (Child -> Parent)
// =============================================================================

export interface BridgeToSdkSuccessResponse {
  type: 'response';
  nonce: number;
  success: true;
  data: any;
}

export interface BridgeToSdkErrorResponse {
  type: 'response';
  nonce: number;
  success: false;
  error: string;
}

export interface BridgeToSdkAuthSignalUpdateMessage {
  type: 'auth_signal_update';
  signalKey: string;
  signalValue: any;
}

export interface BridgeToSdkOpenModalMessage {
  type: 'open_registration_modal';
}

export interface BridgeToSdkCloseModalMessage {
  type: 'close_registration_modal';
}

export interface BridgeToSdkIframeReadyMessage {
  type: 'iframe_ready';
  iframeType: 'bridge' | 'rpc' | 'connect' | 'state' | 'register';
}

export interface BridgeToSdkIframeErrorMessage {
  type: 'iframe_error';
  iframeType: 'bridge' | 'rpc' | 'connect' | 'state' | 'register';
  error: string;
}

export type BridgeToSdkResponse = BridgeToSdkSuccessResponse | BridgeToSdkErrorResponse;

export type BridgeToSdkMessage =
  | BridgeToSdkResponse
  | BridgeToSdkAuthSignalUpdateMessage
  | BridgeToSdkOpenModalMessage
  | BridgeToSdkCloseModalMessage
  | BridgeToSdkIframeReadyMessage
  | BridgeToSdkIframeErrorMessage;

// =============================================================================
// Type Guards for Runtime Type Checking
// =============================================================================

export function isSdkToBridgeMessage(data: unknown): data is SdkToBridgeMessage {
  if (typeof data !== 'object' || data === null) return false;

  const message = data as any;

  // Check for required base properties
  if (typeof message.type !== 'string' || typeof message.nonce !== 'number') {
    return false;
  }

  switch (message.type) {
    case 'whoAmI':
      return true;

    case 'execute':
      return (
        typeof message.data === 'object' &&
        message.data !== null &&
        typeof message.data.manifestId === 'string' &&
        typeof message.data.handler === 'string'
      );

    case 'request_auth_signal':
      return typeof message.signalKey === 'string';

    case 'update_auth_signal':
      return typeof message.signalKey === 'string' && 'signalValue' in message;

    default:
      return false;
  }
}

export function isBridgeToSdkMessage(data: unknown): data is BridgeToSdkMessage {
  if (typeof data !== 'object' || data === null) return false;

  const message = data as any;

  if (typeof message.type !== 'string') return false;

  switch (message.type) {
    case 'response':
      return (
        typeof message.nonce === 'number' &&
        typeof message.success === 'boolean' &&
        (message.success === true ? true : typeof message.error === 'string')
      );

    case 'auth_signal_update':
      return typeof message.signalKey === 'string' && 'signalValue' in message;

    case 'open_registration_modal':
    case 'close_registration_modal':
      return true;

    case 'iframe_ready':
    case 'iframe_error':
      return (
        typeof message.iframeType === 'string' &&
        ['bridge', 'rpc', 'connect', 'state', 'register'].includes(message.iframeType)
      );

    default:
      return false;
  }
}

// =============================================================================
// Specialized Type Guards for Specific Message Types
// =============================================================================

export function isWhoAmIMessage(message: SdkToBridgeMessage): message is SdkToBridgeWhoAmIMessage {
  return message.type === 'whoAmI';
}

export function isExecuteMessage(
  message: SdkToBridgeMessage
): message is SdkToBridgeExecuteMessage {
  return message.type === 'execute';
}

export function isRequestAuthSignalMessage(
  message: SdkToBridgeMessage
): message is SdkToBridgeRequestAuthSignalMessage {
  return message.type === 'request_auth_signal';
}

export function isUpdateAuthSignalMessage(
  message: SdkToBridgeMessage
): message is SdkToBridgeUpdateAuthSignalMessage {
  return message.type === 'update_auth_signal';
}

export function isSuccessResponse(
  message: BridgeToSdkMessage
): message is BridgeToSdkSuccessResponse {
  return message.type === 'response' && (message as any).success === true;
}

export function isErrorResponse(message: BridgeToSdkMessage): message is BridgeToSdkErrorResponse {
  return message.type === 'response' && (message as any).success === false;
}

export function isAuthSignalUpdateMessage(
  message: BridgeToSdkMessage
): message is BridgeToSdkAuthSignalUpdateMessage {
  return message.type === 'auth_signal_update';
}

export function isIframeReadyMessage(
  message: BridgeToSdkMessage
): message is BridgeToSdkIframeReadyMessage {
  return message.type === 'iframe_ready';
}

export function isIframeErrorMessage(
  message: BridgeToSdkMessage
): message is BridgeToSdkIframeErrorMessage {
  return message.type === 'iframe_error';
}

// =============================================================================
// Helper Functions for Creating Messages
// =============================================================================

export function createWhoAmIMessage(nonce: number): SdkToBridgeWhoAmIMessage {
  return { type: 'whoAmI', nonce };
}

export function createExecuteMessage(
  nonce: number,
  manifestId: string,
  handler: string,
  args?: any[],
  manifest?: any
): SdkToBridgeExecuteMessage {
  return {
    type: 'execute',
    nonce,
    data: {
      manifestId,
      handler,
      ...(args !== undefined && { args }),
      ...(manifest !== undefined && { manifest }),
    },
  };
}

export function createRequestAuthSignalMessage(
  nonce: number,
  signalKey: string
): SdkToBridgeRequestAuthSignalMessage {
  return { type: 'request_auth_signal', nonce, signalKey };
}

export function createUpdateAuthSignalMessage(
  nonce: number,
  signalKey: string,
  signalValue: any
): SdkToBridgeUpdateAuthSignalMessage {
  return { type: 'update_auth_signal', nonce, signalKey, signalValue };
}

export function createSuccessResponse(nonce: number, data: any): BridgeToSdkSuccessResponse {
  return { type: 'response', nonce, success: true, data };
}

export function createErrorResponse(nonce: number, error: string): BridgeToSdkErrorResponse {
  return { type: 'response', nonce, success: false, error };
}

export function createAuthSignalUpdateMessage(
  signalKey: string,
  signalValue: any
): BridgeToSdkAuthSignalUpdateMessage {
  return { type: 'auth_signal_update', signalKey, signalValue };
}

export function createOpenModalMessage(): BridgeToSdkOpenModalMessage {
  return { type: 'open_registration_modal' };
}

export function createCloseModalMessage(): BridgeToSdkCloseModalMessage {
  return { type: 'close_registration_modal' };
}

export function createIframeReadyMessage(
  iframeType: 'bridge' | 'rpc' | 'connect' | 'state' | 'register'
): BridgeToSdkIframeReadyMessage {
  return { type: 'iframe_ready', iframeType };
}

export function createIframeErrorMessage(
  iframeType: 'bridge' | 'rpc' | 'connect' | 'state' | 'register',
  error: string
): BridgeToSdkIframeErrorMessage {
  return { type: 'iframe_error', iframeType, error };
}

// =============================================================================
// PostMessage Utilities with Type Safety
// =============================================================================

/**
 * Type-safe postMessage sender for SDK -> Bridge communication
 */
export function sendSdkToBridgeMessage(
  iframe: HTMLIFrameElement,
  message: SdkToBridgeMessage,
  targetOrigin: string = '*'
): void {
  if (!iframe.contentWindow) {
    throw new Error('Iframe contentWindow is not available');
  }
  iframe.contentWindow.postMessage(message, targetOrigin);
}

/**
 * Type-safe postMessage sender for Bridge -> SDK communication
 */
export function sendBridgeToSdkMessage(
  message: BridgeToSdkMessage,
  targetOrigin: string = '*'
): void {
  if (!window.parent) {
    throw new Error('Parent window is not available');
  }
  window.parent.postMessage(message, targetOrigin);
}

/**
 * Type-safe message event listener for SDK (receiving from Bridge)
 */
export function addSdkMessageListener(
  callback: (message: BridgeToSdkMessage, event: MessageEvent) => void
): () => void {
  const handler = (event: MessageEvent) => {
    if (isBridgeToSdkMessage(event.data)) {
      callback(event.data, event);
    }
  };

  window.addEventListener('message', handler);

  // Return cleanup function
  return () => window.removeEventListener('message', handler);
}

/**
 * Type-safe message event listener for Bridge (receiving from SDK)
 */
export function addBridgeMessageListener(
  callback: (message: SdkToBridgeMessage, event: MessageEvent) => void
): () => void {
  const handler = (event: MessageEvent) => {
    // Only handle messages from parent window
    if (event.source === window.parent && isSdkToBridgeMessage(event.data)) {
      callback(event.data, event);
    }
  };

  window.addEventListener('message', handler);

  // Return cleanup function
  return () => window.removeEventListener('message', handler);
}

// =============================================================================
// Advanced Type-Safe Message Handling
// =============================================================================

/**
 * Promise-based type-safe request/response pattern for SDK
 */
export class TypeSafeSdkMessenger {
  private iframe: HTMLIFrameElement;
  private pendingRequests = new Map<
    number,
    {
      resolve: (response: BridgeToSdkSuccessResponse) => void;
      reject: (error: BridgeToSdkErrorResponse) => void;
    }
  >();
  private nonce = 0;
  private cleanup?: () => void;

  constructor(iframe: HTMLIFrameElement) {
    this.iframe = iframe;
    this.setupMessageListener();
  }

  private setupMessageListener() {
    this.cleanup = addSdkMessageListener((message) => {
      if (message.type === 'response') {
        const pending = this.pendingRequests.get(message.nonce);
        if (pending) {
          this.pendingRequests.delete(message.nonce);
          if (message.success) {
            pending.resolve(message as BridgeToSdkSuccessResponse);
          } else {
            pending.reject(message as BridgeToSdkErrorResponse);
          }
        }
      }
    });
  }

  async sendRequest(message: Omit<SdkToBridgeMessage, 'nonce'>): Promise<any> {
    const nonce = ++this.nonce;
    const fullMessage = { ...message, nonce } as SdkToBridgeMessage;

    return new Promise((resolve, reject) => {
      this.pendingRequests.set(nonce, { resolve, reject });

      // Set timeout
      setTimeout(() => {
        if (this.pendingRequests.has(nonce)) {
          this.pendingRequests.delete(nonce);
          reject(new Error(`Request timeout for nonce ${nonce}`));
        }
      }, 30000);

      sendSdkToBridgeMessage(this.iframe, fullMessage);
    });
  }

  destroy() {
    if (this.cleanup) {
      this.cleanup();
    }
    this.pendingRequests.clear();
  }
}

/**
 * Type-safe message handler for Bridge
 */
export class TypeSafeBridgeMessenger {
  private cleanup?: () => void;

  constructor(private messageHandler: (message: SdkToBridgeMessage) => Promise<any> | any) {
    this.setupMessageListener();
  }

  private setupMessageListener() {
    this.cleanup = addBridgeMessageListener(async (message) => {
      try {
        const result = await this.messageHandler(message);

        if ('nonce' in message) {
          // Send success response
          sendBridgeToSdkMessage(createSuccessResponse(message.nonce, result));
        }
      } catch (error) {
        if ('nonce' in message) {
          // Send error response
          const errorMessage = error instanceof Error ? error.message : 'Unknown error';
          sendBridgeToSdkMessage(createErrorResponse(message.nonce, errorMessage));
        }
      }
    });
  }

  sendMessage(message: BridgeToSdkMessage) {
    sendBridgeToSdkMessage(message);
  }

  destroy() {
    if (this.cleanup) {
      this.cleanup();
    }
  }
}
