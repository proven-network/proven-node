import { signal, computed, effect, type Signal } from '@preact/signals-core';

// Local type for user info
export type UserInfo = {
  result: 'anonymous' | 'identified' | 'failure';
  data: any;
};

// Re-export signal types for external use
export type { Signal } from '@preact/signals-core';

// Authentication state types
export type AuthState = 'loading' | 'authenticated' | 'unauthenticated';

// Re-export the computed signal type
export type ComputedSignal<T> = ReturnType<typeof computed<T>>;

export interface AuthStateSignals {
  authState: Signal<AuthState>;
  userInfo: Signal<UserInfo | null>;
  isAuthenticated: ComputedSignal<boolean>;
}

// Signal manager for authentication state that communicates via bridge
export class AuthSignalManager {
  private readonly authStateSignal: Signal<AuthState>;
  private readonly userInfoSignal: Signal<UserInfo | null>;
  private readonly isAuthenticatedSignal: ComputedSignal<boolean>;
  private cleanupFunctions: Array<() => void> = [];
  private sendMessage?: (message: any) => Promise<any>;

  constructor() {
    // Initialize core signals
    this.authStateSignal = signal<AuthState>('loading');
    this.userInfoSignal = signal<UserInfo | null>(null);

    // Create computed signal for authentication status
    this.isAuthenticatedSignal = computed(() => {
      const state = this.authStateSignal.value;

      return state === 'authenticated';
    });
  }

  /**
   * Get auth state signals
   */
  getSignals(): AuthStateSignals {
    return {
      authState: this.authStateSignal,
      userInfo: this.userInfoSignal,
      isAuthenticated: this.isAuthenticatedSignal,
    };
  }

  /**
   * Initialize with sendMessage function for bridge communication
   */
  setSendMessage(sendMessage: (message: any) => Promise<any>): void {
    this.sendMessage = sendMessage;

    // Request initial auth state values from bridge
    this.requestInitialValues();
  }

  /**
   * Request initial auth signal values from bridge
   */
  private async requestInitialValues(): Promise<void> {
    if (!this.sendMessage) return;

    try {
      // Request auth state
      const authStateResponse = await this.sendMessage({
        type: 'request_auth_signal',
        signalKey: 'auth.state',
      });

      if (authStateResponse?.data?.value !== undefined) {
        this.authStateSignal.value = authStateResponse.data.value;
      }

      // Request user info
      const userInfoResponse = await this.sendMessage({
        type: 'request_auth_signal',
        signalKey: 'auth.userInfo',
      });

      if (userInfoResponse?.data?.value !== undefined) {
        this.userInfoSignal.value = userInfoResponse.data.value;
      }
    } catch (error) {
      console.warn('Failed to request initial auth values:', error);
    }
  }

  /**
   * Handle auth signal updates from bridge
   */
  handleAuthSignalUpdate(signalKey: string, value: any): void {
    console.debug('AuthSignalManager: Received update:', signalKey, value);

    switch (signalKey) {
      case 'auth.state':
        this.authStateSignal.value = value;
        break;
      case 'auth.userInfo':
        this.userInfoSignal.value = value;
        break;
      case 'auth.isAuthenticated':
        // This is computed, so we don't update it directly
        break;
      default:
        console.warn('Unknown auth signal key:', signalKey);
    }
  }

  /**
   * Create an effect that runs when authentication state changes
   */
  effect(fn: () => void): () => void {
    const cleanup = effect(fn);
    this.cleanupFunctions.push(cleanup);
    return cleanup;
  }

  /**
   * Clean up all resources
   */
  destroy(): void {
    this.cleanupFunctions.forEach((cleanup) => cleanup());
    this.cleanupFunctions.length = 0;
  }
}
