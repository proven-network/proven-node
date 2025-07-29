/// <reference lib="DOM" />
import { authenticate } from '../../helpers/webauthn';
import { MessageBroker, getWindowIdFromUrl } from '../../helpers/broker';
import { createAllAccessors, type BrokerAccessors } from '../../helpers/accessors';
import { hexToBytes } from '@noble/curves/abstract/utils';
import type { Anonymize, Identify, AnonymizeResponse, IdentifyResponse } from '../../types';
import { signAsync, getPublicKeyAsync } from '@noble/ed25519';
import { getSession } from '../../helpers/sessions';

class ConnectClient {
  applicationId: string;
  broker: MessageBroker;
  windowId: string;
  brokerAccessors: BrokerAccessors;

  constructor() {
    // Extract application ID from URL path
    this.applicationId = globalThis.location.pathname.split('/')[2] || 'unknown';

    // Extract window ID from URL fragment
    this.windowId = getWindowIdFromUrl() || 'unknown';

    // Initialize broker synchronously - will throw if it fails
    this.broker = new MessageBroker(this.windowId, 'connect');

    // Initialize broker accessors
    this.brokerAccessors = createAllAccessors(this.broker, this.windowId);

    this.initializeBroker();
  }

  async initializeBroker() {
    try {
      await this.broker.connect();

      // Set up message handlers
      this.broker.on('registration_complete', async (message) => {
        // Handle successful registration
        if (message.data.prfResult) {
          await this.handleSuccessfulAuth(message.data.prfResult);
        }
      });

      // Listen for state updates from state iframe
      this.broker.on('state_updated', async (message) => {
        // Update UI when auth state changes
        if (message.data.key === 'auth_state') {
          // Use the value from the broadcast instead of fetching again
          const isAuthenticated = message.data.value === 'authenticated';
          await this.updateAuthUIWithValue(isAuthenticated);
        }
      });

      // Notify parent that connect iframe is ready
      parent.postMessage(
        {
          type: 'iframe_ready',
          iframeType: 'connect',
        },
        '*'
      );
    } catch (error) {
      console.error('Connect: Failed to initialize broker:', error);

      // Notify parent of initialization error
      parent.postMessage(
        {
          type: 'iframe_error',
          iframeType: 'connect',
          error: error instanceof Error ? error.message : 'Unknown error',
        },
        '*'
      );

      throw new Error(
        `Connect: Failed to initialize broker: ${error instanceof Error ? error.message : 'Unknown error'}`
      );
    }
  }

  async handleSuccessfulAuth(prfResult: Uint8Array) {
    // Send identify RPC request
    try {
      const session = await getSession(this.applicationId);

      if (!session) {
        throw new Error('Session not found');
      }

      const sessionId = hexToBytes(session.sessionId.replace(/-/g, ''));

      // Get the public key from the PRF result
      const publicKey = await getPublicKeyAsync(prfResult);

      // Sign the session ID with the PRF result
      const sessionIdSignature = await signAsync(sessionId, prfResult);

      const identifyRequest: Identify = {
        type: 'Identify',
        data: {
          passkey_prf_public_key_bytes: publicKey,
          session_id_signature_bytes: sessionIdSignature,
        },
      };

      const response = await this.broker.request<{
        success: boolean;
        data?: IdentifyResponse;
        error?: string;
      }>('rpc_request', identifyRequest, 'rpc');

      if (!response.success) {
        throw new Error(response.error || 'Identify request failed');
      }

      if (!response.data || response.data.type !== 'Identify') {
        throw new Error('Invalid response format from Identify');
      }

      const identifyResult = response.data.data;
      if (identifyResult.result !== 'success') {
        throw new Error(identifyResult.data as string);
      }

      // Only update UI after successful identify RPC
      await this.updateAuthUI();

      // Update auth state in the state system via broker
      await this.updateAuthStateInStateSystem(true);
    } catch (error) {
      console.error('Connect: Failed to send identify RPC request:', error);
    }
  }

  // Update auth state in the state system via broker
  async updateAuthStateInStateSystem(authenticated: boolean): Promise<void> {
    try {
      // Use the typed state accessor for safer state operations
      const authState = authenticated ? 'authenticated' : 'unauthenticated';
      const success = await this.brokerAccessors.state.set('auth_state', authState);

      if (!success) {
        console.warn('Connect: Failed to update auth state in state system');
      }
    } catch (error) {
      console.error('Connect: Failed to update auth state in state system:', error);
    }
  }

  // Auth state methods
  async isSignedIn(): Promise<boolean> {
    try {
      // Read auth state from state system instead of doing WhoAmI
      const authState = await this.brokerAccessors.state.get<string>('auth_state');

      // Check if the user is authenticated
      return authState === 'authenticated';
    } catch (error) {
      console.error('Connect: Failed to check auth status:', error);
      return false;
    }
  }

  async signOut(): Promise<void> {
    try {
      const anonymizeRequest: Anonymize = { type: 'Anonymize', data: null };
      const response = await this.broker.request<{
        success: boolean;
        data?: AnonymizeResponse;
        error?: string;
      }>('rpc_request', anonymizeRequest, 'rpc');

      if (!response.success) {
        throw new Error(response.error || 'Anonymize request failed');
      }

      if (!response.data || response.data.type !== 'Anonymize') {
        throw new Error('Invalid response format from Anonymize');
      }

      const anonymizeResult = response.data.data;
      if (anonymizeResult.result !== 'success') {
        throw new Error(anonymizeResult.data as string);
      }

      await this.updateAuthUI();

      // Update auth state in the state system via broker
      await this.updateAuthStateInStateSystem(false);
    } catch (error) {
      console.error('Connect: Failed to sign out:', error);
    }
  }

  async updateAuthUI() {
    const userSignedIn = await this.isSignedIn();
    this.updateAuthUIWithValue(userSignedIn);
  }

  updateAuthUIWithValue(isAuthenticated: boolean) {
    const signedOutView = document.getElementById('signed-out-view');
    const signedInView = document.getElementById('signed-in-view');
    const container = document.getElementById('auth-container');

    if (isAuthenticated) {
      signedOutView!.style.display = 'none';
      signedInView!.style.display = 'block';
    } else {
      signedOutView!.style.display = 'block';
      signedInView!.style.display = 'none';
      this.resetSignInButton();
    }

    // Show the container now that we've set the correct state
    container!.classList.add('ready');
  }

  resetSignInButton() {
    const button = document.getElementById('auth-button') as HTMLButtonElement;
    if (button) {
      button.textContent = 'Sign In';
      button.disabled = false;
    }
  }

  async handleSignIn() {
    const button = document.getElementById('auth-button') as HTMLButtonElement;
    button.disabled = true;
    button.textContent = 'Signing in...';

    try {
      const prfResult = await authenticate();
      await this.handleSuccessfulAuth(prfResult);
    } catch (error) {
      console.error('Authentication error:', error);

      // Check if this is a "no credentials" error
      const errorMessage = (error as Error).message?.toLowerCase() || '';
      const isNoCredentialsError =
        (error as Error).name === 'NotAllowedError' && errorMessage.includes('immediate');

      if (isNoCredentialsError) {
        // Send message directly to sdk via broker
        await this.broker.send('open_registration_modal', null, 'bridge');
        this.resetSignInButton();
      } else {
        // Other error (user cancelled, PRF not available, etc.)
        this.resetSignInButton();
      }
    }
  }

  // Initialize the client when the page loads
  static init() {
    const client = new ConnectClient();

    // Set up event listeners
    window.addEventListener('load', () => {
      const authButton = document.getElementById('auth-button');
      const signoutButton = document.getElementById('signout-button');

      if (authButton) {
        authButton.addEventListener('click', () => client.handleSignIn());
      }

      if (signoutButton) {
        signoutButton.addEventListener('click', () => client.signOut());
      }

      // Initialize UI state
      client.updateAuthUI().catch(console.error);
    });

    // Make client available globally for debugging
    (globalThis as any).buttonClient = client;
  }
}

// Initialize when the page loads
if (globalThis.document && globalThis.document.readyState === 'loading') {
  globalThis.addEventListener('DOMContentLoaded', ConnectClient.init);
} else {
  // DOM is already loaded or we're in a non-browser environment
  ConnectClient.init();
}
