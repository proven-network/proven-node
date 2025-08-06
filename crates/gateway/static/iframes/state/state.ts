/// <reference lib="DOM" />
import { MessageBroker, getWindowIdFromUrl } from '../../helpers/broker';

import type { StateGetRequest, StateSetRequest, StateValueResponse } from '../../types';

class StateClient {
  applicationId: string;
  broker: MessageBroker;
  windowId: string;
  tabId: string;
  stateWorker: SharedWorker | null = null;
  statePort: MessagePort | null = null;
  isStateWorkerReady = false;

  constructor() {
    // Extract application ID from URL path
    this.applicationId = globalThis.location.pathname.split('/')[2] || 'unknown';

    // Extract window ID from URL fragment
    this.windowId = getWindowIdFromUrl() || 'unknown';

    // Generate unique tab ID
    this.tabId = `tab_${Date.now()}_${Math.random().toString(36).substring(2)}`;

    // Initialize broker synchronously - will throw if it fails
    this.broker = new MessageBroker(this.windowId, 'state');

    // Initialize everything in sequence to ensure worker is ready before reporting ready
    this.initialize();
  }

  async initialize() {
    try {
      // First, connect the broker
      await this.broker.connect();

      // Set up message handlers for generic state operations from bridge
      this.broker.on('get_state', async (message: any, respond) => {
        const request = message.data as StateGetRequest;
        console.debug('State: Received get_state request:', request.key);

        if (respond) {
          try {
            const result = await this.getStateFromWorker(request.key);
            const response: StateValueResponse = {
              type: 'state_value',
              key: request.key,
              value: result.value,
              exists: result.exists,
              tabId: request.tabId,
            };

            respond(response);
          } catch (error) {
            respond({
              success: false,
              error: error instanceof Error ? error.message : 'Unknown error',
            });
          }
        }
      });

      this.broker.on('set_state', async (message: any, respond) => {
        const request = message.data as StateSetRequest;
        console.debug('State: Received set_state request:', request.key, request.value);

        if (respond) {
          try {
            const result = await this.setStateInWorker(request.key, request.value);
            // For set operations, we return success response
            respond({
              success: true,
              data: result.value,
            });
          } catch (error) {
            respond({
              success: false,
              error: error instanceof Error ? error.message : 'Unknown error',
            });
          }
        }
      });

      console.debug('State: Broker initialized successfully');

      // Initialize state worker BEFORE reporting ready
      await this.initializeStateWorker();
      console.debug('State: State worker initialized successfully');

      // Only notify parent that state iframe is ready after everything is initialized
      parent.postMessage(
        {
          type: 'iframe_ready',
          iframeType: 'state',
        },
        '*'
      );
      console.debug('State: Sent iframe_ready message');
    } catch (error) {
      console.error('State: Failed to initialize:', error);

      // Notify parent of initialization error
      parent.postMessage(
        {
          type: 'iframe_error',
          iframeType: 'state',
          error: error instanceof Error ? error.message : 'Unknown error',
        },
        '*'
      );

      throw new Error(
        `State: Failed to initialize: ${error instanceof Error ? error.message : 'Unknown error'}`
      );
    }
  }

  async initializeStateWorker(retries = 3): Promise<void> {
    let lastError: any;

    for (let attempt = 1; attempt <= retries; attempt++) {
      try {
        console.debug(`State: Initializing state worker (attempt ${attempt}/${retries})...`);

        // Connect to state shared worker (no window ID - truly shared across tabs)
        console.debug('State: Creating SharedWorker for state-worker.js...');
        this.stateWorker = new SharedWorker(`../workers/state-worker.js`);

        // Add error handler for the worker itself
        this.stateWorker.onerror = (error) => {
          console.error('State: SharedWorker error:', error);
        };

        this.statePort = this.stateWorker.port;
        console.debug('State: SharedWorker created, port:', this.statePort);

        // Create a promise that resolves when worker confirms it's ready
        const workerReadyPromise = new Promise<void>((resolve, reject) => {
          const timeout = setTimeout(() => {
            reject(new Error('State worker initialization timeout'));
          }, 5000); // 5 second timeout

          const initHandler = (event: MessageEvent) => {
            console.debug('State: Received worker message during init:', event.data);
            if (event.data.type === 'init_confirmed') {
              console.debug('State: Worker confirmed initialization');
              clearTimeout(timeout);
              this.statePort!.removeEventListener('message', initHandler);
              resolve();
            }
          };

          this.statePort!.addEventListener('message', initHandler);
        });

        // Set up permanent message handling AFTER we get the init confirmation
        const permanentHandler = (event: MessageEvent) => {
          this.handleStateWorkerMessage(event.data);
        };

        this.statePort.onmessageerror = (error) => {
          console.error('State: State worker connection error:', error);
          this.isStateWorkerReady = false;
        };

        // Start the port and send init message
        this.statePort.start();
        console.debug('State: Port started, sending init message...');
        this.statePort.postMessage({
          type: 'init',
          tabId: this.tabId,
        });
        console.debug('State: Init message sent, waiting for confirmation...');

        // Wait for worker to confirm it's ready
        await workerReadyPromise;

        // Now set up the permanent message handler
        this.statePort.onmessage = permanentHandler;

        this.isStateWorkerReady = true;
        console.debug('State: State worker initialized and confirmed ready');
        return; // Success!
      } catch (error) {
        lastError = error;
        console.error(
          `State: Failed to initialize state worker (attempt ${attempt}/${retries}):`,
          error
        );

        // Clean up failed attempt
        this.stateWorker = null;
        this.statePort = null;
        this.isStateWorkerReady = false;

        if (attempt < retries) {
          // Wait before retrying with exponential backoff
          const delay = Math.min(1000 * Math.pow(2, attempt - 1), 5000);
          console.debug(`State: Retrying in ${delay}ms...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    // All retries failed
    throw lastError;
  }

  private handleStateWorkerMessage(message: any) {
    console.debug('State: Received message from state worker:', message);

    // Broadcast state update notifications to all iframes for both value queries and set operations
    if ((message.type === 'value' || message.type === 'set_complete') && message.key) {
      console.debug('State: Broadcasting state update to all iframes:', message.key, message.value);
      // Broadcast to all iframes by not specifying a target
      this.broker.broadcast('state_updated', {
        key: message.key,
        value: message.value,
      });
    }
  }

  private async getStateFromWorker(key: string): Promise<{ value: any; exists: boolean }> {
    return new Promise((resolve, reject) => {
      if (!this.statePort || !this.isStateWorkerReady) {
        reject(new Error('State worker not ready'));
        return;
      }

      const handleResponse = (event: MessageEvent) => {
        const response = event.data;
        if (response.type === 'value' && response.key === key) {
          this.statePort!.removeEventListener('message', handleResponse);
          resolve({
            value: response.value,
            exists: response.exists,
          });
        } else if (response.type === 'error') {
          this.statePort!.removeEventListener('message', handleResponse);
          reject(new Error(response.error || 'Unknown error'));
        }
      };

      this.statePort.addEventListener('message', handleResponse);

      this.statePort.postMessage({
        type: 'get',
        key,
        tabId: this.tabId,
      });

      // Timeout after 5 seconds
      setTimeout(() => {
        this.statePort!.removeEventListener('message', handleResponse);
        reject(new Error('Get state timeout'));
      }, 5000);
    });
  }

  private async setStateInWorker(
    key: string,
    value: any
  ): Promise<{ value: any; exists: boolean }> {
    return new Promise((resolve, reject) => {
      if (!this.statePort || !this.isStateWorkerReady) {
        reject(new Error('State worker not ready'));
        return;
      }

      const handleResponse = (event: MessageEvent) => {
        const response = event.data;
        if (response.type === 'set_complete' && response.key === key) {
          this.statePort!.removeEventListener('message', handleResponse);
          resolve({
            value: response.value,
            exists: true,
          });
        } else if (response.type === 'error') {
          this.statePort!.removeEventListener('message', handleResponse);
          reject(new Error(response.error || 'Unknown error'));
        }
      };

      this.statePort.addEventListener('message', handleResponse);

      this.statePort.postMessage({
        type: 'set',
        key,
        value,
        tabId: this.tabId,
      });

      // Timeout after 5 seconds
      setTimeout(() => {
        this.statePort!.removeEventListener('message', handleResponse);
        reject(new Error('Set state timeout'));
      }, 5000);
    });
  }

  // Initialize the client when the page loads
  static init() {
    const client = new StateClient();

    // NO direct message listening from parent window for security!
    // All communication goes through the broker from bridge

    // Make client available globally for debugging
    (globalThis as any).stateClient = client;
  }
}

// Initialize when the page loads
if (globalThis.document && globalThis.document.readyState === 'loading') {
  globalThis.addEventListener('DOMContentLoaded', StateClient.init);
} else {
  // DOM is already loaded or we're in a non-browser environment
  StateClient.init();
}
