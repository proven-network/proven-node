/// <reference lib="DOM" />
import { MessageBroker, getWindowIdFromUrl } from '../../helpers/broker';

// Generic state operation types
interface StateGetRequest {
  type: 'get_state';
  key: string;
  tabId: string;
}

interface StateSetRequest {
  type: 'set_state';
  key: string;
  value: any;
  tabId: string;
}

interface StateGetResponse {
  type: 'state_value';
  key: string;
  value: any;
  exists: boolean;
  tabId: string;
}

interface StateSetResponse {
  type: 'state_set';
  key: string;
  value: any;
  tabId: string;
}

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

    this.initializeBroker();
    this.initializeStateWorker();
  }

  async initializeBroker() {
    try {
      await this.broker.connect();

      // Set up message handlers for generic state operations from bridge
      this.broker.on('get_state', async (message: any, respond) => {
        const request = message.data as StateGetRequest;
        console.debug('State: Received get_state request:', request.key);

        if (respond) {
          try {
            const result = await this.getStateFromWorker(request.key);
            const response: StateGetResponse = {
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
            const response: StateSetResponse = {
              type: 'state_set',
              key: request.key,
              value: result.value,
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

      console.debug('State: Broker initialized successfully');

      // Notify parent that state iframe is ready
      parent.postMessage(
        {
          type: 'iframe_ready',
          iframeType: 'state',
        },
        '*'
      );
    } catch (error) {
      console.error('State: Failed to initialize broker:', error);

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
        `State: Failed to initialize broker: ${error instanceof Error ? error.message : 'Unknown error'}`
      );
    }
  }

  async initializeStateWorker() {
    try {
      // Connect to state shared worker (no window ID - truly shared across tabs)
      this.stateWorker = new SharedWorker(`../workers/state-worker.js`);
      this.statePort = this.stateWorker.port;

      // Set up message handling
      this.statePort.onmessage = (event) => {
        this.handleStateWorkerMessage(event.data);
      };

      this.statePort.onmessageerror = (error) => {
        console.error('State: State worker connection error:', error);
        this.isStateWorkerReady = false;
      };

      // Start the port and send init message
      this.statePort.start();
      this.statePort.postMessage({
        type: 'init',
        tabId: this.tabId,
      });

      this.isStateWorkerReady = true;
      console.debug('State: State worker initialized successfully');
    } catch (error) {
      console.error('State: Failed to initialize state worker:', error);
      throw error;
    }
  }

  private handleStateWorkerMessage(message: any) {
    console.debug('State: Received message from state worker:', message);

    if (message.type === 'value' && message.key) {
      // Forward state update notifications to bridge via broker
      this.broker.send(
        'state_updated',
        {
          key: message.key,
          value: message.value,
        },
        'bridge'
      );
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
if (globalThis.addEventListener) {
  globalThis.addEventListener('DOMContentLoaded', StateClient.init);
} else {
  // Fallback for cases where DOMContentLoaded has already fired
  StateClient.init();
}
