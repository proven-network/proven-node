// Local SDK initialization
import { ProvenSDK } from '@proven-network/sdk';

// Initialize and export the SDK
export const sdk = ProvenSDK({
  logger: {
    debug: (...args: any[]) => console.debug('[SDK]', ...args),
    log: (...args: any[]) => console.log('[SDK]', ...args),
    info: (...args: any[]) => console.info('[SDK]', ...args),
    warn: (...args: any[]) => console.warn('[SDK]', ...args),
    error: (...args: any[]) => console.error('[SDK]', ...args),
  },
  authGatewayOrigin: 'http://localhost:3000',
  applicationId: globalThis.location?.hash.split('=')[1] || 'webpack-todo-example',
});
