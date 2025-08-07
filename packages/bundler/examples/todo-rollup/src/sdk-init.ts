// Local SDK initialization
import { ProvenSDK } from '@proven-network/sdk';

// Helper function to parse URL fragment parameters
function parseFragmentParams(): { applicationId: string; port: string } {
  const hash = globalThis.location?.hash || '';
  const params = new URLSearchParams(hash.substring(1)); // Remove the # and parse

  return {
    applicationId: params.get('application_id') || 'rollup-todo-example',
    port: params.get('port') || '3000',
  };
}

// Parse fragment parameters
const { applicationId, port } = parseFragmentParams();

// Initialize and export the SDK
export const sdk = ProvenSDK({
  logger: {
    debug: (...args: any[]) => console.debug('[SDK]', ...args),
    log: (...args: any[]) => console.log('[SDK]', ...args),
    info: (...args: any[]) => console.info('[SDK]', ...args),
    warn: (...args: any[]) => console.warn('[SDK]', ...args),
    error: (...args: any[]) => console.error('[SDK]', ...args),
  },
  authGatewayOrigin: `http://localhost:${port}`,
  applicationId,
});
