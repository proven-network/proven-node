import { base64UrlToUint8Array, uint8ArrayToBase64Url } from './uint8array';

// Fields that should be converted from Base64URL to ArrayBuffer
const BASE64URL_FIELDS = new Set([
  'publicKey.challenge',
  'publicKey.user.id',
  'publicKey.extensions.prf.eval.first',
]);

// Special handling for array fields that contain Base64URL data
const ARRAY_BASE64URL_FIELDS = new Set([
  'publicKey.allowCredentials',
  'publicKey.excludeCredentials',
]);

// Deeply converts objects with Base64URL strings to ArrayBuffers
function convertOptionsToBuffer(obj: any, path: string[] = []): any {
  if (typeof obj === 'string') {
    // Get the full path
    const fullPath = path.join('.');

    // Only convert if the full path matches one of our specified paths
    if (BASE64URL_FIELDS.has(fullPath)) {
      try {
        return base64UrlToUint8Array(obj);
      } catch (e) {
        console.warn(`Failed to convert ${fullPath} to ArrayBuffer:`, e);
        return obj;
      }
    }
    return obj;
  } else if (Array.isArray(obj)) {
    // Special handling for credential arrays
    const fullPath = path.join('.');
    if (ARRAY_BASE64URL_FIELDS.has(fullPath)) {
      // This is an array of credential descriptors
      return obj.map((item) => {
        if (item && typeof item === 'object' && item.id) {
          try {
            return {
              ...item,
              id: base64UrlToUint8Array(item.id),
            };
          } catch (e) {
            console.warn(`Failed to convert credential ID to ArrayBuffer:`, e);
            return item;
          }
        }
        return item;
      });
    } else {
      // Regular array processing
      return obj.map((item, index) => convertOptionsToBuffer(item, [...path, index.toString()]));
    }
  } else if (obj !== null && typeof obj === 'object') {
    const newObj: any = {};
    for (const key in obj) {
      if (Object.prototype.hasOwnProperty.call(obj, key)) {
        newObj[key] = convertOptionsToBuffer(obj[key], [...path, key]);
      }
    }
    return newObj;
  }
  return obj;
}

// Deeply converts objects with ArrayBuffers to Base64URL strings
function convertResultToBase64Url(obj: any): any {
  if (obj instanceof ArrayBuffer || obj instanceof Uint8Array) {
    return uint8ArrayToBase64Url(obj);
  } else if (Array.isArray(obj)) {
    return obj.map(convertResultToBase64Url);
  } else if (obj !== null && typeof obj === 'object') {
    const newObj: any = {};
    for (const key in obj) {
      if (Object.prototype.hasOwnProperty.call(obj, key)) {
        newObj[key] = convertResultToBase64Url(obj[key]);
      }
    }
    return newObj;
  }
  return obj;
}

// Authentication function - returns PRF result as Promise
export async function authenticate(): Promise<Uint8Array> {
  // Generate a random state parameter which will tie start and finish requests together
  const state = crypto.randomUUID();

  // Get challenge from server - uses start_discoverable_authentication
  const resp = await fetch(`/webauthn/auth/start?state=${state}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
  });

  if (!resp.ok) {
    throw new Error(`Authentication start failed: ${resp.status} ${resp.statusText}`);
  }

  const responseData = await resp.json();
  const options = convertOptionsToBuffer(responseData);

  // Use navigator.credentials.get with discoverable auth options
  const credential = await navigator.credentials.get({
    publicKey: options.publicKey,
    mediation: 'immediate' as CredentialMediationRequirement,
  });

  if (!credential) {
    throw new Error('No credential received from authenticator');
  }

  // Convert credential to JSON format for server
  const credentialJson = convertResultToBase64Url({
    id: credential.id,
    rawId: (credential as any).rawId,
    response: {
      authenticatorData: (credential as any).response.authenticatorData,
      clientDataJSON: (credential as any).response.clientDataJSON,
      signature: (credential as any).response.signature,
      userHandle: (credential as any).response.userHandle,
    },
    type: credential.type,
  });

  // Send credential to server
  const finishResp = await fetch(`/webauthn/auth/finish?state=${state}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(credentialJson),
  });

  if (!finishResp.ok) {
    throw new Error(`Authentication finish failed: ${finishResp.status} ${finishResp.statusText}`);
  }

  // Return PRF results if available
  const extensionResults = (credential as any).getClientExtensionResults?.();
  if (extensionResults?.prf?.results?.first) {
    const prfResult = new Uint8Array(extensionResults.prf.results.first);

    return prfResult;
  }

  throw new Error('PRF extension result not available');
}

// Registration function
export async function register(username: string): Promise<Uint8Array> {
  // Generate a random state parameter which will tie start and finish requests together
  const state = crypto.randomUUID();

  // Get challenge from server
  const resp = await fetch(`/webauthn/register/start?state=${state}`, {
    body: JSON.stringify({ user_name: username }),
    headers: { 'Content-Type': 'application/json' },
    method: 'POST',
  });

  if (!resp.ok) {
    throw new Error(`Registration start failed: ${resp.status} ${resp.statusText}`);
  }

  const responseData = await resp.json();
  const options = convertOptionsToBuffer(responseData);

  // Use navigator.credentials.create
  const credential = await navigator.credentials.create({
    publicKey: options.publicKey,
  });

  if (!credential) {
    throw new Error('No credential received from authenticator');
  }

  // Check if credential was created as discoverable (resident key)
  const extensionResults = (credential as any).getClientExtensionResults?.();

  // Convert credential to JSON format for server
  const credentialJson = convertResultToBase64Url({
    id: credential.id,
    rawId: (credential as any).rawId,
    response: {
      attestationObject: (credential as any).response.attestationObject,
      clientDataJSON: (credential as any).response.clientDataJSON,
    },
    type: credential.type,
  });

  // Send credential to server
  const finishResp = await fetch(`/webauthn/register/finish?state=${state}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(credentialJson),
  });

  if (!finishResp.ok) {
    throw new Error(`Registration finish failed: ${finishResp.status} ${finishResp.statusText}`);
  }

  // Return PRF results if available
  if (extensionResults?.prf?.results?.first) {
    const prfResult = new Uint8Array(extensionResults.prf.results.first);
    return prfResult;
  }

  throw new Error('PRF extension result not available');
}
