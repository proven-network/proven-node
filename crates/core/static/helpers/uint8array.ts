export function areEqualUint8Array(a: Uint8Array, b: Uint8Array) {
  if (a.length !== b.length) return false
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false
  }
  return true
}

export function uint8ArrayToHex(u8a: Uint8Array) {
  return Array.prototype.map
    .call(u8a, (x: number) => ('00' + x.toString(16)).slice(-2))
    .join('')
}

export function hexToUint8Array(hex: string) {
  const u8a = new Uint8Array(hex.length / 2)
  for (let i = 0; i < hex.length; i += 2) {
    u8a[i / 2] = parseInt(hex.slice(i, i + 2), 16)
  }
  return u8a
}

// Helper functions for Base64URL encoding/decoding
export function base64UrlToUint8Array(base64Url: string): Uint8Array {
  // Add padding if needed
  const padding = '='.repeat((4 - base64Url.length % 4) % 4);
  // Convert URL-safe characters back to standard Base64
  const base64 = (base64Url + padding)
    .replace(/-/g, '+')
    .replace(/_/g, '/');
  
  try {
    const raw = window.atob(base64);
    const buffer = new Uint8Array(raw.length);
    for (let i = 0; i < raw.length; i++) {
      buffer[i] = raw.charCodeAt(i);
    }
    return buffer;
  } catch (e) {
    console.error("Failed to decode Base64URL:", base64Url, e);
    throw e;
  }
}

export function uint8ArrayToBase64Url(buffer: ArrayBuffer): string {
  const base64 = window.btoa(String.fromCharCode(...new Uint8Array(buffer)));
  return base64.replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
}
