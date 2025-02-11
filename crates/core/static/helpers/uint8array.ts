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
