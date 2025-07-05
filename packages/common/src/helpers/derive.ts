import { hkdf } from "@noble/hashes/hkdf";
import { sha256 } from "@noble/hashes/sha2";

type SubkeyType = ApplicationEncryptionKey;
type ApplicationEncryptionKey = {
  type: "application_encryption_key";
  applicationId: string;
};

// Process PRF results and derive keys
export const deriveSubkey = async (
  masterSecretBytes: Uint8Array,
  type: SubkeyType
): Promise<Uint8Array | null> => {
  const derivedKeyLengthBytes = 32; // For Ed25519 seed

  let path: string;
  switch (type.type) {
    case "application_encryption_key":
      path = `application_encryption_key/${type.applicationId}`;
      break;
    default:
      throw new Error(`Unknown subkey type: ${type.type}`);
  }

  const derivedSeed = hkdf(
    sha256, // Underlying hash function
    masterSecretBytes, // Master secret (from PRF)
    undefined, // Salt (optional)
    path, // Context-specific information
    derivedKeyLengthBytes // Output length (32 for Ed25519 seed)
  );

  return derivedSeed;
};
