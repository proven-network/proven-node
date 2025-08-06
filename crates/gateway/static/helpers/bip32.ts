import * as bip39 from '@scure/bip39';
import { wordlist } from '@scure/bip39/wordlists/english.js';

export const generateMnemonic = (secretBytes: Uint8Array): string[] => {
  return bip39.entropyToMnemonic(secretBytes, wordlist).split(' ');
};

export const mnemonicToSeed = (mnemonic: string[]): Uint8Array => {
  if (mnemonic.length !== 24) {
    throw new Error('Invalid mnemonic length');
  }

  return bip39.mnemonicToEntropy(mnemonic.join(' '), wordlist);
};
