import { PrivateKey } from '@proven-network/crypto';
export type BytesStore = {
  get(key: string): Promise<Uint8Array | undefined>;
  keys(): Promise<string[]>;
  set(key: string, value: Uint8Array): Promise<void>;
};
export type KeyStore = {
  get(key: string): Promise<PrivateKey | undefined>;
  keys(): Promise<string[]>;
  set(key: string, value: PrivateKey): Promise<void>;
};
export type StringStore = {
  get(key: string): Promise<string | undefined>;
  keys(): Promise<string[]>;
  set(key: string, value: string): Promise<void>;
};
export type NftScopedBytesStore = {
  get(
    nftResourceAddress: string,
    nftId: string | number | Uint8Array,
    key: string
  ): Promise<Uint8Array | undefined>;
  keys(nftResourceAddress: string, nftId: string | number | Uint8Array): Promise<string[]>;
  set(
    nftResourceAddress: string,
    nftId: string | number | Uint8Array,
    key: string,
    value: Uint8Array
  ): Promise<void>;
};
export type NftScopedKeyStore = {
  get(
    nftResourceAddress: string,
    nftId: string | number | Uint8Array,
    key: string
  ): Promise<PrivateKey | undefined>;
  keys(nftResourceAddress: string, nftId: string | number | Uint8Array): Promise<string[]>;
  set(
    nftResourceAddress: string,
    nftId: string | number | Uint8Array,
    key: string,
    value: PrivateKey
  ): Promise<void>;
};
export type NftScopedStringStore = {
  get(
    nftResourceAddress: string,
    nftId: string | number | Uint8Array,
    key: string
  ): Promise<string | undefined>;
  keys(nftResourceAddress: string, nftId: string | number | Uint8Array): Promise<string[]>;
  set(
    nftResourceAddress: string,
    nftId: string | number | Uint8Array,
    key: string,
    value: string
  ): Promise<void>;
};
export declare function getApplicationBytesStore(storeName: string): BytesStore;
export declare function getApplicationKeyStore(storeName: string): KeyStore;
export declare function getApplicationStore(storeName: string): StringStore;
export declare function getPersonalBytesStore(storeName: string): BytesStore;
export declare function getPersonalKeyStore(storeName: string): KeyStore;
export declare function getPersonalStore(storeName: string): StringStore;
export declare function getNftStore(storeName: string): NftScopedStringStore;
export declare function getNftBytesStore(storeName: string): NftScopedBytesStore;
export declare function getNftKeyStore(storeName: string): NftScopedKeyStore;
