type NftStoreGetResponse<T> =
  | "NftDoesNotExist"
  | "NoAccountsInContext"
  | "None"
  | { OwnershipInvalid: string }
  | { Some: T };

type NftStoreSetResponse =
  | "NftDoesNotExist"
  | "NoAccountsInContext"
  | "Ok"
  | { OwnershipInvalid: string };

type PersonalStoreGetResponse<T> = "NoPersonalContext" | "None" | { Some: T };

type PersonalStoreSetResponse = "NoPersonalContext" | "Ok";

declare namespace Deno {
  namespace core {
    const ops: {
      // console
      op_console_log: (level: string, args: any[]) => void;

      // crypto
      op_generate_ed25519: () => number;
      op_get_curve_name: (keyId: number) => "Ed25519";
      op_get_public_key: (keyId: number) => Uint8Array;
      op_sign_bytes: (keyId: number, data: Uint8Array) => Uint8Array;
      op_sign_string: (keyId: number, data: string) => Uint8Array;

      // gateway-api-sdk
      op_get_gateway_network_id: () => number;
      op_get_gateway_origin: () => string;

      // kv
      op_application_keys: (
        storeName: string,
        type: string
      ) => Promise<string[]>;
      op_get_application_bytes: (
        storeName: string,
        key: string
      ) => Promise<Uint8Array | undefined>;
      op_set_application_bytes: (
        storeName: string,
        key: string,
        value: Uint8Array
      ) => Promise<void>;
      op_get_application_key: (
        storeName: string,
        key: string
      ) => Promise<number | undefined>;
      op_set_application_key: (
        storeName: string,
        key: string,
        keyId: number
      ) => Promise<void>;
      op_get_application_string: (
        storeName: string,
        key: string
      ) => Promise<string | undefined>;
      op_set_application_string: (
        storeName: string,
        key: string,
        value: string
      ) => Promise<void>;
      op_nft_keys: (
        storeName: string,
        resourceAddress: string,
        nftId: string,
        type: string
      ) => Promise<NftStoreGetResponse<string[]>>;
      op_get_nft_bytes: (
        storeName: string,
        resourceAddress: string,
        nftId: string,
        key: string
      ) => Promise<NftStoreGetResponse<Uint8Array>>;
      op_set_nft_bytes: (
        storeName: string,
        resourceAddress: string,
        nftId: string,
        key: string,
        value: Uint8Array
      ) => Promise<NftStoreSetResponse>;
      op_get_nft_key: (
        storeName: string,
        resourceAddress: string,
        nftId: string,
        key: string
      ) => Promise<NftStoreGetResponse<number>>;
      op_set_nft_key: (
        storeName: string,
        resourceAddress: string,
        nftId: string,
        key: string,
        keyId: number
      ) => Promise<NftStoreSetResponse>;
      op_get_nft_string: (
        storeName: string,
        resourceAddress: string,
        nftId: string,
        key: string
      ) => Promise<NftStoreGetResponse<string>>;
      op_set_nft_string: (
        storeName: string,
        resourceAddress: string,
        nftId: string,
        key: string,
        value: string
      ) => Promise<NftStoreSetResponse>;
      op_personal_keys: (
        storeName: string,
        type: string
      ) => Promise<PersonalStoreGetResponse<string[]>>;
      op_get_personal_bytes: (
        storeName: string,
        key: string
      ) => Promise<PersonalStoreGetResponse<Uint8Array>>;
      op_set_personal_bytes: (
        storeName: string,
        key: string,
        value: Uint8Array
      ) => Promise<PersonalStoreSetResponse>;
      op_get_personal_key: (
        storeName: string,
        key: string
      ) => Promise<PersonalStoreGetResponse<number>>;
      op_set_personal_key: (
        storeName: string,
        key: string,
        keyId: number
      ) => Promise<PersonalStoreSetResponse>;
      op_get_personal_string: (
        storeName: string,
        key: string
      ) => Promise<PersonalStoreGetResponse<string>>;
      op_set_personal_string: (
        storeName: string,
        key: string,
        value: string
      ) => Promise<PersonalStoreSetResponse>;

      // session
      op_get_current_accounts: () => string[];
      op_get_current_identity: () => string | undefined;
    };
  }
}
