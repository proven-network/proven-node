type HandlerType = "http" | "rpc";

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

type NftDbResponse<T> =
  | "NftDoesNotExist"
  | "NoAccountsInContext"
  | { Ok: T }
  | { OwnershipInvalid: string };

type PersonalStoreGetResponse<T> = "NoPersonalContext" | "None" | { Some: T };

type PersonalStoreSetResponse = "NoPersonalContext" | "Ok";

type PersonalDbResponse<T> = "NoPersonalContext" | { Ok: T };

type SqlValue =
  | { BlobWithName: [string, Uint8Array] }
  | { IntegerWithName: [string, number] }
  | { NullWithName: string }
  | { RealWithName: [string, number] }
  | { TextWithName: [string, string] }
  | { Blob: Uint8Array }
  | { Integer: number }
  | "Null"
  | { Real: number }
  | { Text: string };

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

      // handler
      op_add_allowed_origin: (
        handlerType: HandlerType,
        handlerName: string,
        origin: string
      ) => void;
      op_set_memory_option: (
        handlerType: HandlerType,
        handlerName: string,
        memory: number
      ) => void;
      op_set_path_option: (
        handlerType: "http",
        handlerName: string,
        path: string
      ) => void;
      op_set_timeout_option: (
        handlerType: HandlerType,
        handlerName: string,
        timeout: number
      ) => void;

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

      // sql
      op_create_params_list: () => number;
      op_add_blob_param: (paramListId: number, value: Uint8Array) => void;
      op_add_integer_param: (paramListId: number, value: number) => void;
      op_add_null_param: (paramListId: number) => void;
      op_add_real_param: (paramListId: number, value: number) => void;
      op_add_text_param: (paramListId: number, value: string) => void;
      op_execute_application_sql: (
        storeName: string,
        query: string,
        paramListId?: number
      ) => Promise<number>;
      op_query_application_sql: (
        storeName: string,
        query: string,
        paramListId?: number
      ) => Promise<SqlValue[][]>;
      op_execute_nft_sql: (
        storeName: string,
        resourceAddress: string,
        nftId: string,
        query: string,
        paramListId?: number
      ) => Promise<NftDbResponse<number>>;
      op_query_nft_sql: (
        storeName: string,
        resourceAddress: string,
        nftId: string,
        query: string,
        paramListId?: number
      ) => Promise<NftDbResponse<SqlValue[][]>>;
      op_execute_personal_sql: (
        storeName: string,
        query: string,
        paramListId?: number
      ) => Promise<PersonalDbResponse<number>>;
      op_query_personal_sql: (
        storeName: string,
        query: string,
        paramListId?: number
      ) => Promise<PersonalDbResponse<SqlValue[][]>>;
      op_migrate_application_sql: (
        storeName: string,
        query: string
      ) => Promise<void>;
      op_migrate_nft_sql: (storeName: string, query: string) => Promise<void>;
      op_migrate_personal_sql: (
        storeName: string,
        query: string
      ) => Promise<void>;
    };
  }
}
