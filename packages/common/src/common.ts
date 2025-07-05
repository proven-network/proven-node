// Shared RPC command and response type definitions

// Basic types
export type ExecuteOutput = string | number | boolean | null | undefined;

// Generic response wrapper for all RPC responses
export type RpcResponse<T> = {
  type: string;
  data: T;
};

// Generic result wrapper for command responses
export type CommandResult<TSuccess, TFailure = string> =
  | { result: 'success'; data: TSuccess }
  | { result: 'failure'; data: TFailure }
  | { result: 'error'; data: TFailure };

// Anonymize command and response
export type Anonymize = { type: 'Anonymize'; data: null };
export type AnonymizeResult = CommandResult<null, string>;
export type AnonymizeResponse = RpcResponse<AnonymizeResult>;

// WhoAmI command and response
export type WhoAmI = { type: 'WhoAmI'; data: null };
export type WhoAmIResult =
  | { result: 'anonymous'; data: { session_id: string; origin: string } }
  | {
      result: 'identified';
      data: { identity: any; session_id: string; origin: string };
    } // TODO: define proper Identity type
  | { result: 'failure'; data: string };
export type WhoAmIResponse = RpcResponse<WhoAmIResult>;

// Execute commands with named fields
export type ExecuteHash = {
  type: 'ExecuteHash';
  data: {
    module_hash: string;
    handler_specifier: string;
    args: any[];
  };
};

export type Execute = {
  type: 'Execute';
  data: {
    module?: string;
    manifest?: any; // BundleManifest type
    handler_specifier: string;
    args: any[];
  };
};

// Execute response types
export type ExecuteLog = {
  level: string;
  args: ExecuteOutput[];
};

export type ExecuteSuccess = {
  output: ExecuteOutput;
  duration: {
    secs: number;
    nanos: number;
  };
  logs: ExecuteLog[];
};

export type ExecuteError = {
  duration: {
    secs: number;
    nanos: number;
  };
  logs: ExecuteLog[];
  error: {
    name: string;
    message: string;
    stack?: string;
    // There are more fields, but we don't need them for now
  };
};

// ExecutionResult can be either Ok or Error
export type ExecutionResult = { Ok: ExecuteSuccess } | { Error: ExecuteError };

// Execute response using the new tagged format
export type ExecuteResult = CommandResult<ExecutionResult, string>;
export type ExecuteResponse = RpcResponse<ExecuteResult>;

// ExecuteHash response using the new tagged format
export type ExecuteHashResult =
  | CommandResult<ExecutionResult, string>
  | { result: 'error'; data: null }; // HashUnknown
export type ExecuteHashResponse = RpcResponse<ExecuteHashResult>;

// Identify command and response
export type Identify = {
  type: 'Identify';
  data: {
    passkey_prf_public_key_bytes: Uint8Array;
    session_id_signature_bytes: Uint8Array;
  };
};

export type IdentifyResult = CommandResult<any, string>; // TODO: define proper Identity type
export type IdentifyResponse = RpcResponse<IdentifyResult>;

// CreateApplication command and response
export type CreateApplication = { type: 'CreateApplication'; data: null };
export type CreateApplicationResult = CommandResult<any, string>; // TODO: define proper Application type
export type CreateApplicationResponse = RpcResponse<CreateApplicationResult>;

// Union of all RPC commands
export type RpcCall = Anonymize | Execute | ExecuteHash | Identify | WhoAmI | CreateApplication;
