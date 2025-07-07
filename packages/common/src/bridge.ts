// Message types for SDK ↔ Bridge communication
// Manifest types are shared between SDK and bundler

// SDK -> Bridge Request Messages
export type WhoAmIRequest = {
  type: 'whoAmI';
  nonce: number;
};

export type ExecuteRequest = {
  type: 'execute';
  nonce: number;
  data: {
    manifestId: string;
    manifest?: BundleManifest; // Full manifest on first call
    handler: string;
    args?: any[];
  };
};

export type RequestAuthSignalRequest = {
  type: 'request_auth_signal';
  nonce: number;
  signalKey: string;
};

export type UpdateAuthSignalRequest = {
  type: 'update_auth_signal';
  nonce: number;
  signalKey: string;
  signalValue: any;
};

export type BridgeRequest = ExecuteRequest | RequestAuthSignalRequest | UpdateAuthSignalRequest;

// Bridge -> SDK Response Messages
export type SuccessResponse = {
  type: 'response';
  nonce: number;
  success: true;
  data: any;
};

export type ErrorResponse = {
  type: 'response';
  nonce: number;
  success: false;
  error: string;
};

export type OpenModalResponse = {
  type: 'open_registration_modal';
};

export type CloseModalResponse = {
  type: 'close_registration_modal';
};

export type AuthSignalUpdateResponse = {
  type: 'auth_signal_update';
  signalKey: string;
  signalValue: any;
};

// SDK-specific execution result types (separate from internal RPC types)
export type SdkExecuteLog = {
  level: string;
  args: (string | number | boolean | null | undefined)[];
};

export type SdkExecuteSuccess = {
  output: string | number | boolean | null | undefined;
  duration: {
    secs: number;
    nanos: number;
  };
  logs: SdkExecuteLog[];
};

export type SdkExecuteError = {
  duration: {
    secs: number;
    nanos: number;
  };
  logs: SdkExecuteLog[];
  error: {
    name: string;
    message: string;
    stack?: string;
  };
};

// SDK execution result can be either Ok or Error
export type SdkExecutionResult = { Ok: SdkExecuteSuccess } | { Error: SdkExecuteError };

export type BridgeResponse =
  | SuccessResponse
  | ErrorResponse
  | OpenModalResponse
  | CloseModalResponse
  | AuthSignalUpdateResponse;

// Handler queue types for SDK integration
export interface QueuedHandler {
  manifestId: string;
  manifest?: BundleManifest; // Included on first call only
  handler: string;
  args: any[];
  resolve: (value: any) => void;
  reject: (error: Error) => void;
}

// Manifest types
export interface ParameterInfo {
  name: string;
  type?: string; // TypeScript type if available
  optional: boolean;
  defaultValue?: any;
}

export interface HandlerInfo {
  name: string;
  type: 'rpc' | 'http' | 'event' | 'schedule';
  parameters: ParameterInfo[];
  config?: any;
}

export interface ExecutableModule {
  specifier: string; // Module specifier (e.g., "file:///src/handlers.ts")
  content: string; // Source code content
  handlers: HandlerInfo[]; // Exported handlers (empty if no handlers)
  imports: string[]; // Module imports for dependency resolution
}

export interface BuildMetadata {
  createdAt: string;
  mode: 'development' | 'production';
  pluginVersion: string;
}

export interface BundleManifest {
  id: string;
  version: string;
  modules: ExecutableModule[];
  dependencies: Record<string, string>; // Runtime dependencies (NPM packages)
  metadata?: BuildMetadata; // Optional build metadata (for tooling/debugging)
}
