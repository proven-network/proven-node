// Message types for parent ↔ bridge communication

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

export type WhoAmIMessage = {
  type: 'whoAmI';
  nonce: number;
};

export type ExecuteMessage = {
  type: 'execute';
  nonce: number;
  data: {
    manifestId: string;
    manifest?: BundleManifest; // Full manifest on first call
    handler: string;
    args?: any[];
  };
};

export type ParentToBridgeMessage = WhoAmIMessage | ExecuteMessage;

export type ResponseMessage = {
  type: 'response';
  nonce: number;
  success: boolean;
  data?: any;
  error?: string;
};

export type OpenModalMessage = {
  type: 'open_registration_modal';
};

export type CloseModalMessage = {
  type: 'close_registration_modal';
};

export type BridgeToParentMessage = ResponseMessage | OpenModalMessage | CloseModalMessage;

// Handler queue types for SDK integration
export interface QueuedHandler {
  manifestId: string;
  manifest?: BundleManifest; // Included on first call only
  handler: string;
  args: any[];
  resolve: (value: any) => void;
  reject: (error: Error) => void;
}
