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
  type: 'http' | 'schedule' | 'event' | 'rpc';
  parameters: ParameterInfo[];
  config?: any;
  line?: number;
  column?: number;
}

export interface ManifestModule {
  path: string;
  content: string;
  handlers: HandlerInfo[];
  dependencies: string[]; // Module paths this module imports
}

export interface EntrypointInfo {
  filePath: string;
  moduleSpecifier: string;
  handlers: HandlerInfo[];
  imports: ImportInfo[];
}

export interface ImportInfo {
  module: string;
  type: 'default' | 'named' | 'namespace' | 'side-effect';
  imports?: string[];
  localName?: string;
  isProvenHandler?: boolean;
}

export interface DependencyInfo {
  production: Record<string, string>;
  development: Record<string, string>;
  all: Record<string, string>;
}

export interface BundleMetadata {
  createdAt: string;
  mode: 'development' | 'production';
  pluginVersion: string;
  fileCount: number;
  bundleSize: number;
  sourceMaps: boolean;
  buildMode?: string;
  entrypointCount?: number;
  handlerCount?: number;
}

export interface ProjectInfo {
  name: string;
  version: string;
  description?: string;
  main?: string;
  scripts?: Record<string, string>;
  dependencies?: Record<string, string>;
  devDependencies?: Record<string, string>;
}

export interface SourceInfo {
  relativePath: string;
  content: string;
  size?: number;
}

export interface BundleManifest {
  id: string;
  version: string;
  project: ProjectInfo;
  modules: ManifestModule[];
  entrypoints: EntrypointInfo[];
  sources: SourceInfo[];
  dependencies: DependencyInfo;
  metadata: BundleMetadata;
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
  handler: string;
  args: any[];
  resolve: (value: any) => void;
  reject: (error: Error) => void;
}
