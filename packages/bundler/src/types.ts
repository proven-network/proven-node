import {
  BundleManifest,
  ExecutableModule,
  BuildMetadata,
  HandlerInfo,
  ParameterInfo,
  QueuedHandler,
} from '@proven-network/common';

// Re-export common types for convenience
export type {
  BundleManifest,
  ExecutableModule,
  BuildMetadata,
  HandlerInfo,
  ParameterInfo,
  QueuedHandler,
};

/**
 * Configuration options for the bundler plugins
 */
export interface BundlerOptions {
  /**
   * Output destination for the bundle manifest
   * Can be a file path, HTTP endpoint, or 'development' for dev server integration
   */
  output?: string | 'development';

  /**
   * Custom patterns to identify entrypoints beyond @proven-network/handler imports
   */
  entryPatterns?: string[];

  /**
   * File patterns to include in the bundle
   */
  include?: string[];

  /**
   * File patterns to exclude from the bundle
   */
  exclude?: string[];

  /**
   * Whether to include source maps in the bundle
   */
  sourceMaps?: boolean;

  /**
   * Development vs production mode
   */
  mode?: 'development' | 'production';

  /**
   * Custom package.json path if not in project root
   */
  packageJsonPath?: string;

  /**
   * Whether to include dev dependencies
   */
  includeDevDependencies?: boolean;
}

/**
 * Resolved bundler options with all required fields populated
 */
export interface ResolvedBundlerOptions {
  output: string | 'development';
  entryPatterns: string[];
  include: string[];
  exclude: string[];
  sourceMaps: boolean;
  mode: 'development' | 'production';
  packageJsonPath?: string;
  includeDevDependencies: boolean;
}

/**
 * Project information
 */
export interface ProjectInfo {
  /** Project name from package.json */
  name?: string;

  /** Project version from package.json */
  version?: string;

  /** Project description from package.json */
  description?: string;

  /** Project root directory */
  rootDir: string;

  /** Package.json contents */
  packageJson: Record<string, unknown>;
}

/**
 * Source file information
 */
export interface SourceFile {
  /** Absolute path to file */
  filePath: string;

  /** Relative path from project root */
  relativePath: string;

  /** File contents */
  content: string;

  /** Source map if available */
  sourceMap?: string;

  /** File size in bytes */
  size: number;

  /** Whether this is an entrypoint */
  isEntrypoint?: boolean;
}

/**
 * Entrypoint information
 */
export interface EntrypointInfo {
  /** Module specifier */
  moduleSpecifier: string;

  /** File path */
  filePath: string;

  /** Handlers exported by this entrypoint */
  handlers: HandlerInfo[];

  /** Import information */
  imports: ImportInfo[];
}

/**
 * Import information
 */
export interface ImportInfo {
  /** Module being imported */
  module: string;

  /** Whether this is a @proven-network import */
  isProvenHandler: boolean;

  /** Import specifiers */
  specifiers?: string[];
}

/**
 * Dependency information
 */
export interface DependencyInfo {
  /** Production dependencies */
  production: Record<string, string>;

  /** Development dependencies */
  development: Record<string, string>;

  /** All dependencies combined */
  all: Record<string, string>;
}

/**
 * Resolved dependency information
 */
export interface ResolvedDependency {
  /** Package name */
  name: string;

  /** Resolved version */
  version: string;

  /** Whether this is a dev dependency */
  isDev: boolean;

  /** Transitive dependencies */
  dependencies: string[];
}

// Global types that are injected by the bundler
declare global {
  interface Window {
    __ProvenHandlerQueue__: QueuedHandler[] & {
      push: (handler: QueuedHandler) => number;
    };
    __ProvenManifest__: BundleManifest;
  }
}

// Export the virtual module types directly
export interface ProvenHandlers {
  [key: string]: QueuedHandler;
}

export interface ProvenHandlerTypes {
  [key: string]: string;
}
