import {
  BundleManifest as CommonBundleManifest,
  HandlerInfo as CommonHandlerInfo,
  EntrypointInfo as CommonEntrypointInfo,
  ImportInfo as CommonImportInfo,
  ManifestModule as CommonManifestModule,
  DependencyInfo as CommonDependencyInfo,
  BundleMetadata as CommonBundleMetadata,
  ParameterInfo as CommonParameterInfo,
} from '@proven-network/common';

// Re-export common types for convenience
export type BundleManifest = CommonBundleManifest;
export type HandlerInfo = CommonHandlerInfo;
export type EntrypointInfo = CommonEntrypointInfo;
export type ImportInfo = CommonImportInfo;
export type ManifestModule = CommonManifestModule;
export type DependencyInfo = CommonDependencyInfo;
export type BundleMetadata = CommonBundleMetadata;
export type ParameterInfo = CommonParameterInfo;

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
