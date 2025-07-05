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
 * Information about a discovered entrypoint
 */
export interface EntrypointInfo {
    /** Absolute file path */
    filePath: string;
    /** Module specifier (relative to project root) */
    moduleSpecifier: string;
    /** Handler functions found in this file */
    handlers: HandlerInfo[];
    /** All imports in this file */
    imports: ImportInfo[];
}
/**
 * Information about a handler function
 */
export interface HandlerInfo {
    /** Export name of the handler */
    name: string;
    /** Type of handler (http, schedule, event, etc.) */
    type: 'http' | 'schedule' | 'event' | 'rpc' | 'unknown';
    /** Configuration passed to the handler */
    config?: Record<string, unknown>;
    /** Line number where handler is defined */
    line?: number;
    /** Column number where handler is defined */
    column?: number;
}
/**
 * Information about an import statement
 */
export interface ImportInfo {
    /** Module being imported */
    module: string;
    /** Type of import (default, named, namespace, etc.) */
    type: 'default' | 'named' | 'namespace' | 'side-effect';
    /** Names being imported (for named imports) */
    imports?: string[];
    /** Local name (for default/namespace imports) */
    localName?: string;
    /** Whether this is a Proven Network handler import */
    isProvenHandler?: boolean;
}
/**
 * Complete bundle manifest ready for transmission
 */
export interface BundleManifest {
    /** Project metadata */
    project: ProjectInfo;
    /** All entrypoint files */
    entrypoints: EntrypointInfo[];
    /** All source files included in the bundle */
    sources: SourceFile[];
    /** Dependency information */
    dependencies: DependencyInfo;
    /** Bundle metadata */
    metadata: BundleMetadata;
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
/**
 * Bundle metadata
 */
export interface BundleMetadata {
    /** Timestamp when bundle was created */
    createdAt: string;
    /** Build mode */
    mode: 'development' | 'production';
    /** Plugin version that created the bundle */
    pluginVersion: string;
    /** Total number of files */
    fileCount: number;
    /** Total bundle size in bytes */
    bundleSize: number;
    /** Source maps included */
    sourceMaps: boolean;
    /** Build mode (alias for mode) */
    buildMode?: 'development' | 'production';
    /** Number of entrypoints */
    entrypointCount?: number;
    /** Number of handlers */
    handlerCount?: number;
    /** Number of source files */
    sourceFileCount?: number;
    /** Total source size in bytes */
    totalSourceSize?: number;
    /** Include dev dependencies */
    includeDevDependencies?: boolean;
    /** Timestamp when bundle was generated */
    generatedAt?: string;
}
