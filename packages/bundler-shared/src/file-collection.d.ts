import { EntrypointInfo, SourceFile, BundlerOptions } from './types';
/**
 * Collects and processes source files for bundling
 */
export declare class FileCollection {
  private readonly projectRoot;
  private readonly options;
  private readonly entrypointDiscovery;
  constructor(projectRoot: string, options?: BundlerOptions);
  /**
   * Discovers all entrypoints in the project
   */
  discoverEntrypoints(): Promise<EntrypointInfo[]>;
  /**
   * Collects all source files that should be included in the bundle
   */
  collectSourceFiles(entrypoints: EntrypointInfo[]): Promise<SourceFile[]>;
  /**
   * Finds all source files in the project
   */
  private findSourceFiles;
  /**
   * Builds a dependency graph from the entrypoints
   */
  private buildDependencyGraph;
  /**
   * Recursively builds dependency graph for a file
   */
  private buildDependencyGraphRecursive;
  /**
   * Processes a single source file
   */
  private processSourceFile;
  /**
   * Checks if an import is a local file import
   */
  private isLocalImport;
  /**
   * Resolves an import path to an absolute file path
   */
  resolveImportPath(importPath: string, fromFile: string): string | null;
  /**
   * Async version for internal use
   */
  private resolveImportPathAsync;
  /**
   * Resolves file extension for imports that might omit extensions
   */
  private resolveFileExtension;
  /**
   * Loads source map for a file if it exists
   */
  private loadSourceMap;
  /**
   * Checks if a file exists
   */
  private fileExists;
}
