import * as fs from 'fs';
import * as path from 'path';
import { glob } from 'fast-glob';
import { EntrypointInfo, SourceFile, BundlerOptions } from './types';
import { EntrypointDiscovery } from './entrypoint-discovery';

/**
 * Collects and processes source files for bundling
 */
export class FileCollection {
  private readonly projectRoot: string;
  private readonly options: BundlerOptions;
  private readonly entrypointDiscovery: EntrypointDiscovery;

  constructor(projectRoot: string, options: BundlerOptions = {}) {
    this.projectRoot = projectRoot;
    this.options = options;
    this.entrypointDiscovery = new EntrypointDiscovery(projectRoot, options.entryPatterns);
  }

  /**
   * Discovers all entrypoints in the project
   */
  async discoverEntrypoints(): Promise<EntrypointInfo[]> {
    const sourceFiles = await this.findSourceFiles();
    const entrypoints: EntrypointInfo[] = [];

    for (const filePath of sourceFiles) {
      try {
        const entrypoint = await this.entrypointDiscovery.analyzeFile(filePath);
        if (entrypoint) {
          entrypoints.push(entrypoint);
        }
      } catch (error) {
        // Skip files that can't be parsed
        console.warn(`Failed to analyze ${filePath}:`, error);
      }
    }

    return entrypoints;
  }

  /**
   * Collects all source files that should be included in the bundle
   */
  async collectSourceFiles(entrypoints: EntrypointInfo[]): Promise<SourceFile[]> {
    const allFiles = new Set<string>();
    const sourceFiles: SourceFile[] = [];

    // Add all entrypoint files
    for (const entrypoint of entrypoints) {
      allFiles.add(entrypoint.filePath);
    }

    // Build dependency graph from entrypoints
    const dependencyGraph = await this.buildDependencyGraph(entrypoints);

    // Add all dependencies to the file set
    for (const deps of Array.from(dependencyGraph.values())) {
      deps.forEach((dep) => allFiles.add(dep));
    }

    // Process each file
    for (const filePath of Array.from(allFiles)) {
      try {
        const sourceFile = await this.processSourceFile(filePath, entrypoints);
        sourceFiles.push(sourceFile);
      } catch (error) {
        console.warn(`Failed to process source file ${filePath}:`, error);
      }
    }

    return sourceFiles;
  }

  /**
   * Finds all source files in the project
   */
  private async findSourceFiles(): Promise<string[]> {
    const includePatterns = this.options.include || ['**/*.{ts,tsx,js,jsx,mts,mjs}'];

    const excludePatterns = this.options.exclude || [
      'node_modules/**',
      'dist/**',
      'build/**',
      '.git/**',
      '**/*.test.{ts,tsx,js,jsx}',
      '**/*.spec.{ts,tsx,js,jsx}',
      '**/*.d.ts',
    ];

    const files = await glob(includePatterns, {
      cwd: this.projectRoot,
      ignore: excludePatterns,
      absolute: true,
      onlyFiles: true,
    });

    return files;
  }

  /**
   * Builds a dependency graph from the entrypoints
   */
  private async buildDependencyGraph(
    entrypoints: EntrypointInfo[]
  ): Promise<Map<string, string[]>> {
    const dependencyGraph = new Map<string, string[]>();
    const visited = new Set<string>();

    // Process each entrypoint
    for (const entrypoint of entrypoints) {
      await this.buildDependencyGraphRecursive(entrypoint.filePath, dependencyGraph, visited);
    }

    return dependencyGraph;
  }

  /**
   * Recursively builds dependency graph for a file
   */
  private async buildDependencyGraphRecursive(
    filePath: string,
    dependencyGraph: Map<string, string[]>,
    visited: Set<string>
  ): Promise<void> {
    if (visited.has(filePath)) {
      return;
    }

    visited.add(filePath);

    try {
      const entrypoint = await this.entrypointDiscovery.analyzeFile(filePath);
      if (!entrypoint) {
        return;
      }

      const dependencies: string[] = [];

      // Process imports to find local file dependencies
      for (const importInfo of entrypoint.imports) {
        if (!importInfo.isProvenHandler && this.isLocalImport(importInfo.module)) {
          const resolvedPath = await this.resolveImportPathAsync(filePath, importInfo.module);
          if (resolvedPath && (await this.fileExists(resolvedPath))) {
            dependencies.push(resolvedPath);

            // Recursively process dependencies
            await this.buildDependencyGraphRecursive(resolvedPath, dependencyGraph, visited);
          }
        }
      }

      dependencyGraph.set(filePath, dependencies);
    } catch (error) {
      // Skip files that can't be analyzed
      console.warn(`Failed to build dependency graph for ${filePath}:`, error);
    }
  }

  /**
   * Processes a single source file
   */
  private async processSourceFile(
    filePath: string,
    entrypoints: EntrypointInfo[]
  ): Promise<SourceFile> {
    const content = await fs.promises.readFile(filePath, 'utf-8');
    const relativePath = path.relative(this.projectRoot, filePath);
    const isEntrypoint = entrypoints.some((ep) => ep.filePath === filePath);

    // TODO: Handle source maps if requested
    let sourceMap: string | undefined;
    if (this.options.sourceMaps) {
      sourceMap = await this.loadSourceMap(filePath);
    }

    return {
      filePath,
      relativePath,
      content,
      sourceMap: sourceMap || '',
      size: Buffer.byteLength(content, 'utf8'),
      isEntrypoint,
    };
  }

  /**
   * Checks if an import is a local file import
   */
  private isLocalImport(module: string): boolean {
    return (
      module.startsWith('./') ||
      module.startsWith('../') ||
      (!module.startsWith('@') && !module.includes('/'))
    );
  }

  /**
   * Resolves an import path to an absolute file path
   */
  resolveImportPath(importPath: string, fromFile: string): string | null {
    // Handle external packages
    if (
      importPath.startsWith('@') ||
      (!importPath.startsWith('./') && !importPath.startsWith('../'))
    ) {
      return null;
    }
    const fromDir = path.dirname(fromFile);

    // Handle relative imports
    if (importPath.startsWith('./') || importPath.startsWith('../')) {
      const resolved = path.resolve(fromDir, importPath);
      return path.normalize(resolved);
    }

    return null;
  }

  /**
   * Async version for internal use
   */
  private async resolveImportPathAsync(
    fromFile: string,
    importPath: string
  ): Promise<string | null> {
    const resolved = this.resolveImportPath(importPath, fromFile);
    if (!resolved) {
      return null;
    }
    return await this.resolveFileExtension(resolved);
  }

  /**
   * Resolves file extension for imports that might omit extensions
   */
  private async resolveFileExtension(basePath: string): Promise<string | null> {
    const extensions = ['.ts', '.tsx', '.js', '.jsx', '.mts', '.mjs'];

    // First try the exact path
    if (await this.fileExists(basePath)) {
      return basePath;
    }

    // Try with extensions
    for (const ext of extensions) {
      const withExt = basePath + ext;
      if (await this.fileExists(withExt)) {
        return withExt;
      }
    }

    // Try index files
    for (const ext of extensions) {
      const indexFile = path.join(basePath, `index${ext}`);
      if (await this.fileExists(indexFile)) {
        return indexFile;
      }
    }

    return null;
  }

  /**
   * Loads source map for a file if it exists
   */
  private async loadSourceMap(filePath: string): Promise<string | undefined> {
    const sourceMapPath = filePath + '.map';

    try {
      if (await this.fileExists(sourceMapPath)) {
        return await fs.promises.readFile(sourceMapPath, 'utf-8');
      }
    } catch {
      // Source map loading failed, continue without it
    }

    return undefined;
  }

  /**
   * Checks if a file exists
   */
  private async fileExists(filePath: string): Promise<boolean> {
    try {
      const stats = await fs.promises.stat(filePath);
      return stats.isFile();
    } catch {
      return false;
    }
  }
}
