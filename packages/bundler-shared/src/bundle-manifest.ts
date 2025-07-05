import {
  BundleManifest,
  BundlerOptions,
  DependencyInfo,
  SourceFile,
  EntrypointInfo,
  BundleMetadata,
  ManifestModule,
} from './types';
import { createHash } from 'crypto';
import { PackageAnalysis } from './package-analysis';
import { FileCollection } from './file-collection';

/**
 * Generates bundle manifests from collected data
 */
export class BundleManifestGenerator {
  private readonly projectRoot: string;
  private readonly options: BundlerOptions;
  private readonly packageAnalysis: PackageAnalysis;
  private readonly fileCollection: FileCollection;

  constructor(projectRoot: string, options: BundlerOptions = {}) {
    this.projectRoot = projectRoot;
    this.options = options;
    this.packageAnalysis = new PackageAnalysis(projectRoot);
    this.fileCollection = new FileCollection(projectRoot, options);
  }

  /**
   * Generates a complete bundle manifest
   */
  async generateManifest(): Promise<BundleManifest> {
    console.log('Analyzing project...');
    const project = await this.packageAnalysis.analyzeProject(this.options.packageJsonPath);

    console.log('Analyzing dependencies...');
    const dependencies = await this.packageAnalysis.analyzeDependencies(
      this.options.includeDevDependencies
    );

    console.log('Discovering entrypoints...');
    const entrypoints = await this.fileCollection.discoverEntrypoints();

    if (entrypoints.length === 0) {
      throw new Error(
        'No entrypoints found. Make sure your project contains files that import @proven-network/handler'
      );
    }

    console.log(`Found ${entrypoints.length} entrypoint(s)`);

    console.log('Collecting source files...');
    const sources = await this.fileCollection.collectSourceFiles(entrypoints);

    console.log(`Collected ${sources.length} source file(s)`);

    // Convert sources to modules format
    const modules = this.convertSourcesToModules(sources, entrypoints);

    const metadata = this.generateMetadata(sources, entrypoints);

    // Generate unique manifest ID based on content
    const manifestId = this.generateManifestId(modules, dependencies, metadata);

    return {
      id: manifestId,
      version: project.version || '1.0.0',
      modules,
      entrypoints,
      dependencies,
      metadata,
    };
  }

  /**
   * Validates the generated manifest
   */
  async validateManifest(manifest: BundleManifest): Promise<string[]> {
    const errors: string[] = [];

    // Validate project info
    if (!manifest.project.name) {
      errors.push('Project name is missing from package.json');
    }

    if (!manifest.project.version) {
      errors.push('Project version is missing from package.json');
    }

    // Validate entrypoints
    if (manifest.entrypoints.length === 0) {
      errors.push('No entrypoints found');
    }

    // Validate that all entrypoints have handlers
    for (const entrypoint of manifest.entrypoints) {
      if (entrypoint.handlers.length === 0) {
        errors.push(`Entrypoint ${entrypoint.moduleSpecifier} has no handlers`);
      }
    }

    // Validate dependencies
    const depValidationErrors = await this.packageAnalysis.validateDependencies(
      manifest.dependencies.production
    );
    errors.push(...depValidationErrors);

    // Validate source files
    for (const source of manifest.sources) {
      if (!source.content.trim()) {
        errors.push(`Source file ${source.relativePath} is empty`);
      }
    }

    // Check for circular dependencies
    const circularDeps = this.detectCircularDependencies(manifest.entrypoints, manifest.sources);
    if (circularDeps.length > 0) {
      errors.push(`Circular dependencies detected: ${circularDeps.join(', ')}`);
    }

    return errors;
  }

  /**
   * Optimizes the manifest for production
   */
  optimizeManifest(manifest: BundleManifest): BundleManifest {
    const optimized = { ...manifest };

    if (this.options.mode === 'production') {
      // Remove source maps in production if not explicitly requested
      if (!this.options.sourceMaps) {
        optimized.sources = optimized.sources.map((source) => ({
          ...source,
          sourceMap: undefined,
        }));
      }

      // Remove dev dependencies in production
      optimized.dependencies = {
        production: optimized.dependencies.production,
        development: {},
        all: optimized.dependencies.production,
      };

      // Update metadata
      optimized.metadata = {
        ...optimized.metadata,
        sourceMaps: !!this.options.sourceMaps,
        bundleSize: this.calculateBundleSize(optimized.sources),
      };
    }

    return optimized;
  }

  /**
   * Serializes manifest to JSON
   */
  serializeManifest(manifest: BundleManifest): string {
    return JSON.stringify(manifest, null, this.options.mode === 'development' ? 2 : 0);
  }

  /**
   * Converts source files to modules format with handler information
   */
  private convertSourcesToModules(
    sources: SourceFile[],
    entrypoints: EntrypointInfo[]
  ): ManifestModule[] {
    const modules: ManifestModule[] = [];

    for (const source of sources) {
      // Find handlers for this source file
      const entrypoint = entrypoints.find((ep) => ep.filePath === source.filePath);
      const handlers = entrypoint ? entrypoint.handlers : [];

      // Extract dependencies from imports (simplified)
      const dependencies = entrypoint
        ? entrypoint.imports
            .filter((imp) => imp.module.startsWith('./') || imp.module.startsWith('../'))
            .map((imp) => imp.module)
        : [];

      modules.push({
        path: source.relativePath,
        content: source.content,
        handlers,
        dependencies,
      });
    }

    return modules;
  }

  /**
   * Generates a unique manifest ID based on content hash
   */
  private generateManifestId(
    modules: ManifestModule[],
    dependencies: DependencyInfo,
    metadata: BundleMetadata
  ): string {
    const contentToHash = {
      modules: modules.map((m) => ({ path: m.path, content: m.content, handlers: m.handlers })),
      dependencies: dependencies.production,
      timestamp: metadata.createdAt,
    };

    const hash = createHash('sha256').update(JSON.stringify(contentToHash)).digest('hex');

    return `manifest-${hash.substring(0, 16)}`;
  }

  /**
   * Generates bundle metadata
   */
  private generateMetadata(sources: SourceFile[], entrypoints?: EntrypointInfo[]): BundleMetadata {
    const handlerCount = entrypoints?.reduce((sum, ep) => sum + ep.handlers.length, 0) || 0;
    const totalSourceSize = sources.reduce((sum, source) => sum + source.size, 0);

    return {
      createdAt: new Date().toISOString(),
      mode: this.options.mode || 'development',
      pluginVersion: this.getPluginVersion(),
      fileCount: sources.length,
      bundleSize: this.calculateBundleSize(sources),
      sourceMaps: !!this.options.sourceMaps,
      buildMode: this.options.mode || 'development',
      entrypointCount: entrypoints?.length || 0,
      handlerCount,
      sourceFileCount: sources.length,
      totalSourceSize,
      includeDevDependencies: !!this.options.includeDevDependencies,
      generatedAt: new Date().toISOString(),
    };
  }

  /**
   * Gets the plugin version from package.json
   */
  private getPluginVersion(): string {
    try {
      // Try to get version from the bundler-shared package
      const packageJson = require('../package.json');
      return packageJson.version || '0.0.1';
    } catch {
      return '0.0.1';
    }
  }

  /**
   * Calculates the total bundle size in bytes
   */
  private calculateBundleSize(sources: SourceFile[]): number {
    return sources.reduce((total, source) => {
      const contentSize = Buffer.byteLength(source.content, 'utf8');
      const sourceMapSize = source.sourceMap ? Buffer.byteLength(source.sourceMap, 'utf8') : 0;
      return total + contentSize + sourceMapSize;
    }, 0);
  }

  /**
   * Detects circular dependencies in the module graph
   */
  private detectCircularDependencies(
    entrypoints: EntrypointInfo[],
    sources: SourceFile[]
  ): string[] {
    const circular: string[] = [];
    const visited = new Set<string>();
    const recursionStack = new Set<string>();

    // Build adjacency list from imports
    const adjacencyList = new Map<string, string[]>();

    for (const entrypoint of entrypoints) {
      const deps = entrypoint.imports
        .filter((imp) => !imp.isProvenHandler && this.isLocalImport(imp.module))
        .map((imp) => this.resolveImportToSourcePath(imp.module, entrypoint.filePath, sources));

      adjacencyList.set(entrypoint.filePath, deps.filter(Boolean) as string[]);
    }

    // DFS to detect cycles
    const detectCycle = (node: string, path: string[]): boolean => {
      if (recursionStack.has(node)) {
        circular.push([...path, node].join(' -> '));
        return true;
      }

      if (visited.has(node)) {
        return false;
      }

      visited.add(node);
      recursionStack.add(node);

      const neighbors = adjacencyList.get(node) || [];
      for (const neighbor of neighbors) {
        if (detectCycle(neighbor, [...path, node])) {
          return true;
        }
      }

      recursionStack.delete(node);
      return false;
    };

    // Check each entrypoint
    for (const entrypoint of entrypoints) {
      if (!visited.has(entrypoint.filePath)) {
        detectCycle(entrypoint.filePath, []);
      }
    }

    return circular;
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
   * Resolves an import path to a source file path
   */
  private resolveImportToSourcePath(
    importPath: string,
    fromFile: string,
    sources: SourceFile[]
  ): string | null {
    // This is a simplified resolution - in a real implementation,
    // you'd want more sophisticated module resolution
    const sourceMap = new Map(sources.map((s) => [s.relativePath, s]));

    // Try to find a matching source file
    for (const [sourcePath] of sourceMap) {
      if (sourcePath.includes(importPath) || importPath.includes(sourcePath)) {
        return sourcePath;
      }
    }

    return null;
  }
}

/**
 * Utility functions for working with bundle manifests
 */
export class BundleManifestUtils {
  /**
   * Calculates bundle statistics
   */
  static calculateStats(manifest: BundleManifest) {
    const stats = {
      totalFiles: manifest.sources.length,
      entrypointCount: manifest.entrypoints.length,
      handlerCount: manifest.entrypoints.reduce((sum, ep) => sum + ep.handlers.length, 0),
      dependencyCount: Object.keys(manifest.dependencies.production).length,
      bundleSize: manifest.metadata.bundleSize,
      hasSourceMaps: manifest.metadata.sourceMaps,
    };

    // Handler breakdown
    const handlerTypes = new Map<string, number>();
    for (const entrypoint of manifest.entrypoints) {
      for (const handler of entrypoint.handlers) {
        handlerTypes.set(handler.type, (handlerTypes.get(handler.type) || 0) + 1);
      }
    }

    return {
      ...stats,
      handlersByType: Object.fromEntries(handlerTypes),
    };
  }

  /**
   * Validates manifest format
   */
  static validateFormat(manifest: unknown): manifest is BundleManifest {
    if (!manifest || typeof manifest !== 'object') {
      return false;
    }

    const m = manifest as any;

    return !!(
      m.project &&
      m.entrypoints &&
      Array.isArray(m.entrypoints) &&
      m.sources &&
      Array.isArray(m.sources) &&
      m.dependencies &&
      m.metadata
    );
  }

  /**
   * Extracts handler information for API documentation
   */
  static extractHandlerDocs(manifest: BundleManifest) {
    const handlers: Array<{
      name: string;
      type: string;
      config: Record<string, unknown>;
      file: string;
      line?: number;
    }> = [];

    for (const entrypoint of manifest.entrypoints) {
      for (const handler of entrypoint.handlers) {
        handlers.push({
          name: handler.name,
          type: handler.type,
          config: handler.config || {},
          file: entrypoint.moduleSpecifier,
          line: handler.line,
        });
      }
    }

    return handlers;
  }
}
