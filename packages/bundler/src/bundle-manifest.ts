import {
  BundleManifest,
  ExecutableModule,
  BuildMetadata,
  ResolvedBundlerOptions,
  SourceFile,
  EntrypointInfo,
} from './types';
import { createHash } from 'crypto';
import { PackageAnalysis } from './package-analysis';
import { FileCollection } from './file-collection';

/**
 * Generates bundle manifests from collected data
 */
export class BundleManifestGenerator {
  private readonly projectRoot: string;
  private readonly options: ResolvedBundlerOptions;
  private readonly packageAnalysis: PackageAnalysis;
  private readonly fileCollection: FileCollection;

  constructor(projectRoot: string, options: ResolvedBundlerOptions) {
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

    // Convert sources to simplified executable modules format
    const modules = this.convertSourcesToExecutableModules(sources, entrypoints);

    const metadata = this.generateMetadata(sources, entrypoints);

    // Generate unique manifest ID based on content
    const manifestId = this.generateManifestId(modules, dependencies.production, metadata);

    const manifest: BundleManifest = {
      id: manifestId,
      version: project.version || '1.0.0',
      modules,
      dependencies: dependencies.production, // Only runtime dependencies
    };

    if (this.options.mode === 'development') {
      manifest.metadata = metadata;
    }

    return manifest;
  }

  /**
   * Validates the generated manifest
   */
  async validateManifest(manifest: BundleManifest): Promise<string[]> {
    const errors: string[] = [];

    // Validate basic manifest structure
    if (!manifest.id) {
      errors.push('Manifest ID is missing');
    }

    if (!manifest.version) {
      errors.push('Manifest version is missing');
    }

    // Validate modules
    if (manifest.modules.length === 0) {
      errors.push('No modules found');
    }

    // Validate module content and handlers
    for (const module of manifest.modules) {
      if (!module.content.trim()) {
        errors.push(`Module ${module.specifier} is empty`);
      }

      // Validate handler modules actually have handlers if they import @proven-network/handler
      const hasProvenHandlerImport = module.imports.some((imp: string) =>
        imp.includes('@proven-network/handler')
      );
      if (hasProvenHandlerImport && module.handlers.length === 0) {
        errors.push(
          `Module ${module.specifier} imports @proven-network/handler but defines no handlers`
        );
      }
    }

    // Validate dependencies
    const depValidationErrors = await this.packageAnalysis.validateDependencies(
      manifest.dependencies
    );
    errors.push(...depValidationErrors);

    // Check for circular dependencies
    const circularDeps = this.detectCircularDependencies(manifest.modules);
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
      // Remove metadata in production
      delete optimized.metadata;
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
   * Converts source files to simplified executable modules format
   */
  private convertSourcesToExecutableModules(
    sources: SourceFile[],
    entrypoints: EntrypointInfo[]
  ): ExecutableModule[] {
    const modules: ExecutableModule[] = [];

    for (const source of sources) {
      // Find handlers for this source file
      const entrypoint = entrypoints.find((ep) => ep.filePath === source.filePath);
      const handlers = entrypoint ? entrypoint.handlers : [];

      // Extract all imports (both local and external) and resolve relative imports
      const imports = entrypoint
        ? entrypoint.imports.map((imp) => this.resolveImportSpecifier(imp.module, source, sources))
        : [];

      // Convert relative path to proper file:// URL format
      const moduleSpecifier = source.relativePath.startsWith('file://')
        ? source.relativePath
        : `file:///${source.relativePath.replace(/^\//, '')}`;

      modules.push({
        specifier: moduleSpecifier,
        content: source.content,
        handlers: handlers.map((h) => ({
          name: h.name,
          type: h.type,
          parameters: h.parameters,
          config: h.config,
        })),
        imports,
      });
    }

    return modules;
  }

  /**
   * Resolves an import specifier relative to the current source file
   */
  private resolveImportSpecifier(
    importSpecifier: string,
    currentSource: SourceFile,
    allSources: SourceFile[]
  ): string {
    // If it's an external module (npm package or @proven-network), return as-is
    if (!importSpecifier.startsWith('./') && !importSpecifier.startsWith('../')) {
      return importSpecifier;
    }

    // Resolve relative import to absolute file path
    const path = require('path');
    const currentDir = path.dirname(currentSource.relativePath);
    const resolvedPath = path.resolve('/', currentDir, importSpecifier);

    // Try to find the matching source file (with or without extension)
    const possibleExtensions = ['.ts', '.tsx', '.js', '.jsx', ''];

    for (const ext of possibleExtensions) {
      const candidatePath = resolvedPath + ext;
      const matchingSource = allSources.find(
        (s) =>
          s.relativePath === candidatePath || s.relativePath === candidatePath.replace(/^\//, '')
      );

      if (matchingSource) {
        // Convert to file:// URL format
        return matchingSource.relativePath.startsWith('file://')
          ? matchingSource.relativePath
          : `file:///${matchingSource.relativePath.replace(/^\//, '')}`;
      }
    }

    // If no matching source found, return the resolved path as file:// URL
    const normalizedPath = resolvedPath.replace(/^\//, '');
    return `file:///${normalizedPath}`;
  }

  /**
   * Generates a unique manifest ID based on content hash
   */
  private generateManifestId(
    modules: ExecutableModule[],
    dependencies: Record<string, string>,
    metadata: BuildMetadata
  ): string {
    const contentToHash = {
      modules: modules.map((m) => ({
        specifier: m.specifier,
        content: m.content,
        handlers: m.handlers,
      })),
      dependencies,
      timestamp: metadata.createdAt,
    };

    const hash = createHash('sha256').update(JSON.stringify(contentToHash)).digest('hex');

    return `manifest-${hash.substring(0, 16)}`;
  }

  /**
   * Generates bundle metadata
   */
  private generateMetadata(_sources: SourceFile[], _entrypoints?: EntrypointInfo[]): BuildMetadata {
    return {
      createdAt: new Date().toISOString(),
      mode: this.options.mode || 'development',
      pluginVersion: this.getPluginVersion(),
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
  private calculateBundleSize(modules: ExecutableModule[]): number {
    return modules.reduce((total, module) => {
      const contentSize = Buffer.byteLength(module.content, 'utf8');
      return total + contentSize;
    }, 0);
  }

  /**
   * Detects circular dependencies in the module graph
   */
  private detectCircularDependencies(modules: ExecutableModule[]): string[] {
    const circular: string[] = [];
    const visited = new Set<string>();
    const recursionStack = new Set<string>();

    // Build adjacency list from imports
    const adjacencyList = new Map<string, string[]>();

    for (const module of modules) {
      const deps = module.imports
        .filter((imp: string) => this.isLocalImport(imp))
        .map((imp: string) => this.resolveImportToModuleSpecifier(imp, module.specifier, modules));

      adjacencyList.set(module.specifier, deps.filter(Boolean) as string[]);
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

    // Check each module
    for (const module of modules) {
      if (!visited.has(module.specifier)) {
        detectCycle(module.specifier, []);
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
   * Resolves an import path to a module specifier
   */
  private resolveImportToModuleSpecifier(
    importPath: string,
    fromSpecifier: string,
    modules: ExecutableModule[]
  ): string | null {
    // This is a simplified resolution - in a real implementation,
    // you'd want more sophisticated module resolution
    const moduleMap = new Map(modules.map((m) => [m.specifier, m]));

    // Try to find a matching module
    for (const [moduleSpecifier] of moduleMap) {
      if (moduleSpecifier.includes(importPath) || importPath.includes(moduleSpecifier)) {
        return moduleSpecifier;
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
    const handlerCount = manifest.modules.reduce((sum, module) => sum + module.handlers.length, 0);
    const bundleSize = manifest.modules.reduce(
      (sum, module) => sum + Buffer.byteLength(module.content, 'utf8'),
      0
    );

    const stats = {
      totalModules: manifest.modules.length,
      handlerCount,
      dependencyCount: Object.keys(manifest.dependencies).length,
      bundleSize,
      hasMetadata: !!manifest.metadata,
    };

    // Handler breakdown
    const handlerTypes = new Map<string, number>();
    for (const module of manifest.modules) {
      for (const handler of module.handlers) {
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
      m.id &&
      m.version &&
      m.modules &&
      Array.isArray(m.modules) &&
      m.dependencies &&
      typeof m.dependencies === 'object'
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
    }> = [];

    for (const module of manifest.modules) {
      for (const handler of module.handlers) {
        handlers.push({
          name: handler.name,
          type: handler.type,
          config: handler.config || {},
          file: module.specifier,
        });
      }
    }

    return handlers;
  }
}
