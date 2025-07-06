import { Plugin } from 'rollup';
import {
  BundlerOptions,
  BundleManifestGenerator,
  mergeOptions,
  validateOptions,
  formatFileSize,
  transformHandlers,
  hasHandlerImport,
} from './index';
import { BundleManifest, ExecutableModule } from '@proven-network/common';
import * as fs from 'fs/promises';
import * as path from 'path';

/**
 * Rollup plugin for bundling Proven Network applications
 */
export function provenRollupPlugin(options: BundlerOptions = {}): Plugin {
  // Validate options
  const validationErrors = validateOptions(options);
  if (validationErrors.length > 0) {
    throw new Error(`Invalid options: ${validationErrors.join(', ')}`);
  }

  const mergedOptions = mergeOptions(options);
  let generator: BundleManifestGenerator;
  let projectRoot: string;
  const transformedFiles = new Map<
    string,
    { originalCode: string; code: string; handlers: any[] }
  >();
  let manifestId: string;

  return {
    name: 'proven-rollup-plugin',

    async transform(code, id) {
      // Only transform files that import from @proven-network/handler
      if (!hasHandlerImport(code)) {
        return null;
      }

      // Skip node_modules
      if (id.includes('node_modules')) {
        return null;
      }

      try {
        console.log(`🔍 Transforming ${path.relative(projectRoot, id)}...`);

        // First pass: transform without manifest injection to discover handlers
        const result = await transformHandlers(code, {
          manifest: {
            id: manifestId,
            version: '1.0',
            modules: [],
            dependencies: {},
          },
          manifestId,
          filePath: `file://${id}`,
          projectRoot,
          sourceMap: true,
          skipManifestInjection: true, // Don't inject manifest yet
        });

        // Store discovered handlers and original code for later processing
        transformedFiles.set(id, {
          originalCode: code,
          code: result.code,
          handlers: result.handlers,
        });

        console.log(
          `✅ Discovered ${result.handlers.length} handlers in ${path.relative(projectRoot, id)}`
        );

        return {
          code: result.code,
          map: result.map,
        };
      } catch (error) {
        console.warn(`Failed to transform handlers in ${id}:`, error);
        return null;
      }
    },

    async buildStart(opts) {
      // Get the project root from Rollup's input configuration
      projectRoot = process.cwd();

      // Try to infer project root from input files
      if (opts.input) {
        if (typeof opts.input === 'string') {
          projectRoot = path.dirname(path.resolve(opts.input));
        } else if (Array.isArray(opts.input) && opts.input[0]) {
          projectRoot = path.dirname(path.resolve(opts.input[0]));
        } else if (typeof opts.input === 'object') {
          const firstInput = Object.values(opts.input)[0];
          if (typeof firstInput === 'string') {
            projectRoot = path.dirname(path.resolve(firstInput));
          }
        }
      }

      // Find the actual project root (where package.json is)
      projectRoot = findProjectRoot(projectRoot);

      // Initialize the generator and create a shared manifest ID
      generator = new BundleManifestGenerator(projectRoot, mergedOptions);
      manifestId = `bundle-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

      console.log('🔍 Generating manifest during transformation...');

      console.log('🔍 Proven Rollup Plugin initialized');
    },

    async generateBundle(options, bundle) {
      try {
        console.log('🔍 Creating complete manifest and re-transforming with full manifest...');

        // Create the complete manifest from discovered handlers
        const completeManifest = await createCompleteManifest(
          transformedFiles,
          projectRoot,
          mergedOptions,
          manifestId
        );

        // Re-transform all files with the complete manifest
        await retransformWithCompleteManifest(completeManifest, transformedFiles, projectRoot);

        // Validate the manifest (no need to update with transformed code)
        const validationErrors = await generator.validateManifest(completeManifest);
        if (validationErrors.length > 0) {
          const errorMessage = `Bundle validation failed:\n${validationErrors.join('\n')}`;
          this.error(new Error(errorMessage));
          return;
        }

        // Optimize for production if needed
        const optimizedManifest = generator.optimizeManifest(completeManifest);

        // Serialize the manifest
        const manifestJson = generator.serializeManifest(optimizedManifest);

        // Inject manifest variables into the bundle
        injectManifestVariables(bundle, optimizedManifest);

        // Handle output
        await handleOutput(manifestJson, mergedOptions, projectRoot, this);

        console.log('✅ Proven bundle manifest generated successfully');
        logBundleStats(optimizedManifest);
      } catch (error) {
        const errorMessage = `Failed to generate Proven bundle: ${error instanceof Error ? error.message : String(error)}`;
        this.error(new Error(errorMessage));
      }
    },
  };
}

/**
 * Creates a complete manifest from discovered handlers
 */
async function createCompleteManifest(
  transformedFiles: Map<string, { originalCode: string; code: string; handlers: any[] }>,
  projectRoot: string,
  options: BundlerOptions,
  manifestId: string
): Promise<BundleManifest> {
  const modules: ExecutableModule[] = [];

  // Convert transformed files to manifest modules
  for (const [filePath, transformed] of transformedFiles) {
    if (transformed.handlers.length > 0) {
      modules.push({
        specifier: `file:///${path.relative(projectRoot, filePath)}`,
        content: transformed.originalCode, // Use the original source code
        handlers: transformed.handlers.map((h) => ({
          name: h.name,
          type: h.type,
          parameters: h.parameters,
          config: h.config,
        })),
        imports: [], // TODO: Extract imports if needed
      });
    }
  }

  // Read package.json for dependencies
  let dependencies: Record<string, string> = {};
  try {
    const packageJsonPath = path.join(projectRoot, 'package.json');
    const packageJson = JSON.parse(await fs.readFile(packageJsonPath, 'utf-8'));
    const allDeps = {
      ...packageJson.dependencies,
      ...(options.includeDevDependencies ? packageJson.devDependencies : {}),
    };

    // Filter out file: dependencies as they are local packages
    dependencies = Object.fromEntries(
      Object.entries(allDeps).filter(
        ([, version]) => typeof version === 'string' && !version.startsWith('file:')
      )
    ) as Record<string, string>;
  } catch {
    // Ignore if package.json doesn't exist
  }

  return {
    id: manifestId,
    version: '1.0',
    modules,
    dependencies,
    metadata: {
      createdAt: new Date().toISOString(),
      mode: options.mode || 'development',
      pluginVersion: '0.0.1',
    },
  };
}

/**
 * Re-transforms all files with the complete manifest injected
 */
async function retransformWithCompleteManifest(
  completeManifest: BundleManifest,
  transformedFiles: Map<string, { originalCode: string; code: string; handlers: any[] }>,
  projectRoot: string
): Promise<void> {
  console.log('🔄 Re-transforming files with complete manifest...');

  for (const [filePath, transformed] of transformedFiles) {
    if (transformed.handlers.length > 0) {
      try {
        // Re-transform with the complete manifest and inject it
        const result = await transformHandlers(transformed.originalCode, {
          manifest: completeManifest,
          manifestId: completeManifest.id,
          filePath: `file://${filePath}`,
          projectRoot,
          sourceMap: true,
          // Don't skip manifest injection this time
        });

        // Update the transformed code
        transformedFiles.set(filePath, {
          ...transformed,
          code: result.code,
        });

        console.log(
          `✅ Re-transformed ${path.relative(projectRoot, filePath)} with complete manifest`
        );
      } catch (error) {
        console.warn(`Failed to re-transform ${filePath}:`, error);
      }
    }
  }
}

/**
 * Finds the project root by looking for package.json
 */
function findProjectRoot(startDir: string): string {
  let currentDir = path.resolve(startDir);

  while (true) {
    const packageJsonPath = path.join(currentDir, 'package.json');

    try {
      // Synchronous check since this runs during build setup
      require('fs').accessSync(packageJsonPath);
      return currentDir;
    } catch {
      // package.json not found, go up one directory
    }

    const parentDir = path.dirname(currentDir);
    if (parentDir === currentDir) {
      // Reached filesystem root, fallback to original directory
      return startDir;
    }
    currentDir = parentDir;
  }
}

/**
 * Handles the output of the bundle manifest
 */
async function handleOutput(
  manifestJson: string,
  options: BundlerOptions,
  projectRoot: string,
  plugin: any
): Promise<void> {
  const output = options.output || 'development';

  if (output === 'development') {
    // Emit as rollup asset for development
    plugin.emitFile({
      type: 'asset',
      fileName: 'proven-bundle.json',
      source: manifestJson,
    });
  } else {
    // Write to specified file path
    const outputPath = path.isAbsolute(output) ? output : path.resolve(projectRoot, output);

    try {
      await fs.mkdir(path.dirname(outputPath), { recursive: true });
      await fs.writeFile(outputPath, manifestJson, 'utf-8');
    } catch (error) {
      throw new Error(
        `Failed to write bundle manifest to ${output}: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }
}

/**
 * Injects manifest variables into the bundle
 */
function injectManifestVariables(bundle: any, manifest: BundleManifest): void {
  // Prepare the manifest variables to inject
  const manifestCode = `const _provenManifest = ${JSON.stringify(manifest, null, 2)};
let _provenManifestSent = false;
`;

  // Find the main bundle file and prepend the manifest variables
  for (const [fileName, chunk] of Object.entries(bundle)) {
    if (chunk && typeof chunk === 'object' && 'code' in chunk) {
      const bundleChunk = chunk as any;
      if (bundleChunk.code) {
        // Check if this bundle contains handler references
        if (
          bundleChunk.code.includes('_provenManifest') ||
          bundleChunk.code.includes('_provenManifestSent')
        ) {
          // Inject the manifest variables at the beginning of the bundle
          bundleChunk.code = manifestCode + bundleChunk.code;
          console.log(`✅ Injected manifest variables into ${fileName}`);
          break;
        }
      }
    }
  }
}

/**
 * Logs bundle statistics to the console
 */
function logBundleStats(manifest: BundleManifest): void {
  const handlers = manifest.modules.reduce(
    (sum: number, module: ExecutableModule) => sum + module.handlers.length,
    0
  );
  const bundleSize = manifest.modules.reduce(
    (sum: number, module: ExecutableModule) => sum + Buffer.byteLength(module.content, 'utf8'),
    0
  );

  const stats = {
    modules: manifest.modules.length,
    handlers,
    dependencies: Object.keys(manifest.dependencies).length,
    size: formatFileSize(bundleSize),
  };

  console.log(
    `📊 Bundle stats: ${stats.modules} modules, ${stats.handlers} handlers, ${stats.dependencies} dependencies (${stats.size})`
  );
}

// Named export for the plugin
export { provenRollupPlugin as proven };

// Default export
export default provenRollupPlugin;
