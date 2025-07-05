import { Plugin } from 'rollup';
import {
  BundlerOptions,
  BundleManifestGenerator,
  mergeOptions,
  validateOptions,
  formatFileSize,
} from './index';
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

  return {
    name: 'proven-rollup-plugin',

    buildStart(opts) {
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

      // Initialize the generator
      generator = new BundleManifestGenerator(projectRoot, mergedOptions);

      console.log('🔍 Proven Rollup Plugin initialized');
    },

    async generateBundle() {
      try {
        console.log('🔍 Generating Proven bundle manifest...');

        const manifest = await generator.generateManifest();

        // Validate the manifest
        const validationErrors = await generator.validateManifest(manifest);
        if (validationErrors.length > 0) {
          const errorMessage = `Bundle validation failed:\n${validationErrors.join('\n')}`;
          this.error(new Error(errorMessage));
          return;
        }

        // Optimize for production if needed
        const finalManifest = generator.optimizeManifest(manifest);

        // Serialize the manifest
        const manifestJson = generator.serializeManifest(finalManifest);

        // Handle output
        await handleOutput(manifestJson, mergedOptions, projectRoot, this);

        console.log('✅ Proven bundle manifest generated successfully');
        logBundleStats(finalManifest);
      } catch (error) {
        const errorMessage = `Failed to generate Proven bundle: ${error instanceof Error ? error.message : String(error)}`;
        this.error(new Error(errorMessage));
      }
    },
  };
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
      await fs.writeFile(output, manifestJson, 'utf-8');
    } catch (error) {
      throw new Error(
        `Failed to write bundle manifest to ${output}: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }
}

/**
 * Logs bundle statistics to the console
 */
function logBundleStats(manifest: any): void {
  const stats = {
    entrypoints: manifest.entrypoints.length,
    handlers: manifest.entrypoints.reduce((sum: number, ep: any) => sum + ep.handlers.length, 0),
    files: manifest.sources.length,
    dependencies: Object.keys(manifest.dependencies.dependencies).length,
    size: formatFileSize(manifest.metadata.bundleSize),
  };

  console.log(
    `📊 Bundle stats: ${stats.entrypoints} entrypoints, ${stats.handlers} handlers, ${stats.files} files, ${stats.dependencies} dependencies (${stats.size})`
  );
}

// Named export for the plugin
export { provenRollupPlugin as proven };

// Default export
export default provenRollupPlugin;
