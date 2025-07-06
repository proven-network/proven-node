import { Plugin } from 'vite';
import {
  BundlerOptions,
  BundleManifestGenerator,
  DevWrapperGenerator,
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
 * Vite plugin for bundling Proven Network applications
 */
export function provenVitePlugin(options: BundlerOptions = {}): Plugin {
  // Validate options
  const validationErrors = validateOptions(options);
  if (validationErrors.length > 0) {
    throw new Error(`Invalid options: ${validationErrors.join(', ')}`);
  }

  const mergedOptions = mergeOptions(options);
  let generator: BundleManifestGenerator;
  let projectRoot: string;

  return {
    name: 'proven-vite-plugin',

    configResolved(config) {
      projectRoot = config.root || process.cwd();

      // Merge Vite config with plugin options
      const viteMode = config.command === 'build' ? 'production' : 'development';
      const resolvedOptions = {
        ...mergedOptions,
        mode: mergedOptions.mode || viteMode,
      };

      // Initialize the generator
      generator = new BundleManifestGenerator(projectRoot, resolvedOptions);

      console.log('🔍 Proven Vite Plugin initialized');
    },

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
        // Create a minimal manifest for transformation
        const manifestId = `bundle-${Date.now()}`;

        // Transform the code
        const result = await transformHandlers(code, {
          manifest: {
            id: manifestId,
            version: '1.0',
            modules: [],
            dependencies: {},
          },
          manifestId,
          filePath: `file://${id}`,
          sourceMap: true,
        });

        console.log(`✅ Transformed handlers in ${path.relative(projectRoot, id)}`);

        return {
          code: result.code,
          map: result.map,
        };
      } catch (error) {
        console.warn(`Failed to transform handlers in ${id}:`, error);
        return null;
      }
    },

    async generateBundle() {
      try {
        console.log('🔍 Generating Proven bundle manifest...');

        const manifest = await generator.generateManifest();

        // Validate the manifest
        const validationErrors = await generator.validateManifest(manifest);
        if (validationErrors.length > 0) {
          const errorMessage = `Bundle validation failed:\n${validationErrors.join('\n')}`;
          this.error(errorMessage);
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
        this.error(errorMessage);
      }
    },
  };
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
    // Emit as Vite asset for development
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
        `Failed to write bundle manifest to ${outputPath}: ${error instanceof Error ? error.message : String(error)}`
      );
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
export { provenVitePlugin as proven };

// Default export
export default provenVitePlugin;
