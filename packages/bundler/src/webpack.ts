import { Compiler, WebpackPluginInstance } from 'webpack';
import { validate } from 'schema-utils';
import {
  BundlerOptions,
  BundleManifestGenerator,
  mergeOptions,
  validateOptions,
  formatFileSize,
} from './index';
import * as fs from 'fs/promises';
import * as path from 'path';

const schema = {
  type: 'object',
  properties: {
    output: {
      type: 'string',
    },
    entryPatterns: {
      type: 'array',
      items: {
        type: 'string',
      },
    },
    include: {
      type: 'array',
      items: {
        type: 'string',
      },
    },
    exclude: {
      type: 'array',
      items: {
        type: 'string',
      },
    },
    sourceMaps: {
      type: 'boolean',
    },
    mode: {
      type: 'string',
      enum: ['development', 'production'],
    },
    packageJsonPath: {
      type: 'string',
    },
    includeDevDependencies: {
      type: 'boolean',
    },
  },
  additionalProperties: false,
};

/**
 * Webpack plugin for bundling Proven Network applications
 */
export class ProvenWebpackPlugin implements WebpackPluginInstance {
  private readonly options: BundlerOptions;
  private readonly generator: BundleManifestGenerator;

  constructor(options: BundlerOptions = {}) {
    // Validate options
    validate(schema as any, options, {
      name: 'ProvenWebpackPlugin',
      baseDataPath: 'options',
    });

    const validationErrors = validateOptions(options);
    if (validationErrors.length > 0) {
      throw new Error(`Invalid options: ${validationErrors.join(', ')}`);
    }

    this.options = mergeOptions(options);

    // We'll set the generator later when we have access to the webpack context
    this.generator = null as any;
  }

  apply(compiler: Compiler): void {
    const pluginName = 'ProvenWebpackPlugin';
    const projectRoot = compiler.context || process.cwd();

    // Merge webpack compiler options with plugin options
    const webpackMode =
      compiler.options.mode === 'development' || compiler.options.mode === 'production'
        ? compiler.options.mode
        : 'development';
    const optionsWithMode: BundlerOptions = {
      ...this.options,
      mode: this.options.mode || webpackMode,
    };
    const mergedOptions = mergeOptions(optionsWithMode);

    // Initialize the generator with the project root
    (this as any).generator = new BundleManifestGenerator(projectRoot, mergedOptions);

    // Hook into the compilation process
    compiler.hooks.emit.tapAsync(pluginName, async (compilation, callback) => {
      try {
        await this.generateBundle(compilation, projectRoot);
        callback();
      } catch (error) {
        callback(error as Error);
      }
    });

    // For development mode, also set up watching
    if (this.options.mode === 'development' && this.options.output === 'development') {
      this.setupDevelopmentMode(compiler);
    }
  }

  /**
   * Generates the bundle manifest and emits it
   */
  private async generateBundle(compilation: any, projectRoot: string): Promise<void> {
    try {
      console.log('🔍 Generating Proven bundle manifest...');

      const manifest = await this.generator.generateManifest();

      // Validate the manifest
      const validationErrors = await this.generator.validateManifest(manifest);
      if (validationErrors.length > 0) {
        const errorMessage = `Bundle validation failed:\n${validationErrors.join('\n')}`;
        throw new Error(errorMessage);
      }

      // Optimize for production if needed
      const finalManifest = this.generator.optimizeManifest(manifest);

      // Serialize the manifest
      const manifestJson = this.generator.serializeManifest(finalManifest);

      // Handle output
      await this.handleOutput(manifestJson, compilation, projectRoot);

      console.log('✅ Proven bundle manifest generated successfully');
      this.logBundleStats(finalManifest);
    } catch (error) {
      const errorMessage = `Failed to generate Proven bundle: ${error instanceof Error ? error.message : String(error)}`;
      throw new Error(errorMessage);
    }
  }

  /**
   * Handles the output of the bundle manifest
   */
  private async handleOutput(
    manifestJson: string,
    compilation: any,
    projectRoot: string
  ): Promise<void> {
    const output = this.options.output || 'development';

    if (output === 'development') {
      // Emit as webpack asset for development
      compilation.emitAsset('proven-bundle.json', {
        source: () => manifestJson,
        size: () => manifestJson.length,
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
   * Sets up development mode with file watching
   */
  private setupDevelopmentMode(compiler: Compiler): void {
    compiler.hooks.watchRun.tapAsync('ProvenWebpackPlugin', async (_compiler, callback) => {
      // In development mode, we can hook into webpack's file watching
      // and regenerate the bundle when relevant files change
      callback();
    });
  }

  /**
   * Logs bundle statistics to the console
   */
  private logBundleStats(manifest: any): void {
    const stats = {
      entrypoints: manifest.entrypoints.length,
      handlers: manifest.entrypoints.reduce((sum: number, ep: any) => sum + ep.handlers.length, 0),
      files: manifest.sources.length,
      dependencies: Object.keys(manifest.dependencies.all).length,
      size: formatFileSize(manifest.metadata.bundleSize),
    };

    console.log(
      `📊 Bundle stats: ${stats.entrypoints} entrypoints, ${stats.handlers} handlers, ${stats.files} files, ${stats.dependencies} dependencies (${stats.size})`
    );
  }
}

// Default export for easier importing
export default ProvenWebpackPlugin;
