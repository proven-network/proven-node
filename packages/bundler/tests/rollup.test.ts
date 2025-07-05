import { describe, it, expect, beforeEach, jest } from '@jest/globals';
import { provenRollupPlugin } from '../src/rollup';
import { BundleManifestGenerator } from '../src/index';
import type { Plugin, InputOptions, OutputOptions, OutputBundle } from 'rollup';
import type { BundlerOptions } from '../src/types';

jest.mock('../src/index', () => ({
  BundleManifestGenerator: jest.fn(),
  validateOptions: jest.fn(() => []),
  mergeOptions: jest.fn((options) => options),
  formatFileSize: jest.fn((size) => `${size} bytes`),
}));

const MockBundleManifestGenerator = BundleManifestGenerator as jest.MockedClass<
  typeof BundleManifestGenerator
>;

// Helper to create valid metadata mock
const createMockMetadata = (mode: 'development' | 'production') => ({
  createdAt: new Date().toISOString(),
  mode,
  pluginVersion: '1.0.0',
  fileCount: 1,
  bundleSize: 1000,
  sourceMaps: false,
  buildMode: mode,
  entrypointCount: 1,
  handlerCount: 1,
  sourceFileCount: 1,
  totalSourceSize: 100,
  includeDevDependencies: false,
  generatedAt: new Date().toISOString(),
});

// Helper to initialize plugin by calling buildStart
const initializePlugin = (plugin: any, mockInputOptions: any) => {
  const buildStart = plugin.buildStart;
  if (buildStart) {
    const mockPluginContext = {
      error: jest.fn(),
      warn: jest.fn(),
      emitFile: jest.fn(),
    } as any;
    if (typeof buildStart === 'function') {
      buildStart.call(mockPluginContext, mockInputOptions as any);
    } else if (buildStart && 'handler' in buildStart) {
      buildStart.handler.call(mockPluginContext, mockInputOptions as any);
    }
  }
};

describe('provenRollupPlugin', () => {
  let mockGenerator: jest.Mocked<BundleManifestGenerator>;

  beforeEach(() => {
    jest.clearAllMocks();

    // Setup mock generator
    mockGenerator = {
      generateManifest: jest.fn(),
      validateManifest: jest.fn().mockReturnValue([]),
      optimizeManifest: jest.fn((manifest) => manifest),
      serializeManifest: jest.fn((manifest) => JSON.stringify(manifest, null, 2)),
    } as any;
    MockBundleManifestGenerator.mockImplementation(() => mockGenerator);
  });

  describe('plugin creation', () => {
    it('should create plugin with default options', () => {
      const plugin = provenRollupPlugin();

      expect(plugin).toBeDefined();
      expect(plugin.name).toBe('proven-rollup-plugin');
    });

    it('should create plugin with custom options', () => {
      const options: BundlerOptions = {
        output: './dist/custom-bundle.json',
        mode: 'production',
        sourceMaps: true,
        includeDevDependencies: false,
      };

      const plugin = provenRollupPlugin(options);

      expect(plugin).toBeDefined();
      expect(plugin.name).toBe('proven-rollup-plugin');
    });
  });

  describe('plugin hooks', () => {
    let plugin: Plugin;
    let mockInputOptions: InputOptions;
    let mockOutputOptions: OutputOptions;
    let mockBundle: OutputBundle;

    beforeEach(() => {
      plugin = provenRollupPlugin({
        mode: 'production',
      });

      mockInputOptions = {
        input: 'src/index.ts',
      };

      mockOutputOptions = {
        dir: 'dist',
        format: 'es',
      };

      mockBundle = {
        'main.js': {
          type: 'chunk',
          fileName: 'main.js',
          name: 'main',
          code: 'export default "test";',
          map: null,
          isEntry: true,
          isDynamicEntry: false,
          isImplicitEntry: false,
          facadeModuleId: null,
          moduleIds: [],
          modules: {},
          imports: [],
          dynamicImports: [],
          exports: [],
          implicitlyLoadedBefore: [],
          importedBindings: {},
          referencedFiles: [],
          sourcemapFileName: null,
          preliminaryFileName: 'main.js',
        } as any,
      };
    });

    describe('buildStart', () => {
      it('should initialize plugin', () => {
        const buildStart = plugin.buildStart;

        expect(buildStart).toBeDefined();
        expect(typeof buildStart).toBe('function');

        // Call buildStart if it exists
        if (buildStart) {
          const mockPluginContext = {
            error: jest.fn(),
            warn: jest.fn(),
            emitFile: jest.fn(),
          } as any;
          if (typeof buildStart === 'function') {
            buildStart.call(mockPluginContext, mockInputOptions as any);
          } else if (buildStart && 'handler' in buildStart) {
            buildStart.handler.call(mockPluginContext, mockInputOptions as any);
          }
        }
      });
    });

    describe('generateBundle', () => {
      it('should generate bundle manifest', async () => {
        const mockManifest = {
          id: 'test-manifest-id',
          version: '1.0.0',
          project: {
            name: 'test-project',
            version: '1.0.0',
            description: 'Test project',
          },
          modules: [],
          entrypoints: [
            {
              filePath: '/test/project/src/handler.ts',
              moduleSpecifier: './src/handler',
              handlers: [
                {
                  name: 'handler',
                  type: 'http' as const,
                  parameters: [],
                  config: { path: '/api/test' },
                },
              ],
              imports: [],
            },
          ],
          sources: [
            {
              relativePath: 'src/handler.ts',
              content: 'handler content',
              size: 100,
            },
          ],
          dependencies: {
            production: {},
            development: {},
            all: {},
          },
          metadata: createMockMetadata('production'),
        };

        mockGenerator.generateManifest.mockResolvedValue(mockManifest);

        // Initialize plugin by calling buildStart
        initializePlugin(plugin, mockInputOptions);

        const generateBundle = plugin.generateBundle;

        expect(generateBundle).toBeDefined();
        expect(typeof generateBundle).toBe('function');

        if (generateBundle) {
          const bundleHandler =
            typeof generateBundle === 'function' ? generateBundle : generateBundle.handler;
          const mockPluginContext = {
            error: jest.fn(),
            warn: jest.fn(),
            emitFile: jest.fn(),
          } as any;
          await bundleHandler.call(mockPluginContext, mockOutputOptions as any, mockBundle, true);
        }

        expect(mockGenerator.generateManifest).toHaveBeenCalled();
        expect(mockGenerator.validateManifest).toHaveBeenCalledWith(mockManifest);
      });

      it('should emit bundle as asset in development mode', async () => {
        const devPlugin = provenRollupPlugin({
          output: 'development',
          mode: 'development',
        });

        const mockManifest = {
          id: 'test-manifest-id',
          version: '1.0.0',
          project: {
            name: 'test-project',
            version: '1.0.0',
            description: 'Test project',
          },
          modules: [],
          entrypoints: [],
          sources: [],
          dependencies: {
            production: {},
            development: {},
            all: {},
          },
          metadata: {
            ...createMockMetadata('development'),
          },
        };

        mockGenerator.generateManifest.mockResolvedValue(mockManifest);

        // Initialize plugin by calling buildStart
        initializePlugin(devPlugin, mockInputOptions);

        const emitFile = jest.fn();
        const pluginContext = {
          emitFile,
          getFileName: jest.fn(),
          warn: jest.fn(),
          error: jest.fn(),
        };

        const generateBundle = devPlugin.generateBundle;

        if (generateBundle) {
          const bundleHandler =
            typeof generateBundle === 'function' ? generateBundle : generateBundle.handler;
          await bundleHandler.call(
            pluginContext as any,
            mockOutputOptions as any,
            mockBundle,
            true
          );
        }

        expect(emitFile).toHaveBeenCalledWith({
          type: 'asset',
          fileName: 'proven-bundle.json',
          source: JSON.stringify(mockManifest, null, 2),
        });
      });

      it('should write file in production mode', async () => {
        const fs = require('fs/promises');
        const mockWriteFile = jest.spyOn(fs, 'writeFile').mockResolvedValue(undefined);

        const prodPlugin = provenRollupPlugin({
          output: './dist/bundle.json',
          mode: 'production',
        });

        const mockManifest = {
          id: 'test-manifest-id',
          version: '1.0.0',
          project: {
            name: 'test-project',
            version: '1.0.0',
            description: 'Test project',
          },
          modules: [],
          entrypoints: [],
          sources: [],
          dependencies: {
            production: {},
            development: {},
            all: {},
          },
          metadata: {
            ...createMockMetadata('production'),
          },
        };

        mockGenerator.generateManifest.mockResolvedValue(mockManifest);

        // Initialize plugin by calling buildStart
        initializePlugin(prodPlugin, mockInputOptions);

        const generateBundle = prodPlugin.generateBundle;

        if (generateBundle) {
          const bundleHandler =
            typeof generateBundle === 'function' ? generateBundle : generateBundle.handler;
          const mockPluginContext = {
            error: jest.fn(),
            warn: jest.fn(),
            emitFile: jest.fn(),
          } as any;
          await bundleHandler.call(mockPluginContext, mockOutputOptions as any, mockBundle, true);
        }

        expect(mockWriteFile).toHaveBeenCalledWith(
          './dist/bundle.json',
          JSON.stringify(mockManifest, null, 2),
          'utf-8'
        );

        mockWriteFile.mockRestore();
      });

      it('should handle manifest generation errors', async () => {
        const generationError = new Error('Manifest generation failed');
        mockGenerator.generateManifest.mockRejectedValue(generationError);

        // Initialize plugin by calling buildStart
        initializePlugin(plugin, mockInputOptions);

        const errorFn = jest.fn();
        const pluginContext = {
          error: errorFn,
          warn: jest.fn(),
          emitFile: jest.fn(),
        };

        const generateBundle = plugin.generateBundle;

        if (generateBundle) {
          const bundleHandler =
            typeof generateBundle === 'function' ? generateBundle : generateBundle.handler;
          await bundleHandler.call(
            pluginContext as any,
            mockOutputOptions as any,
            mockBundle,
            true
          );
        }

        expect(errorFn).toHaveBeenCalledWith(
          expect.objectContaining({
            message: 'Failed to generate Proven bundle: Manifest generation failed',
          })
        );
      });

      it('should handle validation errors', async () => {
        const mockManifest = {
          id: 'test-manifest-id',
          version: '1.0.0',
          project: {
            name: 'test-project',
            version: '1.0.0',
            description: 'Test project',
          },
          modules: [],
          entrypoints: [],
          sources: [],
          dependencies: {
            production: {},
            development: {},
            all: {},
          },
          metadata: {
            ...createMockMetadata('production'),
          },
        };

        const validationError = new Error('Manifest validation failed');
        mockGenerator.generateManifest.mockResolvedValue(mockManifest);
        mockGenerator.validateManifest.mockImplementation(() => {
          throw validationError;
        });

        // Initialize plugin by calling buildStart
        initializePlugin(plugin, mockInputOptions);

        const errorFn = jest.fn();
        const pluginContext = {
          error: errorFn,
          warn: jest.fn(),
          emitFile: jest.fn(),
        };

        const generateBundle = plugin.generateBundle;

        if (generateBundle) {
          const bundleHandler =
            typeof generateBundle === 'function' ? generateBundle : generateBundle.handler;
          await bundleHandler.call(
            pluginContext as any,
            mockOutputOptions as any,
            mockBundle,
            true
          );
        }

        expect(errorFn).toHaveBeenCalledWith(
          expect.objectContaining({
            message: 'Failed to generate Proven bundle: Manifest validation failed',
          })
        );
      });

      it('should handle file write errors', async () => {
        const fs = require('fs/promises');
        const writeError = new Error('Failed to write file');
        const mockWriteFile = jest.spyOn(fs, 'writeFile').mockRejectedValue(writeError);

        const prodPlugin = provenRollupPlugin({
          output: './dist/bundle.json',
          mode: 'production',
        });

        const mockManifest = {
          id: 'test-manifest-id',
          version: '1.0.0',
          project: {
            name: 'test-project',
            version: '1.0.0',
            description: 'Test project',
          },
          modules: [],
          entrypoints: [],
          sources: [],
          dependencies: {
            production: {},
            development: {},
            all: {},
          },
          metadata: {
            ...createMockMetadata('production'),
          },
        };

        mockGenerator.generateManifest.mockResolvedValue(mockManifest);

        // Initialize plugin by calling buildStart
        initializePlugin(prodPlugin, mockInputOptions);

        const errorFn = jest.fn();
        const pluginContext = {
          error: errorFn,
          warn: jest.fn(),
          emitFile: jest.fn(),
        };

        const generateBundle = prodPlugin.generateBundle;

        if (generateBundle) {
          const bundleHandler =
            typeof generateBundle === 'function' ? generateBundle : generateBundle.handler;
          await bundleHandler.call(
            pluginContext as any,
            mockOutputOptions as any,
            mockBundle,
            true
          );
        }

        expect(errorFn).toHaveBeenCalledWith(
          expect.objectContaining({
            message:
              'Failed to generate Proven bundle: Failed to write bundle manifest to ./dist/bundle.json: Failed to write file',
          })
        );

        mockWriteFile.mockRestore();
      });

      it('should use process.cwd() as project root', async () => {
        const mockManifest = {
          id: 'test-manifest-id',
          version: '1.0.0',
          project: {
            name: 'test-project',
            version: '1.0.0',
            description: 'Test project',
          },
          modules: [],
          entrypoints: [],
          sources: [],
          dependencies: {
            production: {},
            development: {},
            all: {},
          },
          metadata: {
            ...createMockMetadata('production'),
          },
        };

        mockGenerator.generateManifest.mockResolvedValue(mockManifest);

        // Initialize plugin by calling buildStart
        initializePlugin(plugin, mockInputOptions);

        const generateBundle = plugin.generateBundle;

        if (generateBundle) {
          const bundleHandler =
            typeof generateBundle === 'function' ? generateBundle : generateBundle.handler;
          const mockPluginContext = {
            error: jest.fn(),
            warn: jest.fn(),
            emitFile: jest.fn(),
          } as any;
          await bundleHandler.call(mockPluginContext, mockOutputOptions as any, mockBundle, true);
        }

        expect(MockBundleManifestGenerator).toHaveBeenCalledWith(process.cwd(), expect.any(Object));
      });

      it('should merge options correctly', async () => {
        const customPlugin = provenRollupPlugin({
          sourceMaps: true,
          includeDevDependencies: true,
        });

        const mockManifest = {
          id: 'test-manifest-id',
          version: '1.0.0',
          project: {
            name: 'test-project',
            version: '1.0.0',
            description: 'Test project',
          },
          modules: [],
          entrypoints: [],
          sources: [],
          dependencies: {
            production: {},
            development: {},
            all: {},
          },
          metadata: {
            ...createMockMetadata('development'),
          },
        };

        mockGenerator.generateManifest.mockResolvedValue(mockManifest);

        // Initialize plugin by calling buildStart
        initializePlugin(customPlugin, mockInputOptions);

        const generateBundle = customPlugin.generateBundle;

        if (generateBundle) {
          const bundleHandler =
            typeof generateBundle === 'function' ? generateBundle : generateBundle.handler;
          const mockPluginContext = {
            error: jest.fn(),
            warn: jest.fn(),
            emitFile: jest.fn(),
          } as any;
          await bundleHandler.call(mockPluginContext, mockOutputOptions as any, mockBundle, true);
        }

        expect(MockBundleManifestGenerator).toHaveBeenCalledWith(
          process.cwd(),
          expect.objectContaining({
            sourceMaps: true,
            includeDevDependencies: true,
          })
        );
      });
    });
  });

  describe('Vite integration', () => {
    it('should work with Vite configuration', () => {
      const vitePlugin = provenRollupPlugin({
        output: './dist/vite-bundle.json',
        mode: 'production',
      });

      expect(vitePlugin).toBeDefined();
      expect(vitePlugin.name).toBe('proven-rollup-plugin');
    });

    it('should handle Vite development mode', () => {
      const viteDevPlugin = provenRollupPlugin({
        output: 'development',
        mode: 'development',
        sourceMaps: true,
      });

      expect(viteDevPlugin).toBeDefined();
      expect(viteDevPlugin.name).toBe('proven-rollup-plugin');
    });
  });
});
