import { ProvenWebpackPlugin } from '../src/webpack';
import { BundleManifestGenerator } from '../src/index';
import type { BundlerOptions } from '../src/types';

// Mock webpack types since webpack isn't a dependency
interface MockCompiler {
  hooks: {
    emit: {
      tapAsync: jest.Mock;
    };
  };
  options: {
    context: string;
  };
}

interface MockCompilation {
  assets: Record<string, any>;
  emitAsset: jest.Mock;
}

jest.mock('../src/index', () => ({
  BundleManifestGenerator: jest.fn(),
  validateOptions: jest.fn(() => []),
  mergeOptions: jest.fn((options) => options),
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

describe.skip('ProvenWebpackPlugin', () => {
  let plugin: ProvenWebpackPlugin;
  let mockCompiler: jest.Mocked<MockCompiler>;
  let mockCompilation: jest.Mocked<MockCompilation>;
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

    // Setup mock compiler
    mockCompiler = {
      hooks: {
        emit: {
          tapAsync: jest.fn(),
        },
        watchRun: {
          tapAsync: jest.fn(),
        },
      },
      options: {
        context: '/test/project',
        mode: 'production',
      },
      context: '/test/project',
    } as any;

    // Setup mock compilation
    mockCompilation = {
      errors: [],
      warnings: [],
      assets: {},
      emitAsset: jest.fn(),
      getLogger: jest.fn(() => ({
        info: jest.fn(),
        warn: jest.fn(),
        error: jest.fn(),
      })),
    } as any;

    plugin = new ProvenWebpackPlugin();
  });

  describe('constructor', () => {
    it('should create plugin with default options', () => {
      const defaultPlugin = new ProvenWebpackPlugin();
      expect(defaultPlugin).toBeDefined();
    });

    it('should create plugin with custom options', () => {
      const options: BundlerOptions = {
        output: './dist/custom-bundle.json',
        mode: 'production',
        sourceMaps: true,
        includeDevDependencies: false,
      };

      const customPlugin = new ProvenWebpackPlugin(options);
      expect(customPlugin).toBeDefined();
    });
  });

  describe('apply', () => {
    it('should register emit hook', () => {
      plugin.apply(mockCompiler);

      expect(mockCompiler.hooks.emit.tapAsync).toHaveBeenCalledWith(
        'ProvenWebpackPlugin',
        expect.any(Function)
      );
    });

    it('should generate bundle manifest on emit', async () => {
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
        metadata: {
          ...createMockMetadata('production'),
        },
      };

      mockGenerator.generateManifest.mockResolvedValue(mockManifest);

      plugin.apply(mockCompiler);

      // Get the callback function that was registered
      const emitCallback = mockCompiler.hooks.emit.tapAsync.mock.calls[0][1];

      // Create a promise to handle the async callback
      const callbackPromise = new Promise<void>((resolve, reject) => {
        emitCallback(mockCompilation, (error?: Error | null | false) => {
          if (error) reject(error);
          else resolve();
        });
      });

      await callbackPromise;

      expect(mockGenerator.generateManifest).toHaveBeenCalled();
      expect(mockGenerator.validateManifest).toHaveBeenCalledWith(mockManifest);
    });

    it('should emit asset in development mode', async () => {
      const devPlugin = new ProvenWebpackPlugin({
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

      devPlugin.apply(mockCompiler);

      // Get the callback function
      const emitCallback = mockCompiler.hooks.emit.tapAsync.mock.calls[0][1];

      // Execute the callback
      const callbackPromise = new Promise<void>((resolve, reject) => {
        emitCallback(mockCompilation, (error?: Error | null | false) => {
          if (error) reject(error);
          else resolve();
        });
      });

      await callbackPromise;

      expect(mockCompilation.emitAsset).toHaveBeenCalledWith(
        'proven-bundle.json',
        expect.objectContaining({
          source: expect.any(Function),
          size: expect.any(Function),
        })
      );
    });

    it('should write file in production mode', async () => {
      const fs = require('fs/promises');
      const mockWriteFile = jest.spyOn(fs, 'writeFile').mockResolvedValue(undefined);
      const mockMkdir = jest.spyOn(fs, 'mkdir').mockResolvedValue(undefined);

      const prodPlugin = new ProvenWebpackPlugin({
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

      prodPlugin.apply(mockCompiler);

      // Get the callback function
      const emitCallback = mockCompiler.hooks.emit.tapAsync.mock.calls[0][1];

      // Execute the callback
      const callbackPromise = new Promise<void>((resolve, reject) => {
        emitCallback(mockCompilation, (error?: Error | null | false) => {
          if (error) reject(error);
          else resolve();
        });
      });

      await callbackPromise;

      expect(mockWriteFile).toHaveBeenCalledWith(
        '/test/project/dist/bundle.json',
        JSON.stringify(mockManifest, null, 2),
        'utf-8'
      );

      mockWriteFile.mockRestore();
      mockMkdir.mockRestore();
    });

    it('should handle manifest generation errors', async () => {
      const generationError = new Error('Manifest generation failed');
      mockGenerator.generateManifest.mockRejectedValue(generationError);

      plugin.apply(mockCompiler);

      // Get the callback function
      const emitCallback = mockCompiler.hooks.emit.tapAsync.mock.calls[0][1];

      // Execute the callback
      const callbackPromise = new Promise<void>((resolve, reject) => {
        emitCallback(mockCompilation, (error?: Error | null | false) => {
          if (error) reject(error);
          else resolve();
        });
      });

      await expect(callbackPromise).rejects.toThrow('Manifest generation failed');
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

      plugin.apply(mockCompiler);

      // Get the callback function
      const emitCallback = mockCompiler.hooks.emit.tapAsync.mock.calls[0][1];

      // Execute the callback
      const callbackPromise = new Promise<void>((resolve, reject) => {
        emitCallback(mockCompilation, (error?: Error | null | false) => {
          if (error) reject(error);
          else resolve();
        });
      });

      await expect(callbackPromise).rejects.toThrow('Manifest validation failed');
    });

    it('should handle file write errors', async () => {
      const fs = require('fs/promises');
      const writeError = new Error('Failed to write file');
      const mockWriteFile = jest.spyOn(fs, 'writeFile').mockRejectedValue(writeError);
      const mockMkdir = jest.spyOn(fs, 'mkdir').mockResolvedValue(undefined);

      const prodPlugin = new ProvenWebpackPlugin({
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

      prodPlugin.apply(mockCompiler);

      // Get the callback function
      const emitCallback = mockCompiler.hooks.emit.tapAsync.mock.calls[0][1];

      // Execute the callback
      const callbackPromise = new Promise<void>((resolve, reject) => {
        emitCallback(mockCompilation, (error?: Error | null | false) => {
          if (error) reject(error);
          else resolve();
        });
      });

      await expect(callbackPromise).rejects.toThrow('Failed to write file');

      mockWriteFile.mockRestore();
      mockMkdir.mockRestore();
    });

    it('should use compiler context as project root', async () => {
      mockGenerator.generateManifest.mockResolvedValue({
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
      });

      plugin.apply(mockCompiler);

      // Get the callback function
      const emitCallback = mockCompiler.hooks.emit.tapAsync.mock.calls[0][1];

      // Execute the callback
      const callbackPromise = new Promise<void>((resolve, reject) => {
        emitCallback(mockCompilation, (error?: Error | null | false) => {
          if (error) reject(error);
          else resolve();
        });
      });

      await callbackPromise;

      expect(MockBundleManifestGenerator).toHaveBeenCalledWith('/test/project', expect.any(Object));
    });

    it('should merge webpack mode with plugin options', async () => {
      mockCompiler.options.mode = 'development';

      const pluginWithMode = new ProvenWebpackPlugin({
        sourceMaps: true,
      });

      mockGenerator.generateManifest.mockResolvedValue({
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
      });

      pluginWithMode.apply(mockCompiler);

      // Get the callback function
      const emitCallback = mockCompiler.hooks.emit.tapAsync.mock.calls[0][1];

      // Execute the callback
      const callbackPromise = new Promise<void>((resolve, reject) => {
        emitCallback(mockCompilation, (error?: Error | null | false) => {
          if (error) reject(error);
          else resolve();
        });
      });

      await callbackPromise;

      expect(MockBundleManifestGenerator).toHaveBeenCalledWith(
        '/test/project',
        expect.objectContaining({
          mode: 'development',
          sourceMaps: true,
        })
      );
    });
  });
});
