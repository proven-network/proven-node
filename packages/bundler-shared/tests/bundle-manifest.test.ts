import { describe, it, expect, beforeEach, jest } from '@jest/globals';
import { BundleManifestGenerator } from '../src/bundle-manifest';
import { FileCollection } from '../src/file-collection';
import { PackageAnalysis } from '../src/package-analysis';
import type { BundlerOptions } from '../src/types';

jest.mock('../src/file-collection');
jest.mock('../src/package-analysis');

const MockFileCollection = FileCollection as jest.MockedClass<typeof FileCollection>;
const MockPackageAnalysis = PackageAnalysis as jest.MockedClass<typeof PackageAnalysis>;

describe('BundleManifestGenerator', () => {
  let generator: BundleManifestGenerator;
  let mockFileCollection: jest.Mocked<FileCollection>;
  let mockPackageAnalysis: jest.Mocked<PackageAnalysis>;
  const mockProjectRoot = '/test/project';

  beforeEach(() => {
    jest.clearAllMocks();

    // Setup mock file collection
    mockFileCollection = {
      discoverEntrypoints: jest.fn(),
      collectSourceFiles: jest.fn(),
    } as any;
    MockFileCollection.mockImplementation(() => mockFileCollection);

    // Setup mock package analysis
    mockPackageAnalysis = {
      analyzeProject: jest.fn(),
      analyzeDependencies: jest.fn(),
      validateDependencies: jest.fn(),
    } as any;
    MockPackageAnalysis.mockImplementation(() => mockPackageAnalysis);

    const options: BundlerOptions = {
      mode: 'production',
      includeDevDependencies: false,
    };

    generator = new BundleManifestGenerator(mockProjectRoot, options);
  });

  describe('generateManifest', () => {
    it('should generate complete bundle manifest', async () => {
      // Mock project info
      const mockProject = {
        name: 'test-project',
        version: '1.0.0',
        description: 'Test project',
        rootDir: mockProjectRoot,
        packageJson: {
          name: 'test-project',
          version: '1.0.0',
          description: 'Test project',
          dependencies: {
            express: '^4.18.0',
            '@proven-network/handler': '^1.0.0',
          },
        },
      };

      // Mock dependencies
      const mockDependencies = {
        production: {
          express: '^4.18.0',
          '@proven-network/handler': '^1.0.0',
        },
        development: {},
        all: {
          express: '^4.18.0',
          '@proven-network/handler': '^1.0.0',
        },
      };

      // Mock entrypoints
      const mockEntrypoints = [
        {
          filePath: '/test/project/src/handler.ts',
          moduleSpecifier: './src/handler',
          handlers: [
            {
              name: 'handler',
              type: 'http' as const,
              parameters: [],
              config: {
                path: '/api/test',
                method: 'GET',
              },
            },
          ],
          imports: [
            {
              module: '@proven-network/handler',
              type: 'named' as const,
              imports: ['runOnHttp'],
            },
          ],
        },
      ];

      // Mock source files
      const mockSources = [
        {
          filePath: '/test/project/src/handler.ts',
          relativePath: 'src/handler.ts',
          content: `
          import { runOnHttp } from '@proven-network/handler';
          
          export const handler = runOnHttp(
            { path: '/api/test', method: 'GET' },
            (req) => ({ message: 'Hello' })
          );
        `,
          size: 150,
        },
      ];

      // Setup mocks
      mockPackageAnalysis.analyzeProject.mockResolvedValue(mockProject);
      mockPackageAnalysis.analyzeDependencies.mockResolvedValue(mockDependencies);
      mockFileCollection.discoverEntrypoints.mockResolvedValue(mockEntrypoints);
      mockFileCollection.collectSourceFiles.mockResolvedValue(mockSources);

      const result = await generator.generateManifest();

      expect(result).toBeDefined();
      expect(result.project).toEqual({
        name: mockProject.name,
        version: mockProject.version,
        description: mockProject.description,
        dependencies: mockProject.packageJson.dependencies,
      });
      expect(result.dependencies).toEqual(mockDependencies);
      expect(result.entrypoints).toEqual(mockEntrypoints);
      expect(result.sources).toEqual([
        {
          relativePath: mockSources[0].relativePath,
          content: mockSources[0].content,
          size: mockSources[0].size,
        },
      ]);
      expect(result.metadata).toBeDefined();
      expect(result.metadata.buildMode).toBe('production');
      expect(result.metadata.entrypointCount).toBe(1);
      expect(result.metadata.handlerCount).toBe(1);
    });

    it('should handle development mode', async () => {
      const devOptions: BundlerOptions = {
        mode: 'development',
        includeDevDependencies: true,
        sourceMaps: true,
      };

      const devGenerator = new BundleManifestGenerator(mockProjectRoot, devOptions);

      // Mock minimal data
      mockPackageAnalysis.analyzeProject.mockResolvedValue({
        name: 'test-project',
        version: '1.0.0',
        description: 'Test project',
        rootDir: mockProjectRoot,
        packageJson: { name: 'test-project', version: '1.0.0' },
      });

      mockPackageAnalysis.analyzeDependencies.mockResolvedValue({
        production: {},
        development: { typescript: '^5.0.0' },
        all: { typescript: '^5.0.0' },
      });

      // Mock an entrypoint to avoid the "no entrypoints found" error
      mockFileCollection.discoverEntrypoints.mockResolvedValue([
        {
          filePath: '/test/project/src/handler.ts',
          moduleSpecifier: './src/handler',
          handlers: [
            {
              name: 'handler',
              type: 'http' as const,
              parameters: [],
            },
          ],
          imports: [],
        },
      ]);
      mockFileCollection.collectSourceFiles.mockResolvedValue([
        {
          filePath: '/test/project/src/handler.ts',
          relativePath: 'src/handler.ts',
          content: 'test content',
          size: 12,
        },
      ]);

      const result = await devGenerator.generateManifest();

      expect(result.metadata.buildMode).toBe('development');
      expect(result.metadata.sourceMaps).toBe(true);
    });

    it('should calculate correct statistics', async () => {
      const mockEntrypoints = [
        {
          filePath: '/test/project/src/handler1.ts',
          moduleSpecifier: './src/handler1',
          handlers: [
            { name: 'httpHandler', type: 'http' as const, parameters: [] },
            { name: 'scheduleHandler', type: 'schedule' as const, parameters: [] },
          ],
          imports: [],
        },
        {
          filePath: '/test/project/src/handler2.ts',
          moduleSpecifier: './src/handler2',
          handlers: [{ name: 'eventHandler', type: 'event' as const, parameters: [] }],
          imports: [],
        },
      ];

      const mockSources = [
        {
          filePath: '/test/project/src/handler1.ts',
          relativePath: 'src/handler1.ts',
          content: 'handler1 content',
          size: 100,
        },
        {
          filePath: '/test/project/src/handler2.ts',
          relativePath: 'src/handler2.ts',
          content: 'handler2 content',
          size: 200,
        },
      ];

      mockPackageAnalysis.analyzeProject.mockResolvedValue({
        name: 'test-project',
        version: '1.0.0',
        description: 'Test project',
        rootDir: mockProjectRoot,
        packageJson: { name: 'test-project', version: '1.0.0' },
      });

      mockPackageAnalysis.analyzeDependencies.mockResolvedValue({
        production: {},
        development: {},
        all: {},
      });

      mockFileCollection.discoverEntrypoints.mockResolvedValue(mockEntrypoints);
      mockFileCollection.collectSourceFiles.mockResolvedValue(mockSources);

      const result = await generator.generateManifest();

      expect(result.metadata.entrypointCount).toBe(2);
      expect(result.metadata.handlerCount).toBe(3);
      expect(result.metadata.fileCount).toBe(2);
    });

    it('should handle empty project', async () => {
      mockPackageAnalysis.analyzeProject.mockResolvedValue({
        name: 'empty-project',
        version: '1.0.0',
        description: 'Empty project',
        rootDir: mockProjectRoot,
        packageJson: { name: 'empty-project', version: '1.0.0' },
      });

      mockPackageAnalysis.analyzeDependencies.mockResolvedValue({
        production: {},
        development: {},
        all: {},
      });

      mockFileCollection.discoverEntrypoints.mockResolvedValue([]);
      mockFileCollection.collectSourceFiles.mockResolvedValue([]);

      await expect(generator.generateManifest()).rejects.toThrow(
        'No entrypoints found. Make sure your project contains files that import @proven-network/handler'
      );
    });

    it('should handle errors gracefully', async () => {
      mockPackageAnalysis.analyzeProject.mockRejectedValue(new Error('Package analysis failed'));

      await expect(generator.generateManifest()).rejects.toThrow('Package analysis failed');
    });
  });

  describe('validateManifest', () => {
    it('should validate valid manifest', async () => {
      const validManifest = {
        id: 'test-manifest-id',
        version: '1.0.0',
        project: {
          name: 'test-project',
          version: '1.0.0',
          description: 'Test project',
        },
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
        modules: [],
        dependencies: {
          production: {},
          development: {},
          all: {},
        },
        metadata: {
          createdAt: new Date().toISOString(),
          mode: 'production' as const,
          pluginVersion: '1.0.0',
          fileCount: 1,
          bundleSize: 100,
          sourceMaps: false,
          buildMode: 'production',
          entrypointCount: 1,
          handlerCount: 1,
        },
      };

      mockPackageAnalysis.validateDependencies.mockResolvedValue([]);
      const errors = await generator.validateManifest(validManifest);
      expect(errors).toHaveLength(0);
    });

    it('should reject manifest with missing required fields', async () => {
      const invalidManifest = {
        id: 'test-manifest-id',
        version: '1.0.0',
        project: {
          name: 'test-project',
          version: '1.0.0',
          description: 'Test project',
        },
        entrypoints: [],
        sources: [],
        modules: [],
        dependencies: {
          production: {},
          development: {},
          all: {},
        },
        metadata: {
          createdAt: new Date().toISOString(),
          mode: 'production' as const,
          pluginVersion: '1.0.0',
          fileCount: 0,
          bundleSize: 0,
          sourceMaps: false,
          buildMode: 'production',
          entrypointCount: 0,
          handlerCount: 0,
        },
      };

      mockPackageAnalysis.validateDependencies.mockResolvedValue([]);
      const errors = await generator.validateManifest(invalidManifest as any);
      expect(errors).toContain('No entrypoints found');
    });

    it('should reject manifest with invalid entrypoint', async () => {
      const invalidManifest = {
        id: 'test-manifest-id',
        version: '1.0.0',
        project: {
          name: 'test-project',
          version: '1.0.0',
          description: 'Test project',
        },
        entrypoints: [
          {
            filePath: '/test/project/src/handler.ts',
            moduleSpecifier: './src/handler',
            handlers: [],
            imports: [],
          },
        ],
        sources: [],
        modules: [],
        dependencies: {
          production: {},
          development: {},
          all: {},
        },
        metadata: {
          createdAt: new Date().toISOString(),
          mode: 'production' as const,
          pluginVersion: '1.0.0',
          fileCount: 0,
          bundleSize: 0,
          sourceMaps: false,
          buildMode: 'production',
          entrypointCount: 1,
          handlerCount: 0,
        },
      };

      mockPackageAnalysis.validateDependencies.mockResolvedValue([]);
      const errors = await generator.validateManifest(invalidManifest as any);
      expect(errors).toContain('Entrypoint ./src/handler has no handlers');
    });
  });
});
