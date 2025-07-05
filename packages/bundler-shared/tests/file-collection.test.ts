import { describe, it, expect, beforeEach, jest } from '@jest/globals';
import { FileCollection } from '../src/file-collection';
import { EntrypointDiscovery } from '../src/entrypoint-discovery';
import * as fs from 'fs';
import * as path from 'path';
import { glob } from 'fast-glob';

jest.mock('fs', () => ({
  promises: {
    readFile: jest.fn(),
    stat: jest.fn(),
    readdir: jest.fn(),
  },
}));
jest.mock('path');
jest.mock('fast-glob');
jest.mock('../src/entrypoint-discovery');

const mockFs = fs.promises as jest.Mocked<typeof fs.promises>;
const mockPath = path as jest.Mocked<typeof path>;
const mockGlob = glob as jest.MockedFunction<typeof glob>;
const MockEntrypointDiscovery = EntrypointDiscovery as jest.MockedClass<typeof EntrypointDiscovery>;

describe('FileCollection', () => {
  let collection: FileCollection;
  let mockDiscovery: jest.Mocked<EntrypointDiscovery>;
  const mockProjectRoot = '/test/project';

  beforeEach(() => {
    jest.clearAllMocks();

    // Setup glob mock
    mockGlob.mockResolvedValue([
      '/test/project/src/handler.ts',
      '/test/project/src/api/users.ts',
      '/test/project/src/utils.ts',
    ]);

    // Setup path mocks
    mockPath.resolve.mockImplementation((...args) => {
      // Simple path resolution that handles ./
      let resolved = args.join('/');
      // Remove ./
      resolved = resolved.replace(/\/\.\//g, '/');
      // Handle .. (parent directory)
      while (resolved.includes('/../')) {
        resolved = resolved.replace(/\/[^\/]+\/\.\.\//, '/');
      }
      return resolved;
    });
    mockPath.join.mockImplementation((...args) => args.join('/'));
    mockPath.relative.mockImplementation((from, to) => to.replace(from + '/', ''));
    mockPath.extname.mockImplementation((p) => {
      const parts = p.split('.');
      return parts.length > 1 ? '.' + parts[parts.length - 1] : '';
    });
    mockPath.dirname.mockImplementation((p) => p.split('/').slice(0, -1).join('/'));
    mockPath.normalize.mockImplementation((p) => p);
    mockPath.parse.mockImplementation((p) => {
      const parts = p.split('/');
      const name = parts[parts.length - 1];
      const nameWithoutExt = name.split('.')[0];
      const ext = name.includes('.') ? '.' + name.split('.').slice(1).join('.') : '';
      return {
        root: '/',
        dir: parts.slice(0, -1).join('/'),
        base: name,
        ext,
        name: nameWithoutExt,
      };
    });

    // Setup mock discovery
    mockDiscovery = {
      analyzeFile: jest.fn(),
    } as any;
    MockEntrypointDiscovery.mockImplementation(() => mockDiscovery);

    collection = new FileCollection(mockProjectRoot, {
      include: ['**/*.{ts,tsx,js,jsx}'],
      exclude: ['node_modules/**', '**/*.test.ts'],
    });
  });

  describe('discoverEntrypoints', () => {
    it('should discover entrypoints from file system', async () => {
      const mockFiles = [
        '/test/project/src/handler.ts',
        '/test/project/src/api/users.ts',
        '/test/project/src/utils.ts',
        '/test/project/src/handler.test.ts', // Should be excluded
      ];

      mockFs.readdir.mockImplementation(async (dir) => {
        if (dir === '/test/project/src') {
          return [{ name: 'handler.ts', isFile: () => true, isDirectory: () => false }] as any;
        }
        if (dir === '/test/project/src/api') {
          return [{ name: 'users.ts', isFile: () => true, isDirectory: () => false }] as any;
        }
        return [] as any;
      });

      mockFs.stat.mockImplementation(
        async (filePath) =>
          ({
            isFile: () => !filePath.toString().includes('node_modules'),
            isDirectory: () => false,
          }) as any
      );

      // Mock entrypoint analysis
      mockDiscovery.analyzeFile.mockImplementation(async (filePath) => {
        if (filePath.includes('handler.ts')) {
          return {
            filePath,
            moduleSpecifier: './src/handler',
            handlers: [
              {
                name: 'handler',
                type: 'http' as const,
                config: { path: '/api/test' },
              },
            ],
            imports: [
              {
                module: '@proven-network/handler',
                type: 'named' as const,
                imports: ['runOnHttp'],
                isProvenHandler: true,
              },
              {
                module: './utils',
                type: 'named' as const,
                imports: ['helper'],
                isProvenHandler: false,
              },
            ],
          };
        }
        if (filePath.includes('utils.ts')) {
          return null; // utils.ts is not an entrypoint
        }
        return null;
      });

      const result = await collection.discoverEntrypoints();

      expect(result).toHaveLength(1);
      expect(result[0].filePath).toContain('handler.ts');
      expect(result[0].handlers).toHaveLength(1);
      expect(result[0].handlers[0].type).toBe('http');
    });

    it('should handle glob patterns for file discovery', async () => {
      // Override glob mock to return no files
      mockGlob.mockResolvedValue([]);

      const result = await collection.discoverEntrypoints();

      expect(mockGlob).toHaveBeenCalledWith(['**/*.{ts,tsx,js,jsx}'], {
        cwd: mockProjectRoot,
        ignore: ['node_modules/**', '**/*.test.ts'],
        absolute: true,
        onlyFiles: true,
      });
      expect(result).toHaveLength(0);
    });

    it('should respect include/exclude patterns', async () => {
      const collectionWithPatterns = new FileCollection(mockProjectRoot, {
        include: ['src/**/*.ts'],
        exclude: ['**/*.test.ts', '**/*.spec.ts'],
      });

      mockFs.readdir.mockImplementation(async (dir) => {
        if (dir === '/test/project/src') {
          return [
            { name: 'handler.ts', isFile: () => true, isDirectory: () => false },
            { name: 'handler.test.ts', isFile: () => true, isDirectory: () => false },
            { name: 'utils.js', isFile: () => true, isDirectory: () => false },
          ] as any;
        }
        return [] as any;
      });

      mockFs.stat.mockImplementation(
        async (filePath) =>
          ({
            isFile: () => true,
            isDirectory: () => false,
          }) as any
      );

      mockDiscovery.analyzeFile.mockResolvedValue(null);

      const result = await collectionWithPatterns.discoverEntrypoints();

      // Should only analyze .ts files, not .test.ts or .js files
      expect(mockDiscovery.analyzeFile).toHaveBeenCalledWith('/test/project/src/handler.ts');
      expect(mockDiscovery.analyzeFile).not.toHaveBeenCalledWith(
        '/test/project/src/handler.test.ts'
      );
      expect(mockDiscovery.analyzeFile).not.toHaveBeenCalledWith('/test/project/src/utils.js');
    });
  });

  describe('collectSourceFiles', () => {
    it('should collect source files from entrypoints', async () => {
      // Override analyzeFile mock for dependency resolution
      mockDiscovery.analyzeFile.mockImplementation(async (filePath) => {
        if (filePath.includes('handler.ts')) {
          return {
            filePath,
            moduleSpecifier: './src/handler',
            handlers: [
              {
                name: 'handler',
                type: 'http' as const,
                config: { path: '/api/test' },
              },
            ],
            imports: [
              {
                module: '@proven-network/handler',
                type: 'named' as const,
                imports: ['runOnHttp'],
                isProvenHandler: true,
              },
              {
                module: './utils',
                type: 'named' as const,
                imports: ['helper'],
                isProvenHandler: false,
              },
            ],
          };
        }
        if (filePath.includes('utils.ts')) {
          // Return empty analysis for utils.ts (not an entrypoint but analyzable)
          return {
            filePath,
            moduleSpecifier: './src/utils',
            handlers: [],
            imports: [],
          };
        }
        return null;
      });
      const mockEntrypoints = [
        {
          filePath: '/test/project/src/handler.ts',
          moduleSpecifier: './src/handler',
          handlers: [
            {
              name: 'handler',
              type: 'http' as const,
              config: { path: '/api/test' },
            },
          ],
          imports: [
            {
              module: './utils',
              type: 'named' as const,
              imports: ['helper'],
            },
          ],
        },
      ];

      // Mock file content
      (mockFs.readFile as any).mockImplementation(async (filePath: any, encoding?: any) => {
        if (filePath === '/test/project/src/handler.ts') {
          return `
            import { helper } from './utils';
            import { runOnHttp } from '@proven-network/handler';
            
            export const handler = runOnHttp(
              { path: '/api/test' },
              (req) => ({ result: helper(req.body) })
            );
          `;
        }
        if (filePath === '/test/project/src/utils.ts') {
          return `
            export const helper = (data: any) => {
              return data.toUpperCase();
            };
          `;
        }
        throw new Error('File not found');
      });

      // Mock file existence checks
      mockFs.stat.mockImplementation(async (filePath: any) => {
        const path = String(filePath);
        if (path.includes('utils.ts')) {
          return { isFile: () => true, isDirectory: () => false } as any;
        }
        throw new Error('File not found');
      });

      const result = await collection.collectSourceFiles(mockEntrypoints);

      expect(result).toHaveLength(2);
      expect(result[0].filePath).toBe('/test/project/src/handler.ts');
      expect(result[1].filePath).toBe('/test/project/src/utils.ts');
      expect(result[0].content).toContain('runOnHttp');
      expect(result[1].content).toContain('helper');
    });

    it('should handle missing imported files', async () => {
      const mockEntrypoints = [
        {
          filePath: '/test/project/src/handler.ts',
          moduleSpecifier: './src/handler',
          handlers: [],
          imports: [
            {
              module: './missing-utils',
              type: 'named' as const,
              imports: ['helper'],
            },
          ],
        },
      ];

      (mockFs.readFile as any).mockImplementation(async (filePath: any, encoding?: any) => {
        if (filePath === '/test/project/src/handler.ts') {
          return 'import { helper } from "./missing-utils";';
        }
        throw new Error('File not found');
      });

      mockFs.stat.mockRejectedValue(new Error('File not found'));

      const result = await collection.collectSourceFiles(mockEntrypoints);

      // Should still include the entrypoint file
      expect(result).toHaveLength(1);
      expect(result[0].filePath).toBe('/test/project/src/handler.ts');
    });

    it('should avoid duplicate files', async () => {
      // Override analyzeFile mock for dependency resolution
      mockDiscovery.analyzeFile.mockImplementation(async (filePath) => {
        if (filePath.includes('handler1.ts')) {
          return {
            filePath,
            moduleSpecifier: './src/handler1',
            handlers: [],
            imports: [
              {
                module: './utils',
                type: 'named' as const,
                imports: ['helper'],
                isProvenHandler: false,
              },
            ],
          };
        }
        if (filePath.includes('handler2.ts')) {
          return {
            filePath,
            moduleSpecifier: './src/handler2',
            handlers: [],
            imports: [
              {
                module: './utils',
                type: 'named' as const,
                imports: ['helper'],
                isProvenHandler: false,
              },
            ],
          };
        }
        if (filePath.includes('utils.ts')) {
          return {
            filePath,
            moduleSpecifier: './src/utils',
            handlers: [],
            imports: [],
          };
        }
        return null;
      });
      const mockEntrypoints = [
        {
          filePath: '/test/project/src/handler1.ts',
          moduleSpecifier: './src/handler1',
          handlers: [],
          imports: [{ module: './utils', type: 'named' as const, imports: ['helper'] }],
        },
        {
          filePath: '/test/project/src/handler2.ts',
          moduleSpecifier: './src/handler2',
          handlers: [],
          imports: [{ module: './utils', type: 'named' as const, imports: ['helper'] }],
        },
      ];

      (mockFs.readFile as any).mockImplementation(async (filePath: any, encoding?: any) => {
        const path = String(filePath);
        if (path.includes('handler1.ts')) {
          return 'import { helper } from "./utils";';
        }
        if (path.includes('handler2.ts')) {
          return 'import { helper } from "./utils";';
        }
        if (path.includes('utils.ts')) {
          return 'export const helper = () => {};';
        }
        throw new Error('File not found');
      });

      mockFs.stat.mockImplementation(async (filePath: any) => {
        const path = String(filePath);
        if (path.includes('utils.ts')) {
          return { isFile: () => true, isDirectory: () => false } as any;
        }
        throw new Error('File not found');
      });

      const result = await collection.collectSourceFiles(mockEntrypoints);

      // Should have 3 files total (handler1, handler2, utils), with utils only once
      expect(result).toHaveLength(3);
      const filePaths = result.map((f) => f.filePath);
      expect(filePaths.filter((p) => p.includes('utils.ts'))).toHaveLength(1);
    });
  });

  describe('resolveImportPath', () => {
    it('should resolve relative imports', () => {
      const result = collection.resolveImportPath('./utils', '/test/project/src/handler.ts');
      expect(result).toBe('/test/project/src/utils');
    });

    it('should resolve parent directory imports', () => {
      const result = collection.resolveImportPath(
        '../shared/utils',
        '/test/project/src/api/handler.ts'
      );
      expect(result).toBe('/test/project/src/shared/utils');
    });

    it('should return null for external packages', () => {
      const result = collection.resolveImportPath(
        '@proven-network/handler',
        '/test/project/src/handler.ts'
      );
      expect(result).toBeNull();
    });

    it('should return null for node modules', () => {
      const result = collection.resolveImportPath('express', '/test/project/src/handler.ts');
      expect(result).toBeNull();
    });
  });
});
