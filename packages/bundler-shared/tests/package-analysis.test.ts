import { describe, it, expect, beforeEach, jest } from '@jest/globals';
import { PackageAnalysis } from '../src/package-analysis';
import * as fs from 'fs';
import * as path from 'path';

jest.mock('fs', () => ({
  promises: {
    readFile: jest.fn(),
    access: jest.fn(),
  },
}));
jest.mock('path');

const mockFs = fs.promises as jest.Mocked<typeof fs.promises>;
const mockPath = path as jest.Mocked<typeof path>;

describe('PackageAnalysis', () => {
  let analysis: PackageAnalysis;
  const mockProjectRoot = '/test/project';

  beforeEach(() => {
    jest.clearAllMocks();
    analysis = new PackageAnalysis(mockProjectRoot);

    // Setup path mocks
    mockPath.resolve.mockImplementation((...args) => args.join('/'));
    mockPath.join.mockImplementation((...args) => args.join('/'));
  });

  describe('analyzeProject', () => {
    it('should analyze valid package.json', async () => {
      const mockPackageJson = {
        name: 'test-project',
        version: '1.0.0',
        description: 'Test project',
        main: 'index.js',
        scripts: {
          test: 'jest',
          build: 'tsc',
        },
        dependencies: {
          express: '^4.18.0',
          '@proven-network/handler': '^1.0.0',
        },
        devDependencies: {
          typescript: '^5.0.0',
          '@types/node': '^20.0.0',
        },
      };

      mockFs.access.mockResolvedValue(undefined);
      mockFs.readFile.mockResolvedValue(JSON.stringify(mockPackageJson));

      const result = await analysis.analyzeProject();

      expect(result).toBeDefined();
      expect(result.name).toBe('test-project');
      expect(result.version).toBe('1.0.0');
      expect(result.description).toBe('Test project');
      expect(result.rootDir).toBe(mockProjectRoot);
      expect(result.packageJson).toEqual(mockPackageJson);
    });

    it('should handle missing package.json', async () => {
      mockFs.access.mockRejectedValue(new Error('ENOENT: no such file or directory'));

      await expect(analysis.analyzeProject()).rejects.toThrow('Package.json not found');
    });

    it('should handle invalid JSON', async () => {
      mockFs.access.mockResolvedValue(undefined);
      mockFs.readFile.mockResolvedValue('{ invalid json }');

      await expect(analysis.analyzeProject()).rejects.toThrow('Invalid package.json format');
    });

    it('should handle missing required fields', async () => {
      const invalidPackageJson = {
        description: 'Missing name and version',
      };

      mockFs.access.mockResolvedValue(undefined);
      mockFs.readFile.mockResolvedValue(JSON.stringify(invalidPackageJson));

      await expect(analysis.analyzeProject()).rejects.toThrow(
        'Package.json missing required fields'
      );
    });
  });

  describe('analyzeDependencies', () => {
    beforeEach(async () => {
      const mockPackageJson = {
        name: 'test-project',
        version: '1.0.0',
        dependencies: {
          express: '^4.18.0',
          '@proven-network/handler': '^1.0.0',
          'local-package': 'file:../local-package',
        },
        devDependencies: {
          typescript: '^5.0.0',
          '@types/node': '^20.0.0',
        },
      };

      mockFs.access.mockResolvedValue(undefined);
      mockFs.readFile.mockResolvedValue(JSON.stringify(mockPackageJson));

      // Call analyzeProject first to populate the cache
      await analysis.analyzeProject();
    });

    it('should analyze dependencies with dev dependencies included', async () => {
      const result = await analysis.analyzeDependencies(true);

      expect(result).toBeDefined();
      expect(result.production).toEqual({
        express: '^4.18.0',
        '@proven-network/handler': '^1.0.0',
      });
      expect(result.development).toEqual({
        typescript: '^5.0.0',
        '@types/node': '^20.0.0',
      });
      expect(result.all).toEqual({
        express: '^4.18.0',
        '@proven-network/handler': '^1.0.0',
        typescript: '^5.0.0',
        '@types/node': '^20.0.0',
      });
    });

    it('should analyze dependencies without dev dependencies', async () => {
      const result = await analysis.analyzeDependencies(false);

      expect(result).toBeDefined();
      expect(result.production).toEqual({
        express: '^4.18.0',
        '@proven-network/handler': '^1.0.0',
      });
      expect(result.development).toEqual({});
      expect(result.all).toEqual({
        express: '^4.18.0',
        '@proven-network/handler': '^1.0.0',
      });
    });

    it('should filter out non-npm dependencies', async () => {
      const result = await analysis.analyzeDependencies(true);

      // Local package should be filtered out
      expect(result.production).not.toHaveProperty('local-package');
      expect(result.all).not.toHaveProperty('local-package');
    });

    it('should handle missing dependencies section', async () => {
      const mockPackageJson = {
        name: 'test-project',
        version: '1.0.0',
      };

      mockFs.access.mockResolvedValue(undefined);
      mockFs.readFile.mockResolvedValue(JSON.stringify(mockPackageJson));

      // Create a new analysis instance for this test
      const tempAnalysis = new PackageAnalysis(mockProjectRoot);
      await tempAnalysis.analyzeProject();

      const result = await tempAnalysis.analyzeDependencies(true);

      expect(result.production).toEqual({});
      expect(result.development).toEqual({});
      expect(result.all).toEqual({});
    });
  });

  describe('isValidNpmDependency', () => {
    it('should accept valid npm dependencies', () => {
      expect(analysis.isValidNpmDependency('express', '^4.18.0')).toBe(true);
      expect(analysis.isValidNpmDependency('@types/node', '~20.0.0')).toBe(true);
      expect(analysis.isValidNpmDependency('package', '1.0.0')).toBe(true);
      expect(analysis.isValidNpmDependency('package', '>=1.0.0')).toBe(true);
    });

    it('should reject non-npm dependencies', () => {
      expect(analysis.isValidNpmDependency('local', 'file:../local')).toBe(false);
      expect(
        analysis.isValidNpmDependency('git-package', 'git+https://github.com/user/repo.git')
      ).toBe(false);
      expect(analysis.isValidNpmDependency('github', 'github:user/repo')).toBe(false);
      expect(analysis.isValidNpmDependency('url', 'https://example.com/package.tar.gz')).toBe(
        false
      );
    });
  });

  describe('custom package.json path', () => {
    it('should use custom package.json path', async () => {
      const customPath = '/custom/path/package.json';
      const customAnalysis = new PackageAnalysis(mockProjectRoot, customPath);

      const mockPackageJson = {
        name: 'custom-project',
        version: '2.0.0',
      };

      mockFs.access.mockResolvedValue(undefined);
      mockFs.readFile.mockResolvedValue(JSON.stringify(mockPackageJson));

      const result = await customAnalysis.analyzeProject();

      expect(mockFs.readFile).toHaveBeenCalledWith(customPath, 'utf-8');
      expect(result.name).toBe('custom-project');
      expect(result.version).toBe('2.0.0');
    });
  });
});
