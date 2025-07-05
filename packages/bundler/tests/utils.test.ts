import { describe, it, expect } from '@jest/globals';
import { createDefaultOptions, validateOptions, mergeOptions } from '../src/utils';
import type { BundlerOptions } from '../src/types';

describe('Utils', () => {
  describe('createDefaultOptions', () => {
    it('should create default options', () => {
      const options = createDefaultOptions();

      expect(options).toEqual({
        output: 'development',
        entryPatterns: [],
        include: ['**/*.{ts,tsx,js,jsx,mts,mjs}'],
        exclude: [
          'node_modules/**',
          'dist/**',
          'build/**',
          '.git/**',
          '**/*.test.{ts,tsx,js,jsx}',
          '**/*.spec.{ts,tsx,js,jsx}',
          '**/*.d.ts',
        ],
        sourceMaps: false,
        mode: 'development',
        includeDevDependencies: false,
      });
    });
  });

  describe('validateOptions', () => {
    it('should validate valid options', () => {
      const validOptions: BundlerOptions = {
        output: './dist/bundle.json',
        mode: 'production',
        sourceMaps: true,
        includeDevDependencies: false,
      };

      const errors = validateOptions(validOptions);
      expect(errors).toEqual([]);
    });

    it('should accept development output', () => {
      const options: BundlerOptions = {
        output: 'development',
        mode: 'development',
      };

      const errors = validateOptions(options);
      expect(errors).toEqual([]);
    });

    it('should validate include patterns', () => {
      const options: BundlerOptions = {
        include: ['**/*.ts', '**/*.js'],
        exclude: ['node_modules/**'],
      };

      const errors = validateOptions(options);
      expect(errors).toEqual([]);
    });

    it('should validate entry patterns', () => {
      const options: BundlerOptions = {
        entryPatterns: ['src/handlers/**/*.ts', 'src/api/**/*.ts'],
      };

      const errors = validateOptions(options);
      expect(errors).toEqual([]);
    });

    it('should validate package.json path', () => {
      const options: BundlerOptions = {
        packageJsonPath: './custom/package.json',
      };

      const errors = validateOptions(options);
      expect(errors).toEqual([]);
    });

    it('should reject invalid mode', () => {
      const options: BundlerOptions = {
        mode: 'invalid' as any,
      };

      const errors = validateOptions(options);
      expect(errors).toContain('mode must be "development" or "production"');
    });

    it('should reject invalid output type', () => {
      const options: BundlerOptions = {
        output: 123 as any,
      };

      const errors = validateOptions(options);
      expect(errors).toContain('output must be a string or "development"');
    });

    it('should reject invalid boolean options', () => {
      const options: BundlerOptions = {
        sourceMaps: 'true' as any,
        includeDevDependencies: 'false' as any,
      };

      const errors = validateOptions(options);
      expect(errors).toContain('sourceMaps must be a boolean');
      expect(errors).toContain('includeDevDependencies must be a boolean');
    });

    it('should reject invalid array options', () => {
      const options: BundlerOptions = {
        include: 'not-an-array' as any,
        exclude: 123 as any,
        entryPatterns: 'not-an-array' as any,
      };

      const errors = validateOptions(options);
      expect(errors).toContain('include must be an array of strings');
      expect(errors).toContain('exclude must be an array of strings');
      expect(errors).toContain('entryPatterns must be an array of strings');
    });

    it('should reject non-string array elements', () => {
      const options: BundlerOptions = {
        include: ['**/*.ts', 123] as any,
        exclude: [true, '**/*.test.ts'] as any,
      };

      const errors = validateOptions(options);
      expect(errors).toContain('include must be an array of strings');
      expect(errors).toContain('exclude must be an array of strings');
    });

    it('should validate package.json path type', () => {
      const options: BundlerOptions = {
        packageJsonPath: 123 as any,
      };

      const errors = validateOptions(options);
      expect(errors).toContain('packageJsonPath must be a string');
    });
  });

  describe('mergeOptions', () => {
    it('should merge user options with defaults', () => {
      const userOptions: BundlerOptions = {
        output: './dist/bundle.json',
        mode: 'production',
        sourceMaps: true,
      };

      const merged = mergeOptions(userOptions);

      expect(merged).toEqual({
        output: './dist/bundle.json',
        entryPatterns: [],
        include: ['**/*.{ts,tsx,js,jsx,mts,mjs}'],
        exclude: [
          'node_modules/**',
          'dist/**',
          'build/**',
          '.git/**',
          '**/*.test.{ts,tsx,js,jsx}',
          '**/*.spec.{ts,tsx,js,jsx}',
          '**/*.d.ts',
        ],
        sourceMaps: true,
        mode: 'production',
        includeDevDependencies: false,
      });
    });

    it('should override array options completely', () => {
      const userOptions: BundlerOptions = {
        include: ['**/*.ts'],
        exclude: ['node_modules/**', 'dist/**'],
      };

      const merged = mergeOptions(userOptions);

      expect(merged.include).toEqual(['**/*.ts']);
      expect(merged.exclude).toEqual(['node_modules/**', 'dist/**']);
    });

    it('should handle empty user options', () => {
      const merged = mergeOptions({});

      expect(merged).toEqual(createDefaultOptions());
    });

    it('should handle undefined user options', () => {
      const merged = mergeOptions(undefined);

      expect(merged).toEqual(createDefaultOptions());
    });

    it('should preserve all user options', () => {
      const userOptions: BundlerOptions = {
        output: './custom/bundle.json',
        entryPatterns: ['src/custom/**/*.ts'],
        include: ['**/*.ts', '**/*.tsx'],
        exclude: ['**/*.test.ts'],
        sourceMaps: true,
        mode: 'production',
        packageJsonPath: './custom/package.json',
        includeDevDependencies: true,
      };

      const merged = mergeOptions(userOptions);

      expect(merged).toEqual(userOptions);
    });

    it('should handle partial options', () => {
      const userOptions: BundlerOptions = {
        mode: 'production',
        sourceMaps: true,
      };

      const merged = mergeOptions(userOptions);

      expect(merged.mode).toBe('production');
      expect(merged.sourceMaps).toBe(true);
      expect(merged.output).toBe('development'); // default
      expect(merged.includeDevDependencies).toBe(false); // default
    });
  });
});
