import { BundlerOptions, ResolvedBundlerOptions } from './types';

/**
 * Creates default bundler options
 */
export function createDefaultOptions(): ResolvedBundlerOptions {
  return {
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
  };
}

/**
 * Validates bundler options
 */
export function validateOptions(options: BundlerOptions): string[] {
  const errors: string[] = [];

  // Validate mode
  if (options.mode && !['development', 'production'].includes(options.mode)) {
    errors.push('mode must be "development" or "production"');
  }

  // Validate output
  if (options.output !== undefined && typeof options.output !== 'string') {
    errors.push('output must be a string or "development"');
  }

  // Validate patterns
  if (options.entryPatterns !== undefined) {
    if (!Array.isArray(options.entryPatterns)) {
      errors.push('entryPatterns must be an array of strings');
    } else if (!options.entryPatterns.every((p) => typeof p === 'string')) {
      errors.push('entryPatterns must be an array of strings');
    }
  }

  if (options.include !== undefined) {
    if (!Array.isArray(options.include)) {
      errors.push('include must be an array of strings');
    } else if (!options.include.every((p) => typeof p === 'string')) {
      errors.push('include must be an array of strings');
    }
  }

  if (options.exclude !== undefined) {
    if (!Array.isArray(options.exclude)) {
      errors.push('exclude must be an array of strings');
    } else if (!options.exclude.every((p) => typeof p === 'string')) {
      errors.push('exclude must be an array of strings');
    }
  }

  // Validate packageJsonPath
  if (options.packageJsonPath !== undefined && typeof options.packageJsonPath !== 'string') {
    errors.push('packageJsonPath must be a string');
  }

  // Validate boolean options
  if (options.sourceMaps !== undefined && typeof options.sourceMaps !== 'boolean') {
    errors.push('sourceMaps must be a boolean');
  }

  if (
    options.includeDevDependencies !== undefined &&
    typeof options.includeDevDependencies !== 'boolean'
  ) {
    errors.push('includeDevDependencies must be a boolean');
  }

  return errors;
}

/**
 * Merges user options with defaults
 */
export function mergeOptions(userOptions: BundlerOptions = {}): ResolvedBundlerOptions {
  const defaults = createDefaultOptions();

  return {
    ...defaults,
    ...userOptions,
    // Array options are completely replaced if provided
    entryPatterns: userOptions.entryPatterns ?? defaults.entryPatterns,
    include: userOptions.include ?? defaults.include,
    exclude: userOptions.exclude ?? defaults.exclude,
  };
}

/**
 * Formats file size in human-readable format
 */
export function formatFileSize(bytes: number): string {
  const units = ['B', 'KB', 'MB', 'GB'];
  let size = bytes;
  let unitIndex = 0;

  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex++;
  }

  return `${size.toFixed(unitIndex === 0 ? 0 : 1)} ${units[unitIndex]}`;
}

/**
 * Creates a module specifier from a file path
 */
export function pathToModuleSpecifier(filePath: string, projectRoot: string): string {
  const relativePath = filePath.startsWith(projectRoot)
    ? filePath.slice(projectRoot.length + 1)
    : filePath;

  // Convert Windows paths to Unix-style
  const unixPath = relativePath.replace(/\\/g, '/');

  // Remove file extension
  const withoutExt = unixPath.replace(/\.(ts|tsx|js|jsx|mjs|mts)$/, '');

  // Ensure it starts with './' for relative imports
  return withoutExt.startsWith('.') ? withoutExt : `./${withoutExt}`;
}

/**
 * Debounces a function call
 */
export function debounce<T extends (...args: any[]) => any>(
  func: T,
  wait: number
): (...args: Parameters<T>) => void {
  let timeout: NodeJS.Timeout | null = null;

  return (...args: Parameters<T>) => {
    if (timeout) {
      clearTimeout(timeout);
    }

    timeout = setTimeout(() => {
      func(...args);
    }, wait);
  };
}

/**
 * Creates a unique identifier for a handler
 */
export function createHandlerId(file: string, handlerName: string): string {
  if (handlerName === 'default') {
    return file;
  }
  return `${file}#${handlerName}`;
}

/**
 * Checks if a file path matches any of the given patterns
 */
export function matchesPatterns(filePath: string, patterns: string[]): boolean {
  return patterns.some((pattern) => {
    // Convert glob pattern to regex
    const regex = new RegExp(
      pattern
        .replace(/\*\*/g, '.*')
        .replace(/\*/g, '[^/]*')
        .replace(/\?/g, '.')
        .replace(/\./g, '\\.')
    );
    return regex.test(filePath);
  });
}
