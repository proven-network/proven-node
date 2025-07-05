import { BundlerOptions } from './types';
/**
 * Creates default bundler options
 */
export declare function createDefaultOptions(): BundlerOptions;
/**
 * Validates bundler options
 */
export declare function validateOptions(options: BundlerOptions): string[];
/**
 * Merges user options with defaults
 */
export declare function mergeOptions(userOptions?: BundlerOptions): BundlerOptions;
/**
 * Formats file size in human-readable format
 */
export declare function formatFileSize(bytes: number): string;
/**
 * Creates a module specifier from a file path
 */
export declare function pathToModuleSpecifier(filePath: string, projectRoot: string): string;
/**
 * Debounces a function call
 */
export declare function debounce<T extends (...args: any[]) => any>(
  func: T,
  wait: number
): (...args: Parameters<T>) => void;
/**
 * Creates a unique identifier for a handler
 */
export declare function createHandlerId(file: string, handlerName: string): string;
/**
 * Checks if a file path matches any of the given patterns
 */
export declare function matchesPatterns(filePath: string, patterns: string[]): boolean;
