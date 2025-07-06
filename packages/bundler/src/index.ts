// Export all types
export * from './types';

// Export main classes
export { EntrypointDiscovery } from './entrypoint-discovery';
export { PackageAnalysis } from './package-analysis';
export { FileCollection } from './file-collection';
export { BundleManifestGenerator, BundleManifestUtils } from './bundle-manifest';
export { DevWrapperGenerator } from './dev-wrapper-generator';
export { DevServerUtils } from './dev-server-utils';

// Export utility functions
export { createDefaultOptions, validateOptions, mergeOptions, formatFileSize } from './utils';

// Export AST transformation utilities
export {
  transformHandlers,
  hasHandlerImport,
  type TransformConfig,
  type ExtractedHandler,
} from './ast-transform';
