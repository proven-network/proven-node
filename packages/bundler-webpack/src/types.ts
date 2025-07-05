import { BundlerOptions } from '../../bundler-shared/src/types';

/**
 * Extended options for the webpack plugin
 */
export interface ProvenWebpackPluginOptions extends BundlerOptions {
  /**
   * Whether to emit bundle stats to webpack compilation
   */
  emitStats?: boolean;

  /**
   * Custom webpack output filename for the bundle manifest
   */
  filename?: string;

  /**
   * Whether to watch for file changes in development mode
   */
  watch?: boolean;
}

/**
 * Development server integration options
 */
export interface DevServerOptions {
  /**
   * Endpoint to send bundle updates to
   */
  endpoint?: string;

  /**
   * Headers to include with bundle update requests
   */
  headers?: Record<string, string>;

  /**
   * Whether to automatically reload the development server on bundle changes
   */
  autoReload?: boolean;
}
