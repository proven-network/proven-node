import { BundlerOptions } from '../../bundler-shared/src/types';

/**
 * Extended options for the Rollup plugin
 */
export interface ProvenRollupPluginOptions extends BundlerOptions {
  /**
   * Whether to emit bundle stats to Rollup output
   */
  emitStats?: boolean;

  /**
   * Custom filename for the bundle manifest asset
   */
  filename?: string;

  /**
   * Whether to generate the bundle in watch mode
   */
  watchMode?: boolean;
}

/**
 * Vite-specific integration options
 */
export interface ViteIntegrationOptions {
  /**
   * Whether to integrate with Vite's dev server
   */
  devServer?: boolean;

  /**
   * Vite dev server endpoint for bundle updates
   */
  devServerEndpoint?: string;

  /**
   * Whether to trigger HMR on bundle changes
   */
  hmr?: boolean;
}
