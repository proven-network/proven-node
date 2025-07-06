import { transformHandlers, hasHandlerImport } from './ast-transform';
import { getOptions } from 'loader-utils';

/**
 * Webpack loader that transforms handler functions
 */
export default async function provenHandlerLoader(this: any, source: string): Promise<void> {
  const callback = this.async();
  const options = getOptions(this) || {};
  const pluginName = String(options.pluginName || 'ProvenWebpackPlugin');

  // Get the plugin context from the loader context
  const pluginContext = (this as any)[pluginName];
  if (!pluginContext) {
    // No plugin context, just return the source unchanged
    callback(null, source, this.sourceMap);
    return;
  }

  // Only transform files that import from @proven-network/handler
  if (!hasHandlerImport(source)) {
    callback(null, source, this.sourceMap);
    return;
  }

  try {
    // Get the manifest from the generator
    const { generator, projectRoot } = pluginContext;
    const manifest = await generator.generateManifest();

    // Transform the source code
    const result = await transformHandlers(source, {
      manifest,
      manifestId: manifest.id,
      filePath: `file://${this.resourcePath}`,
      sourceMap: this.sourceMap,
    });

    console.log(`✅ Transformed handlers in ${this.resourcePath.replace(projectRoot, '.')}`);

    callback(null, result.code, result.map);
  } catch (error) {
    callback(error as Error);
  }
}

// Mark this loader as cacheable
export const raw = false;
