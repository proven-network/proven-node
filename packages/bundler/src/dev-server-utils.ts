import { BundleManifest } from './types';
import { DevWrapperGenerator } from './dev-wrapper-generator';

/**
 * Development server utilities for manifest-based development
 */
export class DevServerUtils {
  /**
   * Creates a development bundle from a manifest
   */
  static createDevelopmentBundle(manifest: BundleManifest): {
    manifestJs: string;
    handlersJs: string;
    combinedJs: string;
  } {
    const generator = new DevWrapperGenerator(manifest);

    const manifestJs = generator.generateManifestRegistration();
    const handlersJs = generator.generateHandlerWrappers();
    const hotReloadJs = generator.generateHotReloadSupport();

    const combinedJs = `${manifestJs}\n\n${handlersJs}\n\n${hotReloadJs}`;

    return {
      manifestJs,
      handlersJs,
      combinedJs,
    };
  }

  /**
   * Creates manifest endpoint response
   */
  static createManifestEndpoint(manifest: BundleManifest): object {
    const generator = new DevWrapperGenerator(manifest);
    return generator.generateManifestEndpointResponse();
  }

  /**
   * Generates a simple development server configuration
   */
  static generateDevServerConfig(
    options: {
      port?: number;
      manifestPath?: string;
      outputPath?: string;
    } = {}
  ): object {
    const {
      port = 3001,
      manifestPath = '/_proven/manifest',
      outputPath = '/_proven/bundle.js',
    } = options;

    return {
      port,
      routes: {
        [manifestPath]: {
          method: 'GET',
          description: 'Get current bundle manifest',
          headers: {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
          },
        },
        [outputPath]: {
          method: 'GET',
          description: 'Get development bundle with wrapped handlers',
          headers: {
            'Content-Type': 'application/javascript',
            'Access-Control-Allow-Origin': '*',
          },
        },
        '/_proven/reload': {
          method: 'GET',
          description: 'WebSocket endpoint for hot-reload notifications',
          upgrade: 'websocket',
        },
      },
    };
  }

  /**
   * Creates a simple hot-reload notification message
   */
  static createHotReloadMessage(
    manifest: BundleManifest,
    changeType: 'manifest' | 'handler' | 'module' = 'manifest'
  ): object {
    return {
      type: 'proven:hot-reload',
      changeType,
      manifestId: manifest.id,
      timestamp: new Date().toISOString(),
      manifest: changeType === 'manifest' ? manifest : undefined,
    };
  }

  /**
   * Validates a manifest for development use
   */
  static validateManifestForDevelopment(manifest: BundleManifest): {
    valid: boolean;
    errors: string[];
    warnings: string[];
  } {
    const errors: string[] = [];
    const warnings: string[] = [];

    // Check required fields
    if (!manifest.id) {
      errors.push('Manifest ID is required');
    }

    if (!manifest.version) {
      errors.push('Manifest version is required');
    }

    if (!manifest.modules || manifest.modules.length === 0) {
      errors.push('Manifest must contain at least one module');
    }

    // Check modules
    let handlerCount = 0;
    for (const module of manifest.modules || []) {
      if (!module.path) {
        errors.push(`Module missing path: ${JSON.stringify(module)}`);
      }

      if (!module.content) {
        warnings.push(`Module has empty content: ${module.path}`);
      }

      handlerCount += module.handlers?.length || 0;
    }

    if (handlerCount === 0) {
      warnings.push('No handlers found in manifest');
    }

    // Check metadata
    if (manifest.metadata?.mode !== 'development') {
      warnings.push('Manifest is not in development mode');
    }

    return {
      valid: errors.length === 0,
      errors,
      warnings,
    };
  }

  /**
   * Extracts development-relevant information from a manifest
   */
  static extractDevelopmentInfo(manifest: BundleManifest): {
    handlerCount: number;
    moduleCount: number;
    totalSize: number;
    handlers: Array<{
      name: string;
      type: string;
      module: string;
      parameterCount: number;
    }>;
  } {
    let handlerCount = 0;
    let totalSize = 0;
    const handlers: Array<{
      name: string;
      type: string;
      module: string;
      parameterCount: number;
    }> = [];

    for (const module of manifest.modules) {
      totalSize += module.content.length;

      for (const handler of module.handlers) {
        handlerCount++;
        handlers.push({
          name: handler.name,
          type: handler.type,
          module: module.path,
          parameterCount: handler.parameters?.length || 0,
        });
      }
    }

    return {
      handlerCount,
      moduleCount: manifest.modules.length,
      totalSize,
      handlers,
    };
  }
}
