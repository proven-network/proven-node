import { BundleManifest, HandlerInfo } from './types';

/**
 * Generates development mode wrapper code for handlers
 */
export class DevWrapperGenerator {
  private readonly manifest: BundleManifest;

  constructor(manifest: BundleManifest) {
    this.manifest = manifest;
  }

  /**
   * Generates the complete development bundle with manifest registration and handler wrappers
   */
  generateDevelopmentBundle(): string {
    const manifestRegistration = this.generateManifestRegistration();
    const handlerWrappers = this.generateHandlerWrappers();

    return `${manifestRegistration}\n\n${handlerWrappers}`;
  }

  /**
   * Generates the manifest registration code
   */
  public generateManifestRegistration(): string {
    const manifestJson = JSON.stringify(this.manifest, null, 2);

    return `// Proven Network Manifest Registration
(() => {
  const manifest = ${manifestJson};
  
  // Store manifest globally for SDK to pick up
  window.__ProvenManifest__ = manifest;
  
  // Initialize handler queue if not exists
  window.__ProvenHandlerQueue__ = window.__ProvenHandlerQueue__ || [];
  
  console.debug('Proven: Registered manifest', manifest.id);
})();`;
  }

  /**
   * Generates wrapper functions for all handlers in the manifest
   */
  public generateHandlerWrappers(): string {
    const allHandlers = this.getAllHandlers();

    if (allHandlers.length === 0) {
      return '// No handlers found in manifest';
    }

    const wrappers = allHandlers.map(({ handler }) => this.generateSingleHandlerWrapper(handler));

    return wrappers.join('\n\n');
  }

  /**
   * Gets all handlers from all modules in the manifest
   */
  private getAllHandlers(): Array<{ handler: HandlerInfo; modulePath: string }> {
    const handlers: Array<{ handler: HandlerInfo; modulePath: string }> = [];

    for (const module of this.manifest.modules) {
      for (const handler of module.handlers) {
        handlers.push({ handler, modulePath: module.path });
      }
    }

    return handlers;
  }

  /**
   * Generates a wrapper function for a single handler
   */
  private generateSingleHandlerWrapper(handler: HandlerInfo): string {
    const parameterList = handler.parameters
      .map((param) => {
        if (param.optional && param.defaultValue !== undefined) {
          return `${param.name} = ${JSON.stringify(param.defaultValue)}`;
        }
        return param.name;
      })
      .join(', ');

    const parametersComment =
      handler.parameters.length > 0 ? this.generateParametersComment(handler.parameters) : '';

    const typeAnnotation =
      handler.parameters.length > 0
        ? this.generateTypeAnnotation(handler.parameters)
        : '(...args: any[])';

    return `${parametersComment}export const ${handler.name}: ${typeAnnotation} => Promise<any> = (${parameterList}) => {
  const queue = window.__ProvenHandlerQueue__;
  return new Promise((resolve, reject) => {
    queue.push({
      manifestId: window.__ProvenManifest__.id,
      handler: '${handler.name}',
      args: [${handler.parameters.map((p) => p.name).join(', ')}],
      resolve,
      reject
    });
  });
};`;
  }

  /**
   * Generates JSDoc-style comments for handler parameters
   */
  private generateParametersComment(parameters: any[]): string {
    if (parameters.length === 0) return '';

    const paramDocs = parameters.map((param) => {
      let line = ` * @param {${param.type || 'any'}} ${param.name}`;
      if (param.optional) {
        line += ' (optional)';
      }
      return line;
    });

    return `/**
 * Generated handler wrapper
${paramDocs.join('\n')}
 * @returns {Promise<any>} Handler execution result
 */
`;
  }

  /**
   * Generates TypeScript type annotation for the handler function
   */
  private generateTypeAnnotation(parameters: any[]): string {
    const paramTypes = parameters.map((param) => {
      const type = param.type || 'any';
      const paramName = param.name;
      const optional = param.optional ? '?' : '';
      return `${paramName}${optional}: ${type}`;
    });

    return `(${paramTypes.join(', ')})`;
  }

  /**
   * Generates hot-reload support code
   */
  generateHotReloadSupport(): string {
    return `
// Hot-reload support for development
if (typeof module !== 'undefined' && module.hot) {
  module.hot.accept('./manifest.js', () => {
    try {
      const newManifest = require('./manifest.js').manifest;
      if (window.ProvenSDK?.updateManifest) {
        window.ProvenSDK.updateManifest(newManifest);
        console.log('Proven: Hot-reloaded manifest', newManifest.id);
      }
    } catch (error) {
      console.error('Proven: Failed to hot-reload manifest:', error);
    }
  });
}`;
  }

  /**
   * Generates a complete development server manifest endpoint response
   */
  generateManifestEndpointResponse(): object {
    return {
      manifest: this.manifest,
      timestamp: new Date().toISOString(),
      handlers: this.getAllHandlers().map(({ handler, modulePath }) => ({
        name: handler.name,
        type: handler.type,
        module: modulePath,
        parameters: handler.parameters,
      })),
    };
  }
}
