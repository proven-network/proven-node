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
   * Generates TypeScript type declarations for the handler wrappers
   */
  public generateTypeDeclarations(): string {
    const allHandlers = this.getAllHandlers();

    if (allHandlers.length === 0) {
      return '// No handlers found in manifest';
    }

    // Import QueuedHandler and BundleManifest from common package
    const queuedHandlerInterface = `import { QueuedHandler, BundleManifest } from '@proven-network/common';
declare global {
    interface Window {
        __ProvenHandlerQueue__: QueuedHandler[] & {
            push: (handler: QueuedHandler) => number;
        };
        __ProvenManifest__: BundleManifest;
    }
}`;

    const declarations = allHandlers.map(({ handler }) => {
      const parameters = (handler.parameters || [])
        .map((param) => {
          const optional = param.optional ? '?' : '';
          return `${param.name}${optional}: ${param.type}`;
        })
        .join(', ');

      // Generate JSDoc if handler has parameters
      let jsDoc = '';
      if ((handler.parameters || []).length > 0) {
        const paramDocs = (handler.parameters || [])
          .map((param) => {
            const optional = param.optional ? ' (optional)' : '';
            return `* @param {${param.type}} ${param.name}${optional}`;
          })
          .join('\n ');

        jsDoc = `/**
 * Generated handler wrapper
 ${paramDocs}
 * @returns {Promise<any>} Handler execution result
 */
`;
      }

      return `${jsDoc}export declare const ${handler.name}: (${parameters}) => Promise<any>;`;
    });

    return `${queuedHandlerInterface}\n${declarations.join('\n\n')}\nexport {};`;
  }

  /**
   * Generates wrapper functions for all handlers in the manifest
   */
  public generateHandlerWrappers(): string {
    const allHandlers = this.getAllHandlers();

    if (allHandlers.length === 0) {
      return '// No handlers found in manifest';
    }

    const manifestInit = this.generateManifestInit();
    const wrappers = allHandlers.map(({ handler }) => this.generateSingleHandlerWrapper(handler));

    return `${manifestInit}\n\n${wrappers.join('\n\n')}`;
  }

  /**
   * Gets all handlers from all modules in the manifest
   */
  private getAllHandlers(): Array<{ handler: HandlerInfo; modulePath: string }> {
    const handlers: Array<{ handler: HandlerInfo; modulePath: string }> = [];

    for (const module of this.manifest.modules) {
      for (const handler of module.handlers) {
        handlers.push({ handler, modulePath: module.specifier });
      }
    }

    return handlers;
  }

  /**
   * Gets the module path for a specific handler
   */
  private getHandlerModulePath(handler: HandlerInfo): string {
    for (const module of this.manifest.modules) {
      for (const moduleHandler of module.handlers) {
        if (moduleHandler.name === handler.name) {
          return module.specifier;
        }
      }
    }
    return 'unknown';
  }

  /**
   * Generates the manifest initialization code with tracking
   */
  private generateManifestInit(): string {
    const manifestJson = JSON.stringify(this.manifest, null, 2);

    return `// Proven Network manifest and tracking
let __provenManifestSent = false;
const __provenManifest = ${manifestJson};

// Store manifest globally for compatibility
window.__ProvenManifest__ = __provenManifest;

// Initialize handler queue if not exists
window.__ProvenHandlerQueue__ = window.__ProvenHandlerQueue__ || [];`;
  }

  /**
   * Generates a wrapper function for a single handler
   */
  private generateSingleHandlerWrapper(handler: HandlerInfo): string {
    const handlerModulePath = this.getHandlerModulePath(handler);
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

    // Build handler specifier: file:///module.ts#handlerName
    // The handlerModulePath should already be a proper file:// URL from the manifest
    const handlerSpecifier = `${handlerModulePath}#${handler.name}`;

    return `${parametersComment}export const ${handler.name} = (${parameterList}) => {
  const queue = window.__ProvenHandlerQueue__;
  return new Promise((resolve, reject) => {
    const shouldSendManifest = !__provenManifestSent;
    if (shouldSendManifest) {
      __provenManifestSent = true;
    }
    
    queue.push({
      manifestId: __provenManifest.id,
      manifest: shouldSendManifest ? __provenManifest : undefined,
      handler: '${handlerSpecifier}',
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
