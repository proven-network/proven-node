import { DevWrapperGenerator } from '../src/dev-wrapper-generator';
import { BundleManifest, HandlerInfo, ParameterInfo } from '../src/types';

describe('DevWrapperGenerator', () => {
  let mockManifest: BundleManifest;
  let generator: DevWrapperGenerator;

  beforeEach(() => {
    // Mock window object for tests
    global.window = {
      __ProvenHandlerQueue__: [],
      __ProvenManifest__: { id: 'test-manifest-123' },
    } as any;

    mockManifest = {
      id: 'test-manifest-123',
      version: '1.0.0',
      project: {
        name: 'test-project',
        version: '1.0.0',
        description: 'Test project',
        main: 'index.ts',
        scripts: {},
        dependencies: {},
        devDependencies: {},
      },
      modules: [
        {
          specifier: 'src/handlers.ts',
          content: 'handler content',
          handlers: [
            {
              name: 'simpleHandler',
              type: 'rpc',
              parameters: [],
            },
            {
              name: 'handlerWithParams',
              type: 'rpc',
              parameters: [
                {
                  name: 'request',
                  type: 'CreateRequest',
                  optional: false,
                },
                {
                  name: 'options',
                  type: 'Options',
                  optional: true,
                  defaultValue: {},
                },
              ],
            },
          ],
          imports: [],
        },
      ],
      entrypoints: [],
      sources: [],
      dependencies: {
        production: {},
        development: {},
        all: {},
      },
      metadata: {
        createdAt: new Date().toISOString(),
        mode: 'development',
        pluginVersion: '1.0.0',
      },
    };

    generator = new DevWrapperGenerator(mockManifest);
  });

  afterEach(() => {
    // Clean up global mocks
    delete (global as any).window;
  });

  describe('generateManifestRegistration', () => {
    it('should generate valid manifest registration code', () => {
      const result = generator.generateManifestRegistration();

      expect(result).toContain('window.__ProvenManifest__ = manifest');
      expect(result).toContain(
        'window.__ProvenHandlerQueue__ = window.__ProvenHandlerQueue__ || []'
      );
      expect(result).toContain('"id": "test-manifest-123"');
      expect(result).toContain("console.debug('Proven: Registered manifest', manifest.id)");
    });

    it('should include the complete manifest JSON', () => {
      const result = generator.generateManifestRegistration();

      expect(result).toContain('"name": "test-project"');
      expect(result).toContain('"version": "1.0.0"');
      expect(result).toContain('"mode": "development"');
    });
  });

  describe('generateHandlerWrappers', () => {
    it('should generate wrappers for all handlers', () => {
      const result = generator.generateHandlerWrappers();

      expect(result).toContain('export const simpleHandler =');
      expect(result).toContain('export const handlerWithParams =');
    });

    it('should return comment when no handlers exist', () => {
      const emptyManifest = { ...mockManifest, modules: [] };
      const emptyGenerator = new DevWrapperGenerator(emptyManifest);

      const result = emptyGenerator.generateHandlerWrappers();
      expect(result).toBe('// No handlers found in manifest');
    });

    it('should generate proper JavaScript syntax (no TypeScript)', () => {
      const result = generator.generateHandlerWrappers();

      // Should not contain TypeScript type annotations
      expect(result).not.toContain(': Promise<any>');
      expect(result).not.toContain(': (');
      expect(result).not.toContain('(...args: any[])');

      // Should contain valid JavaScript
      expect(result).toContain('export const');
      expect(result).toContain('= (');
      expect(result).toContain('return new Promise((resolve, reject) => {');
    });
  });

  describe('generated wrapper functions', () => {
    let wrapperCode: string;
    let evalContext: any;

    beforeEach(() => {
      wrapperCode = generator.generateHandlerWrappers();

      // Create evaluation context with mocked window
      evalContext = {
        window: {
          __ProvenHandlerQueue__: [],
          __ProvenManifest__: { id: 'test-manifest-123' },
        },
        Promise: global.Promise,
        exports: {},
      };
    });

    it('should generate handler with no parameters correctly', () => {
      expect(wrapperCode).toContain('export const simpleHandler = () => {');

      // Test the generated function by converting exports to a module-like structure
      const funcCode = `
        ${wrapperCode.replace(/export const/g, 'const')}
        return { simpleHandler };
      `;

      const module = new Function('window', 'Promise', funcCode)(
        evalContext.window,
        evalContext.Promise
      );

      expect(typeof module.simpleHandler).toBe('function');

      // Call the function
      const promise = module.simpleHandler();
      expect(promise).toBeInstanceOf(Promise);

      // Check that it added to the queue
      expect(evalContext.window.__ProvenHandlerQueue__).toHaveLength(1);
      const queueItem = evalContext.window.__ProvenHandlerQueue__[0];
      expect(queueItem.manifestId).toBe('test-manifest-123');
      expect(queueItem.handler).toBe('src/handlers.ts#simpleHandler');
      expect(queueItem.args).toEqual([]);
      expect(typeof queueItem.resolve).toBe('function');
      expect(typeof queueItem.reject).toBe('function');
    });

    it('should generate handler with parameters correctly', () => {
      expect(wrapperCode).toContain(
        'export const handlerWithParams = (request, options = {}) => {'
      );

      // Test the generated function by converting exports to a module-like structure
      const funcCode = `
        ${wrapperCode.replace(/export const/g, 'const')}
        return { handlerWithParams };
      `;

      const module = new Function('window', 'Promise', funcCode)(
        evalContext.window,
        evalContext.Promise
      );

      expect(typeof module.handlerWithParams).toBe('function');

      // Call the function with parameters
      const testRequest = { data: 'test' };
      const testOptions = { flag: true };
      const promise = module.handlerWithParams(testRequest, testOptions);
      expect(promise).toBeInstanceOf(Promise);

      // Check that it added to the queue with correct args
      expect(evalContext.window.__ProvenHandlerQueue__).toHaveLength(1);
      const queueItem = evalContext.window.__ProvenHandlerQueue__[0];
      expect(queueItem.manifestId).toBe('test-manifest-123');
      expect(queueItem.handler).toBe('src/handlers.ts#handlerWithParams');
      expect(queueItem.args).toEqual([testRequest, testOptions]);
    });

    it('should handle default parameter values', () => {
      // Call with only required parameter
      const funcCode = `
        ${wrapperCode.replace(/export const/g, 'const')}
        return { handlerWithParams };
      `;

      const module = new Function('window', 'Promise', funcCode)(
        evalContext.window,
        evalContext.Promise
      );

      const testRequest = { data: 'test' };
      module.handlerWithParams(testRequest); // omit optional parameter

      const queueItem = evalContext.window.__ProvenHandlerQueue__[0];
      expect(queueItem.args).toEqual([testRequest, {}]); // default value should be used
    });

    it('should generate JSDoc comments for handlers with parameters', () => {
      expect(wrapperCode).toContain('/**');
      expect(wrapperCode).toContain('* Generated handler wrapper');
      expect(wrapperCode).toContain('* @param {CreateRequest} request');
      expect(wrapperCode).toContain('* @param {Options} options (optional)');
      expect(wrapperCode).toContain('* @returns {Promise<any>} Handler execution result');
      expect(wrapperCode).toContain('*/');
    });

    it('should not generate JSDoc for handlers without parameters', () => {
      const lines = wrapperCode.split('\n');
      const simpleHandlerIndex = lines.findIndex((line) =>
        line.includes('export const simpleHandler')
      );

      // Check that there's no JSDoc comment before simpleHandler
      const precedingLines = lines.slice(Math.max(0, simpleHandlerIndex - 5), simpleHandlerIndex);
      const hasJSDoc = precedingLines.some(
        (line) => line.includes('/**') || line.includes('* @param')
      );
      expect(hasJSDoc).toBe(false);
    });

    it('should handle promise resolution correctly', async () => {
      const funcCode = `
        ${wrapperCode.replace(/export const/g, 'const')}
        return { simpleHandler };
      `;

      const module = new Function('window', 'Promise', funcCode)(
        evalContext.window,
        evalContext.Promise
      );

      const promise = module.simpleHandler();
      const queueItem = evalContext.window.__ProvenHandlerQueue__[0];

      // Simulate SDK resolving the handler
      const testResult = { success: true, data: 'result' };
      queueItem.resolve(testResult);

      const result = await promise;
      expect(result).toEqual(testResult);
    });

    it('should handle promise rejection correctly', async () => {
      const funcCode = `
        ${wrapperCode.replace(/export const/g, 'const')}
        return { simpleHandler };
      `;

      const module = new Function('window', 'Promise', funcCode)(
        evalContext.window,
        evalContext.Promise
      );

      const promise = module.simpleHandler();
      const queueItem = evalContext.window.__ProvenHandlerQueue__[0];

      // Simulate SDK rejecting the handler
      const testError = new Error('Handler failed');
      queueItem.reject(testError);

      await expect(promise).rejects.toThrow('Handler failed');
    });
  });

  describe('edge cases', () => {
    it('should handle handlers with complex parameter types', () => {
      const complexManifest: BundleManifest = {
        ...mockManifest,
        modules: [
          {
            path: 'src/complex.ts',
            content: 'content',
            handlers: [
              {
                name: 'complexHandler',
                type: 'rpc',
                parameters: [
                  {
                    name: 'arrayParam',
                    type: 'string[]',
                    optional: false,
                  },
                  {
                    name: 'unionParam',
                    type: 'string | number',
                    optional: true,
                  },
                  {
                    name: 'objectParam',
                    type: '{ nested: { value: number } }',
                    optional: true,
                    defaultValue: { nested: { value: 0 } },
                  },
                ],
              },
            ],
            imports: [],
          },
        ],
      };

      const complexGenerator = new DevWrapperGenerator(complexManifest);
      const result = complexGenerator.generateHandlerWrappers();

      expect(result).toContain(
        'export const complexHandler = (arrayParam, unionParam, objectParam = {"nested":{"value":0}}) => {'
      );
      expect(result).toContain('* @param {string[]} arrayParam');
      expect(result).toContain('* @param {string | number} unionParam (optional)');
      expect(result).toContain('* @param {{ nested: { value: number } }} objectParam (optional)');
    });

    it('should handle multiple modules with handlers', () => {
      const multiModuleManifest: BundleManifest = {
        ...mockManifest,
        modules: [
          {
            path: 'src/auth.ts',
            content: 'auth content',
            handlers: [
              {
                name: 'login',
                type: 'rpc',
                parameters: [],
              },
            ],
            dependencies: [],
          },
          {
            path: 'src/data.ts',
            content: 'data content',
            handlers: [
              {
                name: 'fetchData',
                type: 'rpc',
                parameters: [],
              },
            ],
            dependencies: [],
          },
        ],
      };

      const multiGenerator = new DevWrapperGenerator(multiModuleManifest);
      const result = multiGenerator.generateHandlerWrappers();

      expect(result).toContain('export const login =');
      expect(result).toContain('export const fetchData =');
    });

    it('should handle modules without handlers (should not affect output)', () => {
      const mixedManifest: BundleManifest = {
        ...mockManifest,
        modules: [
          {
            specifier: 'src/handlers.ts',
            content: 'handler content',
            handlers: [
              {
                name: 'realHandler',
                type: 'rpc',
                parameters: [],
              },
            ],
            dependencies: [],
          },
          {
            path: 'src/utils.ts',
            content: 'utility functions',
            handlers: [], // No handlers
            dependencies: [],
          },
          {
            path: 'src/types.ts',
            content: 'type definitions',
            handlers: [], // No handlers
            dependencies: [],
          },
        ],
      };

      const mixedGenerator = new DevWrapperGenerator(mixedManifest);
      const result = mixedGenerator.generateHandlerWrappers();

      // Should only generate wrapper for the actual handler
      expect(result).toContain('export const realHandler =');
      expect(result.split('export const').length).toBe(2); // One export + split artifact
    });

    it('should properly escape special characters in default values', () => {
      const specialManifest: BundleManifest = {
        ...mockManifest,
        modules: [
          {
            path: 'src/special.ts',
            content: 'content',
            handlers: [
              {
                name: 'specialHandler',
                type: 'rpc',
                parameters: [
                  {
                    name: 'message',
                    type: 'string',
                    optional: true,
                    defaultValue: 'Hello "world" with \'quotes\' and \nnewlines',
                  },
                ],
              },
            ],
            dependencies: [],
          },
        ],
      };

      const specialGenerator = new DevWrapperGenerator(specialManifest);
      const result = specialGenerator.generateHandlerWrappers();

      // Should properly escape the default value
      expect(result).toContain('message = "Hello \\"world\\" with \'quotes\' and \\nnewlines"');
    });
  });

  describe('generateDevelopmentBundle', () => {
    it('should combine manifest registration and handler wrappers', () => {
      const result = generator.generateDevelopmentBundle();

      expect(result).toContain('// Proven Network Manifest Registration');
      expect(result).toContain('window.__ProvenManifest__ = manifest');
      expect(result).toContain('export const simpleHandler =');
      expect(result).toContain('export const handlerWithParams =');
    });

    it('should separate sections with double newlines', () => {
      const result = generator.generateDevelopmentBundle();

      // Should have proper separation between manifest registration and handlers
      expect(result).toMatch(
        /console\.debug\('Proven: Registered manifest'.*?\n\n.*?export const/s
      );
    });
  });

  describe('non-handler exports behavior', () => {
    it('should not interfere with non-handler exports from handler modules', () => {
      // Create a manifest with a module that has both handlers and non-handler exports
      const mixedExportsManifest: BundleManifest = {
        ...mockManifest,
        modules: [
          {
            specifier: 'src/mixed.ts',
            content: `
            import { run } from '@proven-network/handler';
            
            // Non-handler export - utility function
            export const formatMessage = (msg: string) => \`[LOG] \${msg}\`;
            
            // Non-handler export - constant
            export const API_VERSION = '1.0.0';
            
            // Handler export
            export const processData = run((data: string) => {
              return formatMessage(data);
            });
          `,
            handlers: [
              {
                name: 'processData',
                type: 'rpc',
                parameters: [
                  {
                    name: 'data',
                    type: 'string',
                    optional: false,
                  },
                ],
              },
            ],
            dependencies: [],
          },
        ],
      };

      const mixedGenerator = new DevWrapperGenerator(mixedExportsManifest);
      const result = mixedGenerator.generateHandlerWrappers();

      // Should only generate wrapper for the handler, not the utility functions
      expect(result).toContain('export const processData = (data) => {');

      // Extract the wrapper section (after the manifest)
      const wrapperSection = result.split(
        'window.__ProvenHandlerQueue__ = window.__ProvenHandlerQueue__ || [];'
      )[1];

      // Count the number of generated export statements in the wrapper section only
      const exportMatches = wrapperSection.match(/^export const \w+/gm);
      expect(exportMatches).toHaveLength(1);
      expect(exportMatches![0]).toBe('export const processData');
    });

    it('should allow normal imports of non-handler exports to work unchanged', () => {
      // This test verifies that our wrapper generation doesn't break normal module imports
      // by only generating wrappers for actual handlers, leaving other exports untouched

      const moduleWithUtilsManifest: BundleManifest = {
        ...mockManifest,
        modules: [
          {
            path: 'src/utils.ts',
            content: `
              export const formatDate = (date: Date) => date.toISOString();
              export const calculateSum = (a: number, b: number) => a + b;
              // No handlers in this module
            `,
            handlers: [], // This module has no handlers
            dependencies: [],
          },
          {
            path: 'src/handlers.ts',
            content: `
              import { formatDate, calculateSum } from './utils';
              import { run } from '@proven-network/handler';
              
              export const processNumbers = run((a: number, b: number) => {
                const sum = calculateSum(a, b);
                const timestamp = formatDate(new Date());
                return { sum, timestamp };
              });
            `,
            handlers: [
              {
                name: 'processNumbers',
                type: 'rpc',
                parameters: [
                  { name: 'a', type: 'number', optional: false },
                  { name: 'b', type: 'number', optional: false },
                ],
              },
            ],
            dependencies: ['./utils'],
          },
        ],
      };

      const utilsGenerator = new DevWrapperGenerator(moduleWithUtilsManifest);
      const result = utilsGenerator.generateHandlerWrappers();

      // Should only generate wrapper for the handler in the handler module
      expect(result).toContain('export const processNumbers = (a, b) => {');

      // Extract the wrapper section (after the manifest)
      const wrapperSection = result.split(
        'window.__ProvenHandlerQueue__ = window.__ProvenHandlerQueue__ || [];'
      )[1];

      // Should only have one export in the wrapper section (the handler wrapper)
      const exportMatches = wrapperSection.match(/^export const \w+/gm);
      expect(exportMatches).toHaveLength(1);
      expect(exportMatches![0]).toBe('export const processNumbers');
    });

    it('should handle modules with only non-handler exports correctly', () => {
      const typesOnlyManifest: BundleManifest = {
        ...mockManifest,
        modules: [
          {
            path: 'src/types.ts',
            content: `
              export interface User { id: string; name: string; }
              export type Status = 'active' | 'inactive';
              export const DEFAULT_STATUS = 'active';
            `,
            handlers: [], // No handlers
            dependencies: [],
          },
          {
            path: 'src/constants.ts',
            content: `
              export const APP_NAME = 'MyApp';
              export const VERSION = '1.0.0';
            `,
            handlers: [], // No handlers
            dependencies: [],
          },
        ],
      };

      const typesGenerator = new DevWrapperGenerator(typesOnlyManifest);
      const result = typesGenerator.generateHandlerWrappers();

      // Should return the "no handlers" comment since there are no handlers to wrap
      expect(result).toBe('// No handlers found in manifest');
    });
  });

  describe('integration with real handler queue', () => {
    it('should work with SDK-style handler queue processing', async () => {
      const wrapperCode = generator.generateHandlerWrappers();

      // Simulate a more realistic window object
      const mockWindow = {
        __ProvenHandlerQueue__: [] as any[],
        __ProvenManifest__: { id: 'test-manifest-123' },
      };

      // Generate and execute the handler by converting exports to a module-like structure
      const funcCode = `
        ${wrapperCode.replace(/export const/g, 'const')}
        return { simpleHandler, handlerWithParams };
      `;

      const handlers = new Function('window', 'Promise', funcCode)(mockWindow, Promise);

      // Call handler
      const promise = handlers.simpleHandler();

      // Simulate SDK processing the queue
      expect(mockWindow.__ProvenHandlerQueue__).toHaveLength(1);
      const queueItem = mockWindow.__ProvenHandlerQueue__[0];

      // Simulate successful processing
      setTimeout(() => {
        queueItem.resolve({ status: 'success', data: 'processed' });
      }, 10);

      const result = await promise;
      expect(result).toEqual({ status: 'success', data: 'processed' });
    });
  });
});
