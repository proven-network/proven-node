import * as babel from '@babel/core';
import * as t from '@babel/types';
import { NodePath } from '@babel/traverse';
import { HandlerInfo, BundleManifest } from './types';
import * as pathUtils from 'path';

/**
 * Configuration for AST transformation
 */
export interface TransformConfig {
  manifest: BundleManifest;
  manifestId: string;
  filePath: string;
  projectRoot?: string;
  sourceMap?: boolean;
  skipManifestInjection?: boolean;
}

/**
 * Result of handler extraction
 */
export interface ExtractedHandler {
  name: string;
  type: 'rpc' | 'http' | 'event' | 'schedule';
  parameters: Array<{
    name: string;
    type?: string;
    optional: boolean;
    defaultValue?: any;
  }>;
  config?: any;
  location: {
    line: number;
    column: number;
  };
}

/**
 * Transform source code to replace handler wrapper functions with SDK queue calls
 */
export async function transformHandlers(
  code: string,
  config: TransformConfig
): Promise<{ code: string; map?: any; handlers: ExtractedHandler[] }> {
  const handlers: ExtractedHandler[] = [];

  const result = await babel.transformAsync(code, {
    filename: config.filePath,
    sourceMaps: config.sourceMap,
    presets: [['@babel/preset-typescript', { onlyRemoveTypeImports: true }]],
    plugins: [createHandlerTransformPlugin(config, handlers)],
  });

  if (!result || !result.code) {
    throw new Error('Failed to transform code');
  }

  return {
    code: result.code,
    map: result.map,
    handlers,
  };
}

/**
 * Create a Babel plugin that transforms handler functions
 */
function createHandlerTransformPlugin(
  config: TransformConfig,
  handlers: ExtractedHandler[]
): babel.PluginObj {
  let manifestVarName: string | null = null;
  let manifestSentVarName: string | null = null;
  let hasInjectedManifest = false;

  return {
    name: 'proven-handler-transform',
    visitor: {
      Program: {
        enter(path) {
          // Use fixed variable names to ensure consistency across transformations
          manifestVarName = '_provenManifest';
          manifestSentVarName = '_provenManifestSent';
        },
        exit(path) {
          // Inject manifest and tracking variables at the top of the file if we found handlers
          if (
            hasInjectedManifest &&
            manifestVarName &&
            manifestSentVarName &&
            !config.skipManifestInjection
          ) {
            const manifestDeclaration = t.variableDeclaration('const', [
              t.variableDeclarator(
                t.identifier(manifestVarName),
                // Inject the full manifest from config
                t.valueToNode(config.manifest)
              ),
            ]);

            const manifestSentDeclaration = t.variableDeclaration('let', [
              t.variableDeclarator(t.identifier(manifestSentVarName), t.booleanLiteral(false)),
            ]);

            path.unshiftContainer('body', [manifestDeclaration, manifestSentDeclaration]);
          }
        },
      },

      CallExpression(path) {
        // Check if this is a handler wrapper function call
        const handlerInfo = extractHandlerInfo(path, config.filePath);
        if (!handlerInfo) return;

        // Record the handler
        handlers.push(handlerInfo);

        // Replace the wrapper call with the transformed function
        const replacement = createHandlerReplacement(
          path,
          handlerInfo,
          config,
          manifestVarName!,
          manifestSentVarName!
        );

        path.replaceWith(replacement);
        hasInjectedManifest = true;
      },
    },
  };
}

/**
 * Extract handler information from a CallExpression
 */
function extractHandlerInfo(
  path: NodePath<t.CallExpression>,
  filePath: string
): ExtractedHandler | null {
  const callee = path.node.callee;

  // Check if it's calling run, runWithOptions, runOnHttp, or runOnRadixEvent
  if (!t.isIdentifier(callee)) return null;

  const handlerType = getHandlerType(callee.name);
  if (!handlerType) return null;

  // Extract the handler function (last argument)
  const args = path.node.arguments;
  const handlerFn = handlerType.hasOptions ? args[1] : args[0];
  const options = handlerType.hasOptions ? args[0] : null;

  if (
    !handlerFn ||
    (!t.isFunctionExpression(handlerFn) && !t.isArrowFunctionExpression(handlerFn))
  ) {
    return null;
  }

  // Extract handler name from parent context
  const handlerName = extractHandlerName(path);
  if (!handlerName) return null;

  // Extract parameters
  const parameters = extractParameters(handlerFn);

  // Extract config from options if present
  const config = options && t.isObjectExpression(options) ? extractConfig(options) : undefined;

  return {
    name: handlerName,
    type: handlerType.type,
    parameters,
    config,
    location: {
      line: path.node.loc?.start.line || 0,
      column: path.node.loc?.start.column || 0,
    },
  };
}

/**
 * Get handler type information from function name
 */
function getHandlerType(
  fnName: string
): { type: 'rpc' | 'http' | 'event'; hasOptions: boolean } | null {
  switch (fnName) {
    case 'run':
      return { type: 'rpc', hasOptions: false };
    case 'runWithOptions':
      return { type: 'rpc', hasOptions: true };
    case 'runOnHttp':
      return { type: 'http', hasOptions: true };
    case 'runOnRadixEvent':
      return { type: 'event', hasOptions: true };
    default:
      return null;
  }
}

/**
 * Extract handler name from variable declaration or export
 */
function extractHandlerName(path: NodePath<t.CallExpression>): string | null {
  let parent = path.parent;

  // Handle: const handlerName = run(...)
  if (t.isVariableDeclarator(parent) && t.isIdentifier(parent.id)) {
    return parent.id.name;
  }

  // Handle: export const handlerName = run(...)
  if (t.isVariableDeclarator(parent)) {
    const varDeclaration = path.parentPath?.parent;
    if (varDeclaration && t.isVariableDeclaration(varDeclaration)) {
      const exportDeclaration = path.parentPath?.parentPath?.parent;
      if (exportDeclaration && t.isExportNamedDeclaration(exportDeclaration)) {
        const declaration = exportDeclaration.declaration;
        if (declaration && t.isVariableDeclaration(declaration) && declaration.declarations[0]) {
          const id = declaration.declarations[0].id;
          if (t.isIdentifier(id)) {
            return id.name;
          }
        }
      }
    }
  }

  // Handle: export default run(...)
  if (t.isExportDefaultDeclaration(parent)) {
    return 'default';
  }

  return null;
}

/**
 * Extract parameters from a function
 */
function extractParameters(
  fn: t.FunctionExpression | t.ArrowFunctionExpression
): ExtractedHandler['parameters'] {
  return fn.params.map((param, index) => {
    if (t.isIdentifier(param)) {
      return {
        name: param.name,
        optional: false,
      };
    }

    if (t.isAssignmentPattern(param) && t.isIdentifier(param.left)) {
      return {
        name: param.left.name,
        optional: true,
        defaultValue: param.right,
      };
    }

    // Fallback for complex patterns
    return {
      name: `arg${index}`,
      optional: false,
    };
  });
}

/**
 * Extract configuration from options object
 */
function extractConfig(options: t.ObjectExpression): any {
  const config: any = {};

  for (const prop of options.properties) {
    if (t.isObjectProperty(prop) && t.isIdentifier(prop.key)) {
      const key = prop.key.name;
      if (t.isStringLiteral(prop.value)) {
        config[key] = prop.value.value;
      } else if (t.isNumericLiteral(prop.value)) {
        config[key] = prop.value.value;
      } else if (t.isBooleanLiteral(prop.value)) {
        config[key] = prop.value.value;
      } else if (t.isArrayExpression(prop.value)) {
        // Handle string arrays for allowedOrigins
        config[key] = prop.value.elements
          .map((el) => (t.isStringLiteral(el) ? el.value : null))
          .filter(Boolean);
      }
    }
  }

  return config;
}

/**
 * Create the replacement code for a handler
 */
function createHandlerReplacement(
  path: NodePath<t.CallExpression>,
  handlerInfo: ExtractedHandler,
  config: TransformConfig,
  manifestVarName: string,
  manifestSentVarName: string
): t.Expression {
  const handlerFn = getHandlerFunction(path.node);
  if (!handlerFn) {
    throw new Error('Could not extract handler function');
  }

  // Create the wrapper function with the same parameters
  const wrapperParams = handlerFn.params;
  const paramNames = extractParamNames(handlerFn.params);

  // Build handler specifier using relative path from src directory
  let handlerSpecifier: string;
  if (config.filePath.startsWith('file://')) {
    // Extract the actual file path from file:// URL
    const actualFilePath = config.filePath.replace(/^file:\/\/\//, '');
    // Find the src directory and make path relative to project root
    const srcIndex = actualFilePath.lastIndexOf('/src/');
    if (srcIndex !== -1) {
      const relativePath = actualFilePath.substring(srcIndex + 1); // Keep 'src/' in the path
      handlerSpecifier = `file:///${relativePath}#${handlerInfo.name}`;
    } else {
      // Fallback: use just the filename
      const filename = pathUtils.basename(actualFilePath);
      handlerSpecifier = `file:///${filename}#${handlerInfo.name}`;
    }
  } else {
    handlerSpecifier = `${config.filePath}#${handlerInfo.name}`;
  }

  // Create the promise-returning function body
  const promiseBody = t.blockStatement([
    // const queue = window.__ProvenHandlerQueue__ = window.__ProvenHandlerQueue__ || [];
    t.variableDeclaration('const', [
      t.variableDeclarator(
        t.identifier('queue'),
        t.assignmentExpression(
          '=',
          t.memberExpression(t.identifier('window'), t.identifier('__ProvenHandlerQueue__')),
          t.logicalExpression(
            '||',
            t.memberExpression(t.identifier('window'), t.identifier('__ProvenHandlerQueue__')),
            t.arrayExpression([])
          )
        )
      ),
    ]),
    // return new Promise((resolve, reject) => { ... })
    t.returnStatement(
      t.newExpression(t.identifier('Promise'), [
        t.arrowFunctionExpression(
          [t.identifier('resolve'), t.identifier('reject')],
          t.blockStatement([
            // const shouldSendManifest = !manifestSent;
            t.variableDeclaration('const', [
              t.variableDeclarator(
                t.identifier('shouldSendManifest'),
                t.unaryExpression('!', t.identifier(manifestSentVarName))
              ),
            ]),
            // if (shouldSendManifest) { manifestSent = true; }
            t.ifStatement(
              t.identifier('shouldSendManifest'),
              t.blockStatement([
                t.expressionStatement(
                  t.assignmentExpression(
                    '=',
                    t.identifier(manifestSentVarName),
                    t.booleanLiteral(true)
                  )
                ),
              ])
            ),
            // queue.push({ ... })
            t.expressionStatement(
              t.callExpression(t.memberExpression(t.identifier('queue'), t.identifier('push')), [
                t.objectExpression([
                  t.objectProperty(
                    t.identifier('manifestId'),
                    t.memberExpression(t.identifier(manifestVarName), t.identifier('id'))
                  ),
                  t.objectProperty(
                    t.identifier('manifest'),
                    t.conditionalExpression(
                      t.identifier('shouldSendManifest'),
                      t.identifier(manifestVarName),
                      t.identifier('undefined')
                    )
                  ),
                  t.objectProperty(t.identifier('handler'), t.stringLiteral(handlerSpecifier)),
                  t.objectProperty(
                    t.identifier('args'),
                    t.arrayExpression(paramNames.map((name) => t.identifier(name)))
                  ),
                  t.objectProperty(t.identifier('resolve'), t.identifier('resolve')),
                  t.objectProperty(t.identifier('reject'), t.identifier('reject')),
                ]),
              ])
            ),
          ])
        ),
      ])
    ),
  ]);

  // Create the wrapper function
  if (t.isArrowFunctionExpression(handlerFn)) {
    return t.arrowFunctionExpression(wrapperParams, promiseBody, false);
  } else {
    return t.functionExpression(null, wrapperParams, promiseBody, false, false);
  }
}

/**
 * Get the handler function from the call expression
 */
function getHandlerFunction(
  callExpr: t.CallExpression
): t.FunctionExpression | t.ArrowFunctionExpression | null {
  const callee = callExpr.callee;
  if (!t.isIdentifier(callee)) return null;

  const handlerType = getHandlerType(callee.name);
  if (!handlerType) return null;

  const handlerFn = handlerType.hasOptions ? callExpr.arguments[1] : callExpr.arguments[0];

  if (t.isFunctionExpression(handlerFn) || t.isArrowFunctionExpression(handlerFn)) {
    return handlerFn;
  }

  return null;
}

/**
 * Extract parameter names from function parameters
 */
function extractParamNames(params: Array<t.Node>): string[] {
  return params.map((param, index) => {
    if (t.isIdentifier(param)) {
      return param.name;
    }
    if (t.isAssignmentPattern(param) && t.isIdentifier(param.left)) {
      return param.left.name;
    }
    return `arg${index}`;
  });
}

/**
 * Check if a file imports from @proven-network/handler
 */
export function hasHandlerImport(code: string): boolean {
  let hasImport = false;

  try {
    babel.traverse(babel.parseSync(code, { sourceType: 'module' })!, {
      ImportDeclaration(path) {
        if (path.node.source.value === '@proven-network/handler') {
          hasImport = true;
          path.stop();
        }
      },
    });
  } catch {
    // If parsing fails, assume no import
  }

  return hasImport;
}
