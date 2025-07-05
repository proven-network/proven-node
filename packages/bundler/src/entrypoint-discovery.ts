import { parse } from '@babel/parser';
import traverse from '@babel/traverse';
import * as t from '@babel/types';
import * as fs from 'fs';
import * as path from 'path';
import { EntrypointInfo, HandlerInfo, ImportInfo, ParameterInfo } from './types';

/**
 * Discovers entrypoints by analyzing files for @proven-network/handler imports
 */
export class EntrypointDiscovery {
  private readonly projectRoot: string;
  private readonly customPatterns: string[];

  constructor(projectRoot: string, customPatterns: string[] = []) {
    this.projectRoot = projectRoot;
    this.customPatterns = customPatterns;
  }

  /**
   * Analyzes a file to determine if it's an entrypoint and extract handler information
   */
  async analyzeFile(filePath: string): Promise<EntrypointInfo | null> {
    try {
      const content = await fs.promises.readFile(filePath, 'utf-8');
      const relativePath = path.relative(this.projectRoot, filePath);

      // Parse the file with TypeScript support
      const ast = parse(content, {
        sourceType: 'module',
        plugins: [
          'typescript',
          'jsx',
          'decorators-legacy',
          'classProperties',
          'objectRestSpread',
          'asyncGenerators',
          'functionBind',
          'exportDefaultFrom',
          'exportNamespaceFrom',
          'dynamicImport',
          'nullishCoalescingOperator',
          'optionalChaining',
        ],
      });

      const imports: ImportInfo[] = [];
      const handlers: HandlerInfo[] = [];
      let hasProvenHandlerImport = false;

      // Traverse the AST to find imports and handlers
      traverse(ast, {
        ImportDeclaration: (importPath) => {
          const importInfo = this.analyzeImport(importPath.node);
          imports.push(importInfo);

          if (importInfo.isProvenHandler) {
            hasProvenHandlerImport = true;
          }
        },

        // Look for handler function calls (only direct calls, not in exports)
        CallExpression: (callPath) => {
          // Skip if this call is inside an export declaration
          if (
            callPath.findParent(
              (p) => p.isExportNamedDeclaration() || p.isExportDefaultDeclaration()
            )
          ) {
            return;
          }

          const handlerInfo = this.analyzeHandlerCall(callPath.node);
          if (handlerInfo) {
            handlers.push(handlerInfo);
          }
        },

        // Look for exported handlers
        ExportNamedDeclaration: (exportPath) => {
          if (exportPath.node.declaration) {
            const exportHandlers = this.analyzeExportedHandlers(
              exportPath.node.declaration,
              content
            );
            handlers.push(...exportHandlers);
          }
        },

        ExportDefaultDeclaration: (exportPath) => {
          const defaultHandler = this.analyzeDefaultExport(exportPath.node, content);
          if (defaultHandler) {
            handlers.push(defaultHandler);
          }
        },
      });

      // Check if this file qualifies as an entrypoint
      const isEntrypoint =
        hasProvenHandlerImport || handlers.length > 0 || this.matchesCustomPatterns(relativePath);

      if (!isEntrypoint) {
        return null;
      }

      return {
        filePath,
        moduleSpecifier: this.pathToModuleSpecifier(relativePath),
        handlers,
        imports,
      };
    } catch {
      // If parsing fails, this might not be a JavaScript/TypeScript file
      return null;
    }
  }

  /**
   * Analyzes an import declaration
   */
  private analyzeImport(node: t.ImportDeclaration): ImportInfo {
    const module = node.source.value;
    const isProvenHandler =
      module === '@proven-network/handler' || module.startsWith('@proven-network/');

    if (node.specifiers.length === 0) {
      return {
        module,
        type: 'side-effect',
        isProvenHandler,
      };
    }

    // Check for default import
    const defaultImport = node.specifiers.find((spec) => t.isImportDefaultSpecifier(spec));
    if (defaultImport) {
      return {
        module,
        type: 'default',
        localName: defaultImport.local.name,
        isProvenHandler,
      };
    }

    // Check for namespace import
    const namespaceImport = node.specifiers.find((spec) => t.isImportNamespaceSpecifier(spec));
    if (namespaceImport) {
      return {
        module,
        type: 'namespace',
        localName: namespaceImport.local.name,
        isProvenHandler,
      };
    }

    // Named imports
    const namedImports = node.specifiers
      .filter((spec) => t.isImportSpecifier(spec))
      .map((spec) => {
        const importSpec = spec as t.ImportSpecifier;
        return t.isIdentifier(importSpec.imported)
          ? importSpec.imported.name
          : importSpec.imported.value;
      });

    return {
      module,
      type: 'named',
      imports: namedImports,
      isProvenHandler,
    };
  }

  /**
   * Analyzes a call expression to see if it's a handler function
   */
  private analyzeHandlerCall(node: t.CallExpression): HandlerInfo | null {
    if (!t.isIdentifier(node.callee)) {
      return null;
    }

    const functionName = node.callee.name;
    const handlerType = this.getHandlerType(functionName);

    if (handlerType === 'unknown') {
      return null;
    }

    // Extract configuration from the first argument
    let config: Record<string, unknown> | undefined;
    if (node.arguments.length > 0 && t.isObjectExpression(node.arguments[0])) {
      config = this.extractObjectExpression(node.arguments[0]);
    }

    // Extract parameters from the handler function (second argument for run/runWithOptions)
    const parameters = this.extractHandlerParameters(node);

    return {
      name: functionName,
      type: handlerType,
      parameters,
      config,
      line: node.loc?.start.line || 0,
      column: node.loc?.start.column || 0,
    };
  }

  /**
   * Analyzes exported handlers
   */
  private analyzeExportedHandlers(declaration: t.Declaration, _sourceCode: string): HandlerInfo[] {
    const handlers: HandlerInfo[] = [];

    if (t.isVariableDeclaration(declaration)) {
      for (const declarator of declaration.declarations) {
        if (t.isIdentifier(declarator.id) && t.isCallExpression(declarator.init)) {
          const handler = this.analyzeHandlerCall(declarator.init);
          if (handler) {
            handler.name = declarator.id.name;
            handlers.push(handler);
          }
        }
      }
    }

    return handlers;
  }

  /**
   * Analyzes default export for handlers
   */
  private analyzeDefaultExport(
    node: t.ExportDefaultDeclaration,
    _sourceCode: string
  ): HandlerInfo | null {
    if (t.isCallExpression(node.declaration)) {
      const handler = this.analyzeHandlerCall(node.declaration);
      if (handler) {
        handler.name = 'default';
        return handler;
      }
    }
    return null;
  }

  /**
   * Determines handler type from function name
   */
  private getHandlerType(functionName: string): HandlerInfo['type'] | 'unknown' {
    switch (functionName) {
      case 'runOnHttp':
        return 'http';
      case 'runOnSchedule':
        return 'schedule';
      case 'runOnProvenEvent':
      case 'runOnRadixEvent':
        return 'event';
      case 'runWithOptions':
      case 'run':
        return 'rpc';
      default:
        return 'unknown';
    }
  }

  /**
   * Extracts values from an object expression
   */
  private extractObjectExpression(node: t.ObjectExpression): Record<string, unknown> {
    const result: Record<string, unknown> = {};

    for (const property of node.properties) {
      if (t.isObjectProperty(property) && !property.computed) {
        let key: string;

        if (t.isIdentifier(property.key)) {
          key = property.key.name;
        } else if (t.isStringLiteral(property.key)) {
          key = property.key.value;
        } else {
          continue;
        }

        // Extract simple values
        if (t.isStringLiteral(property.value)) {
          result[key] = property.value.value;
        } else if (t.isNumericLiteral(property.value)) {
          result[key] = property.value.value;
        } else if (t.isBooleanLiteral(property.value)) {
          result[key] = property.value.value;
        } else if (t.isNullLiteral(property.value)) {
          result[key] = null;
        }
      }
    }

    return result;
  }

  /**
   * Checks if a file path matches custom patterns
   */
  private matchesCustomPatterns(relativePath: string): boolean {
    return this.customPatterns.some((pattern) => {
      // Simple glob-like matching
      const regex = new RegExp(
        pattern.replace(/\*/g, '.*').replace(/\?/g, '.').replace(/\./g, '\\.')
      );
      return regex.test(relativePath);
    });
  }

  /**
   * Extracts parameter information from a handler function
   */
  private extractHandlerParameters(node: t.CallExpression): ParameterInfo[] {
    const parameters: ParameterInfo[] = [];

    // For run/runWithOptions, the handler function is the last argument
    let handlerArg:
      | t.Expression
      | t.SpreadElement
      | t.JSXNamespacedName
      | t.ArgumentPlaceholder
      | null = null;

    if (node.arguments.length >= 2) {
      const arg = node.arguments[node.arguments.length - 1];
      if (arg && !t.isArgumentPlaceholder(arg)) {
        handlerArg = arg;
      }
    } else if (node.arguments.length === 1) {
      // For runOnHttp etc., the handler is the first argument
      const arg = node.arguments[0];
      if (arg && !t.isArgumentPlaceholder(arg)) {
        handlerArg = arg;
      }
    }

    if (
      !handlerArg ||
      t.isSpreadElement(handlerArg) ||
      t.isJSXNamespacedName(handlerArg) ||
      t.isArgumentPlaceholder(handlerArg)
    ) {
      return parameters;
    }

    // Extract parameters based on the handler type
    if (t.isFunctionExpression(handlerArg) || t.isArrowFunctionExpression(handlerArg)) {
      for (const param of handlerArg.params) {
        const paramInfo = this.extractParameterInfo(param);
        if (paramInfo) {
          parameters.push(paramInfo);
        }
      }
    }

    return parameters;
  }

  /**
   * Extracts information from a function parameter
   */
  private extractParameterInfo(
    param: t.Identifier | t.Pattern | t.RestElement | t.TSParameterProperty
  ): ParameterInfo | null {
    if (t.isIdentifier(param)) {
      return {
        name: param.name,
        type:
          param.typeAnnotation && t.isTSTypeAnnotation(param.typeAnnotation)
            ? this.extractTypeString(param.typeAnnotation.typeAnnotation)
            : 'unknown',
        optional: false,
      };
    }

    if (t.isAssignmentPattern(param)) {
      // Parameter with default value
      if (t.isIdentifier(param.left)) {
        return {
          name: param.left.name,
          type:
            param.left.typeAnnotation && t.isTSTypeAnnotation(param.left.typeAnnotation)
              ? this.extractTypeString(param.left.typeAnnotation.typeAnnotation)
              : 'unknown',
          optional: true,
          defaultValue: this.extractDefaultValue(param.right),
        };
      }
    }

    if (t.isObjectPattern(param)) {
      // Destructured object parameter
      return {
        name: '(destructured)',
        type: 'object',
        optional: false,
      };
    }

    if (t.isArrayPattern(param)) {
      // Destructured array parameter
      return {
        name: '(destructured)',
        type: 'array',
        optional: false,
      };
    }

    if (t.isRestElement(param) && t.isIdentifier(param.argument)) {
      // Rest parameter
      return {
        name: '...' + param.argument.name,
        type: 'any[]',
        optional: true,
      };
    }

    return null;
  }

  /**
   * Extracts a type string from a TypeScript type annotation
   */
  private extractTypeString(typeNode: t.TSType): string {
    if (t.isTSStringKeyword(typeNode)) return 'string';
    if (t.isTSNumberKeyword(typeNode)) return 'number';
    if (t.isTSBooleanKeyword(typeNode)) return 'boolean';
    if (t.isTSAnyKeyword(typeNode)) return 'any';
    if (t.isTSUnknownKeyword(typeNode)) return 'unknown';
    if (t.isTSVoidKeyword(typeNode)) return 'void';
    if (t.isTSArrayType(typeNode)) {
      return this.extractTypeString(typeNode.elementType) + '[]';
    }
    if (t.isTSTypeReference(typeNode) && t.isIdentifier(typeNode.typeName)) {
      return typeNode.typeName.name;
    }
    // For complex types, return a generic description
    return 'any';
  }

  /**
   * Extracts a default value from an expression
   */
  private extractDefaultValue(node: t.Expression): any {
    if (t.isStringLiteral(node)) return node.value;
    if (t.isNumericLiteral(node)) return node.value;
    if (t.isBooleanLiteral(node)) return node.value;
    if (t.isNullLiteral(node)) return null;
    if (t.isIdentifier(node) && node.name === 'undefined') return undefined;
    if (t.isArrayExpression(node)) return [];
    if (t.isObjectExpression(node)) return {};
    return undefined;
  }

  /**
   * Converts a file path to a module specifier
   */
  private pathToModuleSpecifier(relativePath: string): string {
    // Convert Windows paths to Unix-style
    const unixPath = relativePath.replace(/\\/g, '/');

    // Remove file extension for module specifier
    const withoutExt = unixPath.replace(/\.(ts|tsx|js|jsx|mjs)$/, '');

    // Ensure it starts with './' for relative imports
    return withoutExt.startsWith('.') ? withoutExt : `./${withoutExt}`;
  }
}
