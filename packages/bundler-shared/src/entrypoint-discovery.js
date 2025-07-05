"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.EntrypointDiscovery = void 0;
const parser_1 = require("@babel/parser");
const traverse_1 = __importDefault(require("@babel/traverse"));
const t = __importStar(require("@babel/types"));
const fs = __importStar(require("fs"));
const path = __importStar(require("path"));
/**
 * Discovers entrypoints by analyzing files for @proven-network/handler imports
 */
class EntrypointDiscovery {
    constructor(projectRoot, customPatterns = []) {
        this.projectRoot = projectRoot;
        this.customPatterns = customPatterns;
    }
    /**
     * Analyzes a file to determine if it's an entrypoint and extract handler information
     */
    async analyzeFile(filePath) {
        try {
            const content = await fs.promises.readFile(filePath, 'utf-8');
            const relativePath = path.relative(this.projectRoot, filePath);
            // Parse the file with TypeScript support
            const ast = (0, parser_1.parse)(content, {
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
            const imports = [];
            const handlers = [];
            let hasProvenHandlerImport = false;
            // Traverse the AST to find imports and handlers
            (0, traverse_1.default)(ast, {
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
                    if (callPath.findParent((p) => p.isExportNamedDeclaration() || p.isExportDefaultDeclaration())) {
                        return;
                    }
                    const handlerInfo = this.analyzeHandlerCall(callPath.node, content);
                    if (handlerInfo) {
                        handlers.push(handlerInfo);
                    }
                },
                // Look for exported handlers
                ExportNamedDeclaration: (exportPath) => {
                    if (exportPath.node.declaration) {
                        const exportHandlers = this.analyzeExportedHandlers(exportPath.node.declaration, content);
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
            const isEntrypoint = hasProvenHandlerImport || handlers.length > 0 || this.matchesCustomPatterns(relativePath);
            if (!isEntrypoint) {
                return null;
            }
            return {
                filePath,
                moduleSpecifier: this.pathToModuleSpecifier(relativePath),
                handlers,
                imports,
            };
        }
        catch (error) {
            // If parsing fails, this might not be a JavaScript/TypeScript file
            return null;
        }
    }
    /**
     * Analyzes an import declaration
     */
    analyzeImport(node) {
        const module = node.source.value;
        const isProvenHandler = module === '@proven-network/handler' || module.startsWith('@proven-network/');
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
            const importSpec = spec;
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
    analyzeHandlerCall(node, sourceCode) {
        if (!t.isIdentifier(node.callee)) {
            return null;
        }
        const functionName = node.callee.name;
        const handlerType = this.getHandlerType(functionName);
        if (handlerType === 'unknown') {
            return null;
        }
        // Extract configuration from the first argument
        let config;
        if (node.arguments.length > 0 && t.isObjectExpression(node.arguments[0])) {
            config = this.extractObjectExpression(node.arguments[0]);
        }
        return {
            name: functionName,
            type: handlerType,
            config,
            line: node.loc?.start.line,
            column: node.loc?.start.column,
        };
    }
    /**
     * Analyzes exported handlers
     */
    analyzeExportedHandlers(declaration, sourceCode) {
        const handlers = [];
        if (t.isVariableDeclaration(declaration)) {
            for (const declarator of declaration.declarations) {
                if (t.isIdentifier(declarator.id) && t.isCallExpression(declarator.init)) {
                    const handler = this.analyzeHandlerCall(declarator.init, sourceCode);
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
    analyzeDefaultExport(node, sourceCode) {
        if (t.isCallExpression(node.declaration)) {
            const handler = this.analyzeHandlerCall(node.declaration, sourceCode);
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
    getHandlerType(functionName) {
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
    extractObjectExpression(node) {
        const result = {};
        for (const property of node.properties) {
            if (t.isObjectProperty(property) && !property.computed) {
                let key;
                if (t.isIdentifier(property.key)) {
                    key = property.key.name;
                }
                else if (t.isStringLiteral(property.key)) {
                    key = property.key.value;
                }
                else {
                    continue;
                }
                // Extract simple values
                if (t.isStringLiteral(property.value)) {
                    result[key] = property.value.value;
                }
                else if (t.isNumericLiteral(property.value)) {
                    result[key] = property.value.value;
                }
                else if (t.isBooleanLiteral(property.value)) {
                    result[key] = property.value.value;
                }
                else if (t.isNullLiteral(property.value)) {
                    result[key] = null;
                }
            }
        }
        return result;
    }
    /**
     * Checks if a file path matches custom patterns
     */
    matchesCustomPatterns(relativePath) {
        return this.customPatterns.some((pattern) => {
            // Simple glob-like matching
            const regex = new RegExp(pattern.replace(/\*/g, '.*').replace(/\?/g, '.').replace(/\./g, '\\.'));
            return regex.test(relativePath);
        });
    }
    /**
     * Converts a file path to a module specifier
     */
    pathToModuleSpecifier(relativePath) {
        // Convert Windows paths to Unix-style
        const unixPath = relativePath.replace(/\\/g, '/');
        // Remove file extension for module specifier
        const withoutExt = unixPath.replace(/\.(ts|tsx|js|jsx|mjs)$/, '');
        // Ensure it starts with './' for relative imports
        return withoutExt.startsWith('.') ? withoutExt : `./${withoutExt}`;
    }
}
exports.EntrypointDiscovery = EntrypointDiscovery;
