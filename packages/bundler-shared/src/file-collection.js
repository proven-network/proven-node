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
Object.defineProperty(exports, "__esModule", { value: true });
exports.FileCollection = void 0;
const fs = __importStar(require("fs"));
const path = __importStar(require("path"));
const fast_glob_1 = require("fast-glob");
const entrypoint_discovery_1 = require("./entrypoint-discovery");
/**
 * Collects and processes source files for bundling
 */
class FileCollection {
    constructor(projectRoot, options = {}) {
        this.projectRoot = projectRoot;
        this.options = options;
        this.entrypointDiscovery = new entrypoint_discovery_1.EntrypointDiscovery(projectRoot, options.entryPatterns);
    }
    /**
     * Discovers all entrypoints in the project
     */
    async discoverEntrypoints() {
        const sourceFiles = await this.findSourceFiles();
        const entrypoints = [];
        for (const filePath of sourceFiles) {
            try {
                const entrypoint = await this.entrypointDiscovery.analyzeFile(filePath);
                if (entrypoint) {
                    entrypoints.push(entrypoint);
                }
            }
            catch (error) {
                // Skip files that can't be parsed
                console.warn(`Failed to analyze ${filePath}:`, error);
            }
        }
        return entrypoints;
    }
    /**
     * Collects all source files that should be included in the bundle
     */
    async collectSourceFiles(entrypoints) {
        const allFiles = new Set();
        const sourceFiles = [];
        // Add all entrypoint files
        for (const entrypoint of entrypoints) {
            allFiles.add(entrypoint.filePath);
        }
        // Build dependency graph from entrypoints
        const dependencyGraph = await this.buildDependencyGraph(entrypoints);
        // Add all dependencies to the file set
        for (const deps of dependencyGraph.values()) {
            deps.forEach((dep) => allFiles.add(dep));
        }
        // Process each file
        for (const filePath of allFiles) {
            try {
                const sourceFile = await this.processSourceFile(filePath, entrypoints);
                sourceFiles.push(sourceFile);
            }
            catch (error) {
                console.warn(`Failed to process source file ${filePath}:`, error);
            }
        }
        return sourceFiles;
    }
    /**
     * Finds all source files in the project
     */
    async findSourceFiles() {
        const includePatterns = this.options.include || ['**/*.{ts,tsx,js,jsx,mts,mjs}'];
        const excludePatterns = this.options.exclude || [
            'node_modules/**',
            'dist/**',
            'build/**',
            '.git/**',
            '**/*.test.{ts,tsx,js,jsx}',
            '**/*.spec.{ts,tsx,js,jsx}',
            '**/*.d.ts',
        ];
        const files = await (0, fast_glob_1.glob)(includePatterns, {
            cwd: this.projectRoot,
            ignore: excludePatterns,
            absolute: true,
            onlyFiles: true,
        });
        return files;
    }
    /**
     * Builds a dependency graph from the entrypoints
     */
    async buildDependencyGraph(entrypoints) {
        const dependencyGraph = new Map();
        const visited = new Set();
        // Process each entrypoint
        for (const entrypoint of entrypoints) {
            await this.buildDependencyGraphRecursive(entrypoint.filePath, dependencyGraph, visited);
        }
        return dependencyGraph;
    }
    /**
     * Recursively builds dependency graph for a file
     */
    async buildDependencyGraphRecursive(filePath, dependencyGraph, visited) {
        if (visited.has(filePath)) {
            return;
        }
        visited.add(filePath);
        try {
            const entrypoint = await this.entrypointDiscovery.analyzeFile(filePath);
            if (!entrypoint) {
                return;
            }
            const dependencies = [];
            // Process imports to find local file dependencies
            for (const importInfo of entrypoint.imports) {
                if (!importInfo.isProvenHandler && this.isLocalImport(importInfo.module)) {
                    const resolvedPath = await this.resolveImportPathAsync(filePath, importInfo.module);
                    if (resolvedPath && (await this.fileExists(resolvedPath))) {
                        dependencies.push(resolvedPath);
                        // Recursively process dependencies
                        await this.buildDependencyGraphRecursive(resolvedPath, dependencyGraph, visited);
                    }
                }
            }
            dependencyGraph.set(filePath, dependencies);
        }
        catch (error) {
            // Skip files that can't be analyzed
            console.warn(`Failed to build dependency graph for ${filePath}:`, error);
        }
    }
    /**
     * Processes a single source file
     */
    async processSourceFile(filePath, entrypoints) {
        const content = await fs.promises.readFile(filePath, 'utf-8');
        const relativePath = path.relative(this.projectRoot, filePath);
        const fileExtension = path.extname(filePath);
        const isEntrypoint = entrypoints.some((ep) => ep.filePath === filePath);
        // TODO: Handle source maps if requested
        let sourceMap;
        if (this.options.sourceMaps) {
            sourceMap = await this.loadSourceMap(filePath);
        }
        return {
            filePath,
            relativePath,
            content,
            sourceMap,
            size: Buffer.byteLength(content, 'utf8'),
            isEntrypoint,
        };
    }
    /**
     * Checks if an import is a local file import
     */
    isLocalImport(module) {
        return (module.startsWith('./') ||
            module.startsWith('../') ||
            (!module.startsWith('@') && !module.includes('/')));
    }
    /**
     * Resolves an import path to an absolute file path
     */
    resolveImportPath(importPath, fromFile) {
        // Handle external packages
        if (importPath.startsWith('@') ||
            (!importPath.startsWith('./') && !importPath.startsWith('../'))) {
            return null;
        }
        const fromDir = path.dirname(fromFile);
        // Handle relative imports
        if (importPath.startsWith('./') || importPath.startsWith('../')) {
            const resolved = path.resolve(fromDir, importPath);
            return path.normalize(resolved);
        }
        return null;
    }
    /**
     * Async version for internal use
     */
    async resolveImportPathAsync(fromFile, importPath) {
        const resolved = this.resolveImportPath(importPath, fromFile);
        if (!resolved) {
            return null;
        }
        return await this.resolveFileExtension(resolved);
    }
    /**
     * Resolves file extension for imports that might omit extensions
     */
    async resolveFileExtension(basePath) {
        const extensions = ['.ts', '.tsx', '.js', '.jsx', '.mts', '.mjs'];
        // First try the exact path
        if (await this.fileExists(basePath)) {
            return basePath;
        }
        // Try with extensions
        for (const ext of extensions) {
            const withExt = basePath + ext;
            if (await this.fileExists(withExt)) {
                return withExt;
            }
        }
        // Try index files
        for (const ext of extensions) {
            const indexFile = path.join(basePath, `index${ext}`);
            if (await this.fileExists(indexFile)) {
                return indexFile;
            }
        }
        return null;
    }
    /**
     * Loads source map for a file if it exists
     */
    async loadSourceMap(filePath) {
        const sourceMapPath = filePath + '.map';
        try {
            if (await this.fileExists(sourceMapPath)) {
                return await fs.promises.readFile(sourceMapPath, 'utf-8');
            }
        }
        catch (error) {
            // Source map loading failed, continue without it
        }
        return undefined;
    }
    /**
     * Checks if a file exists
     */
    async fileExists(filePath) {
        try {
            const stats = await fs.promises.stat(filePath);
            return stats.isFile();
        }
        catch {
            return false;
        }
    }
}
exports.FileCollection = FileCollection;
