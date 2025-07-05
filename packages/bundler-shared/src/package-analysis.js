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
exports.PackageAnalysis = void 0;
const fs = __importStar(require("fs"));
const path = __importStar(require("path"));
const semver = __importStar(require("semver"));
/**
 * Analyzes package.json and resolves dependencies
 */
class PackageAnalysis {
    constructor(projectRoot, packageJsonPath) {
        this.packageJsonCache = null;
        this.projectRoot = projectRoot;
        this.packageJsonPath = packageJsonPath;
    }
    /**
     * Loads and analyzes the project's package.json
     */
    async analyzeProject(packageJsonPath) {
        const pkgPath = packageJsonPath || this.packageJsonPath || path.join(this.projectRoot, 'package.json');
        if (!(await this.fileExists(pkgPath))) {
            throw new Error('Package.json not found');
        }
        let packageJson;
        try {
            packageJson = await this.loadPackageJson(pkgPath);
        }
        catch (error) {
            throw new Error('Invalid package.json format');
        }
        // Validate required fields
        if (!packageJson.name || !packageJson.version) {
            throw new Error('Package.json missing required fields');
        }
        this.packageJsonCache = packageJson;
        return {
            name: packageJson.name,
            version: packageJson.version,
            description: packageJson.description,
            rootDir: this.projectRoot,
            packageJson,
        };
    }
    /**
     * Analyzes dependencies and creates dependency information
     */
    async analyzeDependencies(includeDevDependencies = false) {
        if (!this.packageJsonCache) {
            throw new Error('Project must be analyzed first');
        }
        const pkg = this.packageJsonCache;
        const allDependencies = pkg.dependencies || {};
        const allDevDependencies = pkg.devDependencies || {};
        const peerDependencies = pkg.peerDependencies || {};
        // Filter out non-npm dependencies
        const production = this.filterNpmDependencies(allDependencies);
        const development = includeDevDependencies
            ? this.filterNpmDependencies(allDevDependencies)
            : {};
        const all = { ...production, ...(includeDevDependencies ? development : {}) };
        return {
            production,
            development,
            all,
        };
    }
    /**
     * Validates that all dependencies can be resolved
     */
    async validateDependencies(dependencies) {
        const errors = [];
        for (const [name, versionSpec] of Object.entries(dependencies)) {
            try {
                if (!this.isValidVersionSpec(versionSpec)) {
                    errors.push(`Invalid version specification for ${name}: ${versionSpec}`);
                    continue;
                }
                // Check if package exists in node_modules
                const packagePath = await this.resolvePackagePath(name);
                if (!packagePath) {
                    errors.push(`Package not found: ${name}`);
                    continue;
                }
                // Validate version compatibility
                const installedVersion = await this.getInstalledVersion(packagePath);
                if (installedVersion && !semver.satisfies(installedVersion, versionSpec)) {
                    errors.push(`Version mismatch for ${name}: required ${versionSpec}, installed ${installedVersion}`);
                }
            }
            catch (error) {
                errors.push(`Error validating ${name}: ${error instanceof Error ? error.message : String(error)}`);
            }
        }
        return errors;
    }
    /**
     * Extracts NPM package requirements from dependencies
     */
    extractNpmRequirements(dependencies) {
        const requirements = [];
        for (const [name, versionSpec] of Object.entries(dependencies)) {
            // Skip non-NPM dependencies (file:, git:, etc.)
            if (this.isNpmDependency(versionSpec)) {
                requirements.push({ name, version: versionSpec });
            }
        }
        return requirements;
    }
    /**
     * Loads package.json from a file path
     */
    async loadPackageJson(filePath) {
        try {
            const content = await fs.promises.readFile(filePath, 'utf-8');
            return JSON.parse(content);
        }
        catch (error) {
            throw new Error(`Failed to load package.json from ${filePath}: ${error instanceof Error ? error.message : String(error)}`);
        }
    }
    /**
     * Resolves the dependency tree for the given dependencies
     */
    async resolveDependencyTree(dependencies, devDependencies) {
        const resolved = [];
        const visited = new Set();
        // Process production dependencies
        for (const [name, versionSpec] of Object.entries(dependencies)) {
            if (!visited.has(name)) {
                const dependency = await this.resolveSingleDependency(name, versionSpec, false);
                if (dependency) {
                    resolved.push(dependency);
                    visited.add(name);
                }
            }
        }
        // Process dev dependencies
        for (const [name, versionSpec] of Object.entries(devDependencies)) {
            if (!visited.has(name)) {
                const dependency = await this.resolveSingleDependency(name, versionSpec, true);
                if (dependency) {
                    resolved.push(dependency);
                    visited.add(name);
                }
            }
        }
        return resolved;
    }
    /**
     * Resolves a single dependency
     */
    async resolveSingleDependency(name, versionSpec, isDev) {
        try {
            const packagePath = await this.resolvePackagePath(name);
            if (!packagePath) {
                return null;
            }
            const installedVersion = await this.getInstalledVersion(packagePath);
            if (!installedVersion) {
                return null;
            }
            // Get transitive dependencies
            const packageJsonPath = path.join(packagePath, 'package.json');
            const packageJson = await this.loadPackageJson(packageJsonPath);
            const transitiveDeps = Object.keys(packageJson.dependencies || {});
            return {
                name,
                version: installedVersion,
                isDev,
                dependencies: transitiveDeps,
            };
        }
        catch (error) {
            // Failed to resolve this dependency
            return null;
        }
    }
    /**
     * Resolves the path to a package in node_modules
     */
    async resolvePackagePath(packageName) {
        // Start from project root and walk up the directory tree
        let currentDir = this.projectRoot;
        while (true) {
            const nodeModulesPath = path.join(currentDir, 'node_modules', packageName);
            if (await this.fileExists(nodeModulesPath)) {
                return nodeModulesPath;
            }
            const parentDir = path.dirname(currentDir);
            if (parentDir === currentDir) {
                // Reached filesystem root
                break;
            }
            currentDir = parentDir;
        }
        return null;
    }
    /**
     * Gets the installed version of a package
     */
    async getInstalledVersion(packagePath) {
        try {
            const packageJsonPath = path.join(packagePath, 'package.json');
            const packageJson = await this.loadPackageJson(packageJsonPath);
            return packageJson.version || null;
        }
        catch (error) {
            return null;
        }
    }
    /**
     * Checks if a version specification is valid
     */
    isValidVersionSpec(versionSpec) {
        try {
            // Handle special cases
            if (versionSpec === 'latest' || versionSpec === '*') {
                return true;
            }
            // Check if it's a valid semver range
            return semver.validRange(versionSpec) !== null;
        }
        catch {
            return false;
        }
    }
    /**
     * Checks if a dependency is a valid NPM dependency (public method for tests)
     */
    isValidNpmDependency(name, versionSpec) {
        return this.isNpmDependency(versionSpec);
    }
    /**
     * Filters dependencies to only include NPM packages
     */
    filterNpmDependencies(dependencies) {
        const filtered = {};
        for (const [name, versionSpec] of Object.entries(dependencies)) {
            if (this.isNpmDependency(versionSpec)) {
                filtered[name] = versionSpec;
            }
        }
        return filtered;
    }
    /**
     * Checks if a version specification refers to an NPM package
     */
    isNpmDependency(versionSpec) {
        return (!versionSpec.startsWith('file:') &&
            !versionSpec.startsWith('git:') &&
            !versionSpec.startsWith('git+') &&
            !versionSpec.startsWith('github:') &&
            !versionSpec.startsWith('http:') &&
            !versionSpec.startsWith('https:') &&
            !versionSpec.startsWith('./') &&
            !versionSpec.startsWith('../'));
    }
    /**
     * Checks if a file exists
     */
    async fileExists(filePath) {
        try {
            await fs.promises.access(filePath);
            return true;
        }
        catch {
            return false;
        }
    }
}
exports.PackageAnalysis = PackageAnalysis;
