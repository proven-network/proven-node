import { ProjectInfo, DependencyInfo } from './types';
/**
 * Analyzes package.json and resolves dependencies
 */
export declare class PackageAnalysis {
    private readonly projectRoot;
    private readonly packageJsonPath?;
    private packageJsonCache;
    constructor(projectRoot: string, packageJsonPath?: string);
    /**
     * Loads and analyzes the project's package.json
     */
    analyzeProject(packageJsonPath?: string): Promise<ProjectInfo>;
    /**
     * Analyzes dependencies and creates dependency information
     */
    analyzeDependencies(includeDevDependencies?: boolean): Promise<DependencyInfo>;
    /**
     * Validates that all dependencies can be resolved
     */
    validateDependencies(dependencies: Record<string, string>): Promise<string[]>;
    /**
     * Extracts NPM package requirements from dependencies
     */
    extractNpmRequirements(dependencies: Record<string, string>): Array<{
        name: string;
        version: string;
    }>;
    /**
     * Loads package.json from a file path
     */
    private loadPackageJson;
    /**
     * Resolves the dependency tree for the given dependencies
     */
    private resolveDependencyTree;
    /**
     * Resolves a single dependency
     */
    private resolveSingleDependency;
    /**
     * Resolves the path to a package in node_modules
     */
    private resolvePackagePath;
    /**
     * Gets the installed version of a package
     */
    private getInstalledVersion;
    /**
     * Checks if a version specification is valid
     */
    private isValidVersionSpec;
    /**
     * Checks if a dependency is a valid NPM dependency (public method for tests)
     */
    isValidNpmDependency(name: string, versionSpec: string): boolean;
    /**
     * Filters dependencies to only include NPM packages
     */
    private filterNpmDependencies;
    /**
     * Checks if a version specification refers to an NPM package
     */
    private isNpmDependency;
    /**
     * Checks if a file exists
     */
    private fileExists;
}
