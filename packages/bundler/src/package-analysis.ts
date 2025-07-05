import * as fs from 'fs';
import * as path from 'path';
import * as semver from 'semver';
import { ProjectInfo, DependencyInfo, ResolvedDependency } from './types';

/**
 * Analyzes package.json and resolves dependencies
 */
export class PackageAnalysis {
  private readonly projectRoot: string;
  private readonly packageJsonPath?: string;
  private packageJsonCache: Record<string, unknown> | null = null;

  constructor(projectRoot: string, packageJsonPath?: string) {
    this.projectRoot = projectRoot;
    this.packageJsonPath = packageJsonPath || '';
  }

  /**
   * Loads and analyzes the project's package.json
   */
  async analyzeProject(packageJsonPath?: string): Promise<ProjectInfo> {
    const pkgPath =
      packageJsonPath || this.packageJsonPath || path.join(this.projectRoot, 'package.json');

    if (!(await this.fileExists(pkgPath))) {
      throw new Error('Package.json not found');
    }

    let packageJson: Record<string, unknown>;
    try {
      packageJson = await this.loadPackageJson(pkgPath);
    } catch {
      throw new Error('Invalid package.json format');
    }

    // Validate required fields
    if (!packageJson.name || !packageJson.version) {
      throw new Error('Package.json missing required fields');
    }

    this.packageJsonCache = packageJson;

    return {
      name: packageJson.name as string,
      version: packageJson.version as string,
      description: (packageJson.description as string) || '',
      rootDir: this.projectRoot,
      packageJson,
    };
  }

  /**
   * Analyzes dependencies and creates dependency information
   */
  async analyzeDependencies(includeDevDependencies = false): Promise<DependencyInfo> {
    if (!this.packageJsonCache) {
      throw new Error('Project must be analyzed first');
    }

    const pkg = this.packageJsonCache;
    const allDependencies = (pkg.dependencies as Record<string, string>) || {};
    const allDevDependencies = (pkg.devDependencies as Record<string, string>) || {};

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
  async validateDependencies(dependencies: Record<string, string>): Promise<string[]> {
    const errors: string[] = [];

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
          errors.push(
            `Version mismatch for ${name}: required ${versionSpec}, installed ${installedVersion}`
          );
        }
      } catch (error) {
        errors.push(
          `Error validating ${name}: ${error instanceof Error ? error.message : String(error)}`
        );
      }
    }

    return errors;
  }

  /**
   * Extracts NPM package requirements from dependencies
   */
  extractNpmRequirements(
    dependencies: Record<string, string>
  ): Array<{ name: string; version: string }> {
    const requirements: Array<{ name: string; version: string }> = [];

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
  private async loadPackageJson(filePath: string): Promise<Record<string, unknown>> {
    try {
      const content = await fs.promises.readFile(filePath, 'utf-8');
      return JSON.parse(content);
    } catch (error) {
      throw new Error(
        `Failed to load package.json from ${filePath}: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  /**
   * Resolves the dependency tree for the given dependencies
   */
  private async resolveDependencyTree(
    dependencies: Record<string, string>,
    devDependencies: Record<string, string>
  ): Promise<ResolvedDependency[]> {
    const resolved: ResolvedDependency[] = [];
    const visited = new Set<string>();

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
  private async resolveSingleDependency(
    name: string,
    versionSpec: string,
    isDev: boolean
  ): Promise<ResolvedDependency | null> {
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
      const transitiveDeps = Object.keys(
        (packageJson.dependencies as Record<string, string>) || {}
      );

      return {
        name,
        version: installedVersion,
        isDev,
        dependencies: transitiveDeps,
      };
    } catch {
      // Failed to resolve this dependency
      return null;
    }
  }

  /**
   * Resolves the path to a package in node_modules
   */
  private async resolvePackagePath(packageName: string): Promise<string | null> {
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
  private async getInstalledVersion(packagePath: string): Promise<string | null> {
    try {
      const packageJsonPath = path.join(packagePath, 'package.json');
      const packageJson = await this.loadPackageJson(packageJsonPath);
      return (packageJson.version as string) || null;
    } catch {
      return null;
    }
  }

  /**
   * Checks if a version specification is valid
   */
  private isValidVersionSpec(versionSpec: string): boolean {
    try {
      // Handle special cases
      if (versionSpec === 'latest' || versionSpec === '*') {
        return true;
      }

      // Check if it's a valid semver range
      return semver.validRange(versionSpec) !== null;
    } catch {
      return false;
    }
  }

  /**
   * Checks if a dependency is a valid NPM dependency (public method for tests)
   */
  isValidNpmDependency(name: string, versionSpec: string): boolean {
    return this.isNpmDependency(versionSpec);
  }

  /**
   * Filters dependencies to only include NPM packages
   */
  private filterNpmDependencies(dependencies: Record<string, string>): Record<string, string> {
    const filtered: Record<string, string> = {};
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
  private isNpmDependency(versionSpec: string): boolean {
    return (
      !versionSpec.startsWith('file:') &&
      !versionSpec.startsWith('git:') &&
      !versionSpec.startsWith('git+') &&
      !versionSpec.startsWith('github:') &&
      !versionSpec.startsWith('http:') &&
      !versionSpec.startsWith('https:') &&
      !versionSpec.startsWith('./') &&
      !versionSpec.startsWith('../')
    );
  }

  /**
   * Checks if a file exists
   */
  private async fileExists(filePath: string): Promise<boolean> {
    try {
      await fs.promises.access(filePath);
      return true;
    } catch {
      return false;
    }
  }
}
