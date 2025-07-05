import { BundleManifest, BundlerOptions } from './types';
/**
 * Generates bundle manifests from collected data
 */
export declare class BundleManifestGenerator {
    private readonly projectRoot;
    private readonly options;
    private readonly packageAnalysis;
    private readonly fileCollection;
    constructor(projectRoot: string, options?: BundlerOptions);
    /**
     * Generates a complete bundle manifest
     */
    generateManifest(): Promise<BundleManifest>;
    /**
     * Validates the generated manifest
     */
    validateManifest(manifest: BundleManifest): Promise<string[]>;
    /**
     * Optimizes the manifest for production
     */
    optimizeManifest(manifest: BundleManifest): BundleManifest;
    /**
     * Serializes manifest to JSON
     */
    serializeManifest(manifest: BundleManifest): string;
    /**
     * Generates bundle metadata
     */
    private generateMetadata;
    /**
     * Gets the plugin version from package.json
     */
    private getPluginVersion;
    /**
     * Calculates the total bundle size in bytes
     */
    private calculateBundleSize;
    /**
     * Detects circular dependencies in the module graph
     */
    private detectCircularDependencies;
    /**
     * Checks if an import is a local file import
     */
    private isLocalImport;
    /**
     * Resolves an import path to a source file path
     */
    private resolveImportToSourcePath;
}
/**
 * Utility functions for working with bundle manifests
 */
export declare class BundleManifestUtils {
    /**
     * Calculates bundle statistics
     */
    static calculateStats(manifest: BundleManifest): {
        handlersByType: {
            [k: string]: number;
        };
        totalFiles: number;
        entrypointCount: number;
        handlerCount: number;
        dependencyCount: number;
        bundleSize: number;
        hasSourceMaps: boolean;
    };
    /**
     * Validates manifest format
     */
    static validateFormat(manifest: unknown): manifest is BundleManifest;
    /**
     * Extracts handler information for API documentation
     */
    static extractHandlerDocs(manifest: BundleManifest): {
        name: string;
        type: string;
        config: Record<string, unknown>;
        file: string;
        line?: number;
    }[];
}
