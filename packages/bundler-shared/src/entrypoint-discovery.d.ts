import { EntrypointInfo } from './types';
/**
 * Discovers entrypoints by analyzing files for @proven-network/handler imports
 */
export declare class EntrypointDiscovery {
    private readonly projectRoot;
    private readonly customPatterns;
    constructor(projectRoot: string, customPatterns?: string[]);
    /**
     * Analyzes a file to determine if it's an entrypoint and extract handler information
     */
    analyzeFile(filePath: string): Promise<EntrypointInfo | null>;
    /**
     * Analyzes an import declaration
     */
    private analyzeImport;
    /**
     * Analyzes a call expression to see if it's a handler function
     */
    private analyzeHandlerCall;
    /**
     * Analyzes exported handlers
     */
    private analyzeExportedHandlers;
    /**
     * Analyzes default export for handlers
     */
    private analyzeDefaultExport;
    /**
     * Determines handler type from function name
     */
    private getHandlerType;
    /**
     * Extracts values from an object expression
     */
    private extractObjectExpression;
    /**
     * Checks if a file path matches custom patterns
     */
    private matchesCustomPatterns;
    /**
     * Converts a file path to a module specifier
     */
    private pathToModuleSpecifier;
}
