# @proven-network/bundler-shared

Core utilities and types for Proven Network bundler plugins.

## Installation

```bash
npm install @proven-network/bundler-shared
```

## Usage

This package is primarily intended for use by bundler plugins, but you can also use it directly to analyze and process Proven Network applications.

```typescript
import {
  BundleManifestGenerator,
  EntrypointDiscovery,
  PackageAnalysis,
  FileCollection,
} from '@proven-network/bundler-shared';

// Generate a complete bundle manifest
const generator = new BundleManifestGenerator('/path/to/project', {
  mode: 'production',
  includeDevDependencies: false,
});

const manifest = await generator.generateManifest();
```

## API Reference

### Classes

#### `BundleManifestGenerator`

Main class for generating bundle manifests.

```typescript
const generator = new BundleManifestGenerator(projectRoot, options);
const manifest = await generator.generateManifest();
```

#### `EntrypointDiscovery`

Discovers and analyzes entrypoint files.

```typescript
const discovery = new EntrypointDiscovery(projectRoot, customPatterns);
const entrypoint = await discovery.analyzeFile('/path/to/file.ts');
```

#### `PackageAnalysis`

Analyzes package.json and dependencies.

```typescript
const analysis = new PackageAnalysis(projectRoot);
const project = await analysis.analyzeProject();
const dependencies = await analysis.analyzeDependencies();
```

#### `FileCollection`

Collects and processes source files.

```typescript
const collection = new FileCollection(projectRoot, options);
const entrypoints = await collection.discoverEntrypoints();
const sources = await collection.collectSourceFiles(entrypoints);
```

### Types

#### `BundleManifest`

Complete bundle manifest structure.

```typescript
interface BundleManifest {
  project: ProjectInfo;
  entrypoints: EntrypointInfo[];
  sources: SourceFile[];
  dependencies: DependencyInfo;
  metadata: BundleMetadata;
}
```

#### `EntrypointInfo`

Information about a discovered entrypoint.

```typescript
interface EntrypointInfo {
  filePath: string;
  moduleSpecifier: string;
  handlers: HandlerInfo[];
  imports: ImportInfo[];
}
```

#### `HandlerInfo`

Information about a handler function.

```typescript
interface HandlerInfo {
  name: string;
  type: 'http' | 'schedule' | 'event' | 'rpc' | 'unknown';
  config?: Record<string, unknown>;
  line?: number;
  column?: number;
}
```

### Utilities

#### `createDefaultOptions()`

Creates default bundler options.

```typescript
const defaultOptions = createDefaultOptions();
```

#### `validateOptions(options)`

Validates bundler options.

```typescript
const errors = validateOptions(options);
if (errors.length > 0) {
  throw new Error(`Invalid options: ${errors.join(', ')}`);
}
```

#### `mergeOptions(userOptions)`

Merges user options with defaults.

```typescript
const finalOptions = mergeOptions(userOptions);
```

## Handler Detection

The package automatically detects these handler types:

- `runOnHttp` → `'http'`
- `runOnSchedule` → `'schedule'`
- `runOnProvenEvent`, `runOnRadixEvent` → `'event'`
- `runWithOptions`, `run` → `'rpc'`

## Dependency Resolution

The package resolves dependencies by:

1. Parsing `package.json`
2. Validating version specifications
3. Checking installed packages in `node_modules`
4. Building dependency trees
5. Filtering out non-NPM dependencies (file:, git:, etc.)

## File Collection

Files are collected based on:

- Entrypoint discovery results
- Dependency graph traversal
- Include/exclude patterns
- File extension filtering

Supported file types:

- `.ts`, `.tsx` - TypeScript
- `.js`, `.jsx` - JavaScript
- `.mts`, `.mjs` - ESM modules

## AST Analysis

The package uses Babel parser with these plugins:

- `typescript` - TypeScript syntax
- `jsx` - JSX syntax
- `decorators-legacy` - Decorator support
- `classProperties` - Class properties
- `objectRestSpread` - Object spread
- `asyncGenerators` - Async generators
- `functionBind` - Function bind operator
- `exportDefaultFrom` - Export default from
- `exportNamespaceFrom` - Export namespace from
- `dynamicImport` - Dynamic imports
- `nullishCoalescingOperator` - Nullish coalescing
- `optionalChaining` - Optional chaining
