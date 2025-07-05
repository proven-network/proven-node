# @proven-network/bundler

Unified bundler package for Proven Network applications, supporting both Rollup and Webpack build systems.

## Overview

This package provides a comprehensive solution for bundling Proven Network applications with support for multiple build tools. It combines the functionality of the previous separate packages (`@proven-network/bundler-shared`, `@proven-network/bundler-rollup`, and `@proven-network/webpack-plugin`) into a single, unified package.

The bundler analyzes your project to:

- Discover and analyze entrypoint files
- Generate complete bundle manifests
- Collect source files and dependencies
- Detect handler functions and their configurations
- Support both development and production modes

## Installation

```bash
npm install --save-dev @proven-network/bundler
```

## Usage

### Rollup Plugin

```javascript
// rollup.config.js
import { provenRollupPlugin } from '@proven-network/bundler';

export default {
  plugins: [
    provenRollupPlugin({
      output: './dist/proven-bundle.json',
      mode: 'production',
    }),
  ],
};
```

### Webpack Plugin

```javascript
// webpack.config.js
const { ProvenWebpackPlugin } = require('@proven-network/bundler');

module.exports = {
  plugins: [
    new ProvenWebpackPlugin({
      output: './dist/proven-bundle.json',
      mode: 'production',
    }),
  ],
};
```

### Vite Integration

```javascript
// vite.config.js
import { defineConfig } from 'vite';
import { provenRollupPlugin } from '@proven-network/bundler';

export default defineConfig({
  plugins: [
    provenRollupPlugin({
      output: './proven-bundle.json',
      mode: process.env.NODE_ENV === 'production' ? 'production' : 'development',
    }),
  ],
});
```

### Direct Usage

You can also use the core functionality directly:

```typescript
import {
  BundleManifestGenerator,
  EntrypointDiscovery,
  PackageAnalysis,
  FileCollection,
} from '@proven-network/bundler';

// Generate a complete bundle manifest
const generator = new BundleManifestGenerator('/path/to/project', {
  mode: 'production',
  includeDevDependencies: false,
});

const manifest = await generator.generateManifest();
```

## Configuration

### Rollup Plugin Options

```typescript
interface ProvenRollupPluginOptions {
  /** Output destination for bundle manifest */
  output?: string;

  /** Custom patterns to identify entrypoints */
  entryPatterns?: string[];

  /** File patterns to include */
  include?: string[];

  /** File patterns to exclude */
  exclude?: string[];

  /** Include source maps */
  sourceMaps?: boolean;

  /** Build mode */
  mode?: 'development' | 'production';

  /** Custom package.json path */
  packageJsonPath?: string;

  /** Include dev dependencies */
  includeDevDependencies?: boolean;
}
```

### Webpack Plugin Options

```typescript
interface ProvenWebpackPluginOptions {
  /** Output destination for bundle manifest */
  output?: string | 'development';

  /** Custom patterns to identify entrypoints */
  entryPatterns?: string[];

  /** File patterns to include */
  include?: string[];

  /** File patterns to exclude */
  exclude?: string[];

  /** Include source maps */
  sourceMaps?: boolean;

  /** Build mode */
  mode?: 'development' | 'production';

  /** Custom package.json path */
  packageJsonPath?: string;

  /** Include dev dependencies */
  includeDevDependencies?: boolean;
}
```

### Default Options

```javascript
{
  output: './dist/proven-bundle.json', // Rollup
  output: 'development', // Webpack
  entryPatterns: [],
  include: ['**/*.{ts,tsx,js,jsx,mts,mjs}'],
  exclude: [
    'node_modules/**',
    'dist/**',
    'build/**',
    '.git/**',
    '**/*.test.{ts,tsx,js,jsx}',
    '**/*.spec.{ts,tsx,js,jsx}',
    '**/*.d.ts',
  ],
  sourceMaps: false,
  mode: 'development',
  includeDevDependencies: false,
}
```

## Examples

### Development Mode

```javascript
// rollup.config.js
import { provenRollupPlugin } from '@proven-network/bundler';

export default {
  plugins: [
    provenRollupPlugin({
      output: './dist/proven-bundle-dev.json',
      sourceMaps: true,
      includeDevDependencies: true,
      mode: 'development',
    }),
  ],
};
```

### Production Mode

```javascript
// webpack.config.js
const { ProvenWebpackPlugin } = require('@proven-network/bundler');

module.exports = {
  mode: 'production',
  plugins: [
    new ProvenWebpackPlugin({
      output: './dist/proven-bundle.json',
      mode: 'production',
      sourceMaps: false,
      includeDevDependencies: false,
    }),
  ],
};
```

### Next.js Integration

```javascript
// next.config.js
const { ProvenWebpackPlugin } = require('@proven-network/bundler');

/** @type {import('next').NextConfig} */
const config = {
  webpack: (config) => {
    config.plugins.push(
      new ProvenWebpackPlugin({
        output: './proven-bundle.json',
        mode: process.env.NODE_ENV === 'production' ? 'production' : 'development',
      })
    );
    return config;
  },
};

module.exports = config;
```

### Custom File Patterns

```javascript
// Configuration for both Rollup and Webpack
{
  // Include additional patterns for entrypoint discovery
  entryPatterns: ['src/handlers/**/*.ts', 'src/api/**/*.ts'],
  // Include additional file types
  include: [
    '**/*.{ts,tsx,js,jsx}',
    '**/*.json', // Include JSON files
  ],
  // Exclude test files and documentation
  exclude: [
    '**/*.test.{ts,tsx,js,jsx}',
    '**/*.spec.{ts,tsx,js,jsx}',
    'docs/**',
    'examples/**'
  ],
}
```

### Multi-Environment Config

```javascript
// rollup.config.js
import { provenRollupPlugin } from '@proven-network/bundler';

const isDev = process.env.NODE_ENV === 'development';

export default {
  plugins: [
    provenRollupPlugin({
      output: `./dist/proven-bundle${isDev ? '-dev' : ''}.json`,
      mode: isDev ? 'development' : 'production',
      sourceMaps: isDev,
      includeDevDependencies: isDev,
    }),
  ],
};
```

### Multiple Builds

```javascript
// rollup.config.js
import { provenRollupPlugin } from '@proven-network/bundler';

export default [
  {
    input: 'src/index.ts',
    output: { dir: 'dist/esm', format: 'esm' },
    plugins: [
      provenRollupPlugin({
        output: './dist/proven-bundle-esm.json',
      }),
    ],
  },
  {
    input: 'src/index.ts',
    output: { dir: 'dist/cjs', format: 'cjs' },
    plugins: [
      provenRollupPlugin({
        output: './dist/proven-bundle-cjs.json',
      }),
    ],
  },
];
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
  id: string;
  version: string;
  project: ProjectInfo;
  modules: ManifestModule[];
  entrypoints: EntrypointInfo[];
  sources: SourceInfo[];
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
  type: 'http' | 'schedule' | 'event' | 'rpc';
  parameters: ParameterInfo[];
  config?: any;
  line?: number;
  column?: number;
}
```

#### `ProjectInfo`

Project information included in the manifest.

```typescript
interface ProjectInfo {
  name: string;
  version: string;
  description?: string;
  main?: string;
  scripts?: Record<string, string>;
  dependencies?: Record<string, string>;
  devDependencies?: Record<string, string>;
}
```

#### `SourceInfo`

Source file information.

```typescript
interface SourceInfo {
  relativePath: string;
  content: string;
  size?: number;
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

## Bundle Manifest Structure

The generated bundle manifest contains:

```json
{
  "id": "unique-manifest-id",
  "version": "1.0.0",
  "project": {
    "name": "my-app",
    "version": "1.0.0",
    "description": "My application",
    "dependencies": {
      "@proven-network/handler": "^1.0.0"
    }
  },
  "modules": [
    {
      "path": "./src/handler.ts",
      "content": "/* module content */",
      "handlers": [
        {
          "name": "apiHandler",
          "type": "http",
          "parameters": [],
          "config": {
            "path": "/api/hello",
            "method": "GET"
          }
        }
      ],
      "dependencies": []
    }
  ],
  "entrypoints": [
    {
      "filePath": "/path/to/handler.ts",
      "moduleSpecifier": "./src/handler",
      "handlers": [
        {
          "name": "apiHandler",
          "type": "http",
          "parameters": [],
          "config": {
            "path": "/api/hello",
            "method": "GET"
          }
        }
      ],
      "imports": []
    }
  ],
  "sources": [
    {
      "relativePath": "src/handler.ts",
      "content": "/* source content */",
      "size": 1234
    }
  ],
  "dependencies": {
    "production": {
      "@proven-network/handler": "^1.0.0"
    },
    "development": {},
    "all": {
      "@proven-network/handler": "^1.0.0"
    }
  },
  "metadata": {
    "createdAt": "2024-01-01T00:00:00Z",
    "mode": "production",
    "pluginVersion": "1.0.0",
    "fileCount": 1,
    "bundleSize": 1234,
    "sourceMaps": false,
    "buildMode": "production",
    "entrypointCount": 1,
    "handlerCount": 1
  }
}
```

## Handler Detection

The package automatically detects these handler types:

- `runOnHttp` → `'http'`
- `runOnSchedule` → `'schedule'`
- `runOnProvenEvent`, `runOnRadixEvent` → `'event'`
- `runWithOptions`, `run` → `'rpc'`

## Features

### Webpack-Specific Features

#### Development Mode Asset Emission

When using `output: 'development'` with the Webpack plugin, the bundle manifest is emitted as a webpack asset that can be accessed via the dev server:

```
http://localhost:8080/proven-bundle.json
```

This is useful for development workflows where you want to automatically update your Proven Network deployment when code changes.

#### Integration with Webpack Dev Server

The Webpack plugin integrates seamlessly with webpack-dev-server and other webpack-based development tools.

### Rollup-Specific Features

#### Rollup Hooks

The Rollup plugin uses the following Rollup hooks:

- `buildStart`: Initializes the bundle analysis
- `generateBundle`: Analyzes the final bundle and generates the manifest
- `writeBundle`: Outputs the manifest file

#### Vite Compatibility

Works seamlessly with Vite, SvelteKit, and other Rollup-based tools.

### Shared Features

#### Dependency Resolution

The package resolves dependencies by:

1. Parsing `package.json`
2. Validating version specifications
3. Checking installed packages in `node_modules`
4. Building dependency trees
5. Filtering out non-NPM dependencies (file:, git:, etc.)

#### File Collection

Files are collected based on:

- Entrypoint discovery results
- Dependency graph traversal
- Include/exclude patterns
- File extension filtering

Supported file types:

- `.ts`, `.tsx` - TypeScript
- `.js`, `.jsx` - JavaScript
- `.mts`, `.mjs` - ESM modules

#### AST Analysis

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

## Error Handling

The plugins will emit build errors if:

- No entrypoints are found
- Bundle validation fails
- File I/O errors occur
- Invalid configuration is provided

Check your build output for detailed error messages.

## Performance

The bundler is designed to be efficient:

- Uses AST caching for repeated file analysis
- Implements smart dependency graph traversal
- Skips files that don't match patterns
- Parallelizes file processing where possible

For large projects, consider:

- Using more specific `include`/`exclude` patterns
- Excluding unnecessary file types
- Using development mode for faster iterations

## Compatibility

- **Rollup**: 3.x and 4.x
- **Webpack**: 5.x
- **Node.js**: 18+
- **TypeScript**: 5.x
- **Vite**: 4.x and 5.x
- Works with Next.js, SvelteKit, and other frameworks
