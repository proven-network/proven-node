# @proven-network/webpack-plugin

Webpack plugin for bundling Proven Network applications.

## Installation

```bash
npm install --save-dev @proven-network/webpack-plugin
```

## Usage

### Basic Setup

```javascript
// webpack.config.js
const ProvenWebpackPlugin = require('@proven-network/webpack-plugin');

module.exports = {
  // your webpack configuration
  plugins: [
    new ProvenWebpackPlugin({
      output: './dist/proven-bundle.json',
      mode: 'production',
    }),
  ],
};
```

### TypeScript

```typescript
// webpack.config.ts
import { ProvenWebpackPlugin } from '@proven-network/webpack-plugin';

export default {
  plugins: [
    new ProvenWebpackPlugin({
      output: './dist/proven-bundle.json',
      mode: 'production',
    }),
  ],
};
```

### Development Mode

```javascript
// webpack.config.js
const ProvenWebpackPlugin = require('@proven-network/webpack-plugin');

module.exports = {
  mode: 'development',
  plugins: [
    new ProvenWebpackPlugin({
      output: 'development', // Emits as webpack asset
      sourceMaps: true,
      includeDevDependencies: true,
    }),
  ],
};
```

### Production Mode

```javascript
// webpack.config.js
const ProvenWebpackPlugin = require('@proven-network/webpack-plugin');

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

## Configuration

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
  output: 'development',
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

### Next.js Integration

```javascript
// next.config.js
const ProvenWebpackPlugin = require('@proven-network/webpack-plugin');

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
// webpack.config.js
const ProvenWebpackPlugin = require('@proven-network/webpack-plugin');

module.exports = {
  plugins: [
    new ProvenWebpackPlugin({
      // Include additional patterns for entrypoint discovery
      entryPatterns: ['src/handlers/**/*.ts', 'src/api/**/*.ts'],
      // Include additional file types
      include: [
        '**/*.{ts,tsx,js,jsx}',
        '**/*.json', // Include JSON files
      ],
      // Exclude test files and documentation
      exclude: ['**/*.test.{ts,tsx,js,jsx}', '**/*.spec.{ts,tsx,js,jsx}', 'docs/**', 'examples/**'],
    }),
  ],
};
```

### Multi-Environment Config

```javascript
// webpack.config.js
const ProvenWebpackPlugin = require('@proven-network/webpack-plugin');

const isDev = process.env.NODE_ENV === 'development';

module.exports = {
  plugins: [
    new ProvenWebpackPlugin({
      output: isDev ? 'development' : './dist/proven-bundle.json',
      mode: isDev ? 'development' : 'production',
      sourceMaps: isDev,
      includeDevDependencies: isDev,
    }),
  ],
};
```

### Custom Package.json Location

```javascript
// webpack.config.js
const ProvenWebpackPlugin = require('@proven-network/webpack-plugin');

module.exports = {
  plugins: [
    new ProvenWebpackPlugin({
      packageJsonPath: './packages/api/package.json',
      output: './dist/api-bundle.json',
    }),
  ],
};
```

## Integration with Webpack Dev Server

When using `output: 'development'`, the plugin emits the bundle manifest as a webpack asset that can be accessed via the dev server:

```
http://localhost:8080/proven-bundle.json
```

This is useful for development workflows where you want to automatically update your Proven Network deployment when code changes.

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

## Error Handling

The plugin will emit webpack compilation errors if:

- No entrypoints are found
- Bundle validation fails
- File I/O errors occur
- Invalid configuration is provided

Check your webpack build output for detailed error messages.

## Performance

The plugin is designed to be efficient:

- Uses AST caching for repeated file analysis
- Implements smart dependency graph traversal
- Skips files that don't match patterns
- Parallelizes file processing where possible

For large projects, consider:

- Using more specific `include`/`exclude` patterns
- Excluding unnecessary file types
- Using development mode for faster iterations
