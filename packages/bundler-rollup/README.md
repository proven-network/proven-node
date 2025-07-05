# @proven-network/bundler-rollup

Rollup plugin for bundling Proven Network applications.

## Installation

```bash
npm install --save-dev @proven-network/bundler-rollup
```

## Usage

### Basic Setup

```javascript
// rollup.config.js
import { provenRollupPlugin } from '@proven-network/bundler-rollup';

export default {
  // your rollup configuration
  plugins: [
    provenRollupPlugin({
      output: './dist/proven-bundle.json',
      mode: 'production',
    }),
  ],
};
```

### TypeScript

```typescript
// rollup.config.ts
import { provenRollupPlugin } from '@proven-network/bundler-rollup';

export default {
  plugins: [
    provenRollupPlugin({
      output: './dist/proven-bundle.json',
      mode: 'production',
    }),
  ],
};
```

### Development Mode

```javascript
// rollup.config.js
import { provenRollupPlugin } from '@proven-network/bundler-rollup';

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
// rollup.config.js
import { provenRollupPlugin } from '@proven-network/bundler-rollup';

export default {
  plugins: [
    provenRollupPlugin({
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

### Default Options

```javascript
{
  output: './dist/proven-bundle.json',
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

### Vite Integration

```javascript
// vite.config.js
import { defineConfig } from 'vite';
import { provenRollupPlugin } from '@proven-network/bundler-rollup';

export default defineConfig({
  plugins: [
    provenRollupPlugin({
      output: './proven-bundle.json',
      mode: process.env.NODE_ENV === 'production' ? 'production' : 'development',
    }),
  ],
});
```

### Custom File Patterns

```javascript
// rollup.config.js
import { provenRollupPlugin } from '@proven-network/bundler-rollup';

export default {
  plugins: [
    provenRollupPlugin({
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
// rollup.config.js
import { provenRollupPlugin } from '@proven-network/bundler-rollup';

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
import { provenRollupPlugin } from '@proven-network/bundler-rollup';

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

### Custom Package.json Location

```javascript
// rollup.config.js
import { provenRollupPlugin } from '@proven-network/bundler-rollup';

export default {
  plugins: [
    provenRollupPlugin({
      packageJsonPath: './packages/api/package.json',
      output: './dist/api-bundle.json',
    }),
  ],
};
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

## Error Handling

The plugin will emit Rollup build errors if:

- No entrypoints are found
- Bundle validation fails
- File I/O errors occur
- Invalid configuration is provided

Check your Rollup build output for detailed error messages.

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

## Rollup Hooks

The plugin uses the following Rollup hooks:

- `buildStart`: Initializes the bundle analysis
- `generateBundle`: Analyzes the final bundle and generates the manifest
- `writeBundle`: Outputs the manifest file

## Compatibility

- Rollup 3.x and 4.x
- Node.js 18+
- TypeScript 5.x
- Works with Vite, SvelteKit, and other Rollup-based tools
