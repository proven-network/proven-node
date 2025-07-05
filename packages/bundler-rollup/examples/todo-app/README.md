# Rollup Todo App Example

This example demonstrates how to use the Proven Network rollup bundler to create a todo application with manifest-based handler execution and ES module optimization.

## Features

- ✅ **ES Module Bundling**: Leverages Rollup's ES module system with tree shaking
- ✅ **Handler Discovery**: Rollup plugin automatically discovers handlers across multiple modules
- ✅ **Manifest Generation**: Creates bundle manifest with handler metadata and module dependencies
- ✅ **Local Package References**: Uses local `@proven-network/handler` and `@proven-network/session` packages
- ✅ **TypeScript Integration**: Full TypeScript support with @rollup/plugin-typescript
- ✅ **Authentication Integration**: Demonstrates user authentication with handlers
- ✅ **CRUD Operations**: Full todo management (Create, Read, Update, Delete)
- ✅ **Filtering & Search**: Filter todos by status and search by text
- ✅ **Statistics**: Real-time todo completion statistics
- ✅ **Development Mode**: Watch mode with automatic rebuilding
- ✅ **Production Optimization**: Minification and optimization for production builds

## Project Structure

```
todo-app/
├── src/
│   ├── index.ts              # Main entry point (ES modules)
│   ├── types.ts              # TypeScript type definitions
│   ├── todo-handlers.ts      # Todo CRUD handlers
│   └── auth-handlers.ts      # Authentication handlers
├── dist/                     # Built output (created after build)
├── rollup.config.js          # Rollup configuration with Proven plugin
├── package.json              # Dependencies and scripts
├── tsconfig.json             # TypeScript configuration
├── index.html                # Frontend demo page
├── server.js                 # Development server (port 8081)
└── README.md                 # This file
```

## Setup

1. **Install dependencies:**
   ```bash
   npm install
   ```

2. **Build the application:**
   ```bash
   npm run build
   ```

3. **Start the development server:**
   ```bash
   npm run serve
   ```

4. **Open the app:**
   Navigate to http://localhost:8081

## Development Workflow

### Watch Mode
For development with automatic rebuilding:
```bash
npm run dev
```

This will watch your source files and rebuild the bundle whenever changes are detected.

### Production Build
For optimized production build with minification:
```bash
npm run build:prod
```

## How It Works

### 1. ES Module Handler Discovery
The rollup plugin scans your ES module files and discovers handlers:

```typescript
import { run } from '@proven-network/handler';

export const createTodo = run((request: CreateTodoRequest): Todo => {
  // Handler implementation
});
```

### 2. Tree Shaking & Optimization
Rollup automatically removes unused code and optimizes the bundle:
- Dead code elimination
- ES module tree shaking
- Efficient dependency bundling
- Optional minification for production

### 3. Manifest Generation
The plugin generates a comprehensive bundle manifest:
- ES module dependency graph
- Handler metadata extraction
- Type information preservation
- Source map integration

### 4. Runtime Execution
The SDK uses the manifest for efficient handler execution:

```javascript
const manifest = await loadManifest();
const result = await sdk.executeHandler(manifest, 'createTodo', [todoData]);
```

## Rollup Configuration

### Plugin Setup
```javascript
import { provenRollupPlugin } from '@proven-network/bundler-rollup';

export default {
  input: 'src/index.ts',
  plugins: [
    provenRollupPlugin({
      output: 'development',        // Output manifest as asset
      mode: 'development',         // Development optimizations
      entryPatterns: ['./src/**/*.ts'], // Handler discovery patterns
      sourceMaps: true,            // Include source maps
      includeDevDependencies: true, // Dev dependency analysis
    }),
  ],
};
```

### TypeScript Integration
```javascript
import typescript from '@rollup/plugin-typescript';

plugins: [
  typescript({
    tsconfig: './tsconfig.json',
    sourceMap: true,
  }),
  // ... other plugins
]
```

## Available Handlers

### Todo Management
- `createTodo(request: CreateTodoRequest): Todo` - Create a new todo
- `getTodos(filter?: TodoFilter): Todo[]` - Get todos with optional filtering
- `updateTodo(request: UpdateTodoRequest): Todo` - Update an existing todo
- `deleteTodo(todoId: string): boolean` - Delete a todo by ID
- `toggleAllTodos(completed: boolean): Todo[]` - Mark all todos as completed/uncompleted
- `getTodoStats(): object` - Get todo statistics

### Authentication
- `isAuthenticated(): boolean` - Check if user is authenticated
- `getCurrentUser(): object` - Get current user information
- `getUserPermissions(): object` - Get user permissions

## Rollup-Specific Features

### ES Module Benefits
- **Tree Shaking**: Unused code is automatically removed
- **Smaller Bundles**: Only imported code is included
- **Modern JavaScript**: Native ES module support
- **Better Performance**: Optimized module loading

### Plugin Ecosystem
The example integrates with Rollup's plugin ecosystem:
- `@rollup/plugin-typescript` - TypeScript compilation
- `@rollup/plugin-node-resolve` - Node module resolution
- `@rollup/plugin-commonjs` - CommonJS compatibility

### Development vs Production
- **Development**: Fast builds, source maps, readable output
- **Production**: Minification, optimization, smaller bundles

## Comparison with Webpack

| Feature | Rollup | Webpack |
|---------|--------|---------|
| Module System | ES modules native | CommonJS + ES modules |
| Bundle Size | Smaller (tree shaking) | Larger (module overhead) |
| Build Speed | Faster for libraries | Faster for applications |
| Configuration | Simpler | More complex |
| Plugin Ecosystem | Focused | Comprehensive |

## Troubleshooting

### Build Issues
1. Ensure all dependencies are installed: `npm install`
2. Check TypeScript configuration compatibility
3. Verify ES module syntax in source files
4. Check for circular dependencies

### Runtime Issues
1. Make sure the Proven Network SDK is running (port 3000)
2. Check browser console for error messages
3. Verify manifest generation in build output
4. Ensure handler exports are correct

### ES Module Issues
1. Check `"type": "module"` in package.json
2. Use `.mjs` extension if needed
3. Verify import/export syntax
4. Check for CommonJS compatibility issues

## Performance Tips

### Development
- Use watch mode for fast rebuilds
- Enable source maps for debugging
- Use development mode optimizations

### Production
- Enable minification
- Disable source maps for smaller bundles
- Use production mode optimizations
- Consider code splitting for larger apps

## Next Steps

- Add persistent storage backend
- Implement server-side rendering
- Add more complex state management
- Integrate with external APIs
- Add comprehensive test suite
- Implement progressive web app features