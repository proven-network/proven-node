# Webpack Todo App Example

This example demonstrates how to use the Proven Network webpack bundler to create a todo application with manifest-based handler execution.

## Features

- ✅ **Handler Discovery**: Webpack plugin automatically discovers handlers across multiple modules
- ✅ **Manifest Generation**: Creates bundle manifest with handler metadata
- ✅ **Local Package References**: Uses local `@proven-network/handler` and `@proven-network/session` packages
- ✅ **Authentication Integration**: Demonstrates user authentication with handlers
- ✅ **CRUD Operations**: Full todo management (Create, Read, Update, Delete)
- ✅ **Filtering & Search**: Filter todos by status and search by text
- ✅ **Statistics**: Real-time todo completion statistics
- ✅ **Development Mode**: Hot-reload support with webpack watch mode

## Project Structure

```
todo-app/
├── src/
│   ├── index.ts              # Main entry point
│   ├── types.ts              # TypeScript type definitions
│   ├── todo-handlers.ts      # Todo CRUD handlers
│   └── auth-handlers.ts      # Authentication handlers
├── dist/                     # Built output (created after build)
├── webpack.config.js         # Webpack configuration with Proven plugin
├── package.json              # Dependencies and scripts
├── tsconfig.json             # TypeScript configuration
├── index.html                # Frontend demo page
├── server.js                 # Development server
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
   Navigate to http://localhost:8080

## Development Workflow

### Watch Mode

For development with automatic rebuilding:

```bash
npm run dev
```

This will watch your source files and rebuild the bundle whenever changes are detected.

### Production Build

For optimized production build:

```bash
npm run build:prod
```

## How It Works

### 1. Handler Discovery

The webpack plugin scans your source files and discovers handlers wrapped with `run()`:

```typescript
import { run } from '@proven-network/handler';

export const createTodo = run((request: CreateTodoRequest): Todo => {
  // Handler implementation
});
```

### 2. Manifest Generation

The plugin generates a bundle manifest that includes:

- Module information and dependencies
- Handler metadata (parameters, types, etc.)
- Bundle statistics and optimization info

### 3. Runtime Execution

The SDK uses the manifest to execute handlers:

```javascript
const manifest = await loadManifest();
const result = await sdk.executeHandler(manifest, 'createTodo', [todoData]);
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

## Key Features Demonstrated

### Local Package Integration

The example uses local packages to demonstrate the development workflow:

- `@proven-network/handler` for handler wrapping
- `@proven-network/session` for authentication

### Webpack Plugin Configuration

```javascript
new ProvenWebpackPlugin({
  output: 'development', // Output manifest as webpack asset
  mode: 'development', // Development mode optimizations
  entryPatterns: ['./src/**/*.ts'], // Scan patterns for handlers
  sourceMaps: true, // Include source map information
  includeDevDependencies: true, // Include dev dependencies in analysis
});
```

### TypeScript Support

Full TypeScript support with:

- Type-safe handler parameters
- Interface definitions for todos and requests
- Proper module resolution for local packages

### Authentication Flow

The app demonstrates:

- Checking authentication status with handlers
- Conditional UI based on authentication
- User permissions and authorization

## Troubleshooting

### Build Issues

1. Ensure all dependencies are installed: `npm install`
2. Check that TypeScript is configured correctly
3. Verify local package paths in package.json

### Runtime Issues

1. Make sure the Proven Network SDK is running (typically on port 3000)
2. Check browser console for error messages
3. Verify manifest generation in the build output

### Authentication Issues

1. Ensure you're signed in to the Proven Network
2. Check that the applicationId matches your setup
3. Verify auth button initialization

## Next Steps

- Add persistent storage backend
- Implement real-time collaboration
- Add more complex filtering and sorting
- Integrate with external APIs
- Add unit tests for handlers
