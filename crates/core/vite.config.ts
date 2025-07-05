import { defineConfig } from 'vite';
import { resolve } from 'path';

// Check if we're building a specific entry
const entry = process.env.VITE_ENTRY;

const entries = {
  bridge: 'static/iframes/bridge/bridge.ts',
  connect: 'static/iframes/connect/connect.ts',
  register: 'static/iframes/register/register.ts',
  rpc: 'static/iframes/rpc/rpc.ts',
  'broker-worker': 'static/workers/broker-worker.ts',
  'rpc-worker': 'static/workers/rpc-worker.ts',
};

const pathMap = {
  bridge: 'iframes/bridge/bridge.js',
  connect: 'iframes/connect/connect.js',
  register: 'iframes/register/register.js',
  rpc: 'iframes/rpc/rpc.js',
  'broker-worker': 'workers/broker-worker.js',
  'rpc-worker': 'workers/rpc-worker.js',
};

// If building a specific entry, build only that one
const buildInput = entry ? { [entry]: entries[entry] } : entries;

export default defineConfig({
  build: {
    lib: entry
      ? {
          entry: entries[entry],
          name: entry.replace('-', '_'), // Convert hyphens to underscores for valid JS identifiers
          fileName: () => pathMap[entry],
          formats: ['iife'],
        }
      : undefined,
    rollupOptions: entry
      ? {
          output: {
            // For single entry builds, inline everything
            inlineDynamicImports: true,
          },
        }
      : {
          input: buildInput,
          output: {
            entryFileNames: (chunkInfo: any) => {
              return pathMap[chunkInfo.name] || '[name].js';
            },
            chunkFileNames: '[name]-[hash].js',
            assetFileNames: '[name].[ext]',
            format: 'es',
            inlineDynamicImports: false,
            manualChunks: undefined,
          },
          external: [],
        },
    outDir: 'dist',
    emptyOutDir: !entry, // Only empty for full builds
    sourcemap: true,
    chunkSizeWarningLimit: 2000,
  },
  resolve: {
    alias: {
      '@proven-network/common': resolve(__dirname, '../../packages/common/src'),
    },
  },
});
