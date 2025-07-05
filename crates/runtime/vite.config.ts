import { defineConfig } from 'vite';
import { resolve } from 'path';

export default defineConfig({
  build: {
    rollupOptions: {
      input: {
        openai: resolve(__dirname, 'node_modules/openai/index.mjs'),
        uuid: resolve(__dirname, 'node_modules/uuid/dist/esm-browser/index.js'),
      },
      output: {
        dir: 'vendor',
        entryFileNames: (chunkInfo: any) => {
          if (chunkInfo.name === 'openai') {
            return 'openai/index.mjs';
          }
          if (chunkInfo.name === 'uuid') {
            return 'uuid/index.mjs';
          }
          return '[name].mjs';
        },
        format: 'esm',
      },
      external: ['node-fetch', '@types/node-fetch', 'form-data-encoder', 'formdata-node'],
      plugins: [],
    },
    outDir: 'vendor',
    emptyOutDir: true,
  },
});
