import { defineConfig } from 'vite';
import { resolve } from 'path';
import dts from 'vite-plugin-dts';

export default defineConfig({
  plugins: [
    dts({
      include: ['src/**/*'],
      outDir: 'dist',
      insertTypesEntry: true,
    }),
  ],
  build: {
    lib: {
      entry: resolve(__dirname, 'src/index.ts'),
      name: 'ProvenHandler',
      formats: ['cjs', 'es', 'umd'],
      fileName: (format) => {
        if (format === 'cjs') return 'handler.js';
        if (format === 'es') return 'handler.mjs';
        if (format === 'umd') return 'handler.umd.js';
        return `handler.${format}.js`;
      },
    },
    rollupOptions: {
      external: ['@proven-network/crypto'],
      output: {
        exports: 'named',
      },
    },
    sourcemap: true,
  },
  define: { 'process.env.NODE_ENV': '"production"' },
});
