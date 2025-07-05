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
      name: 'ProvenCommon',
      formats: ['cjs', 'es'],
      fileName: (format) => {
        if (format === 'cjs') return 'index.js';
        if (format === 'es') return 'index.mjs';
        return `index.${format}.js`;
      },
    },
    rollupOptions: {
      external: ['tslib'],
      output: {
        exports: 'named',
      },
    },
    sourcemap: true,
  },
});
