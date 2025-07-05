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
      name: 'ProvenSQL',
      formats: ['cjs', 'es', 'umd'],
      fileName: (format) => {
        if (format === 'cjs') return 'sql.js';
        if (format === 'es') return 'sql.mjs';
        if (format === 'umd') return 'sql.umd.js';
        return `sql.${format}.js`;
      },
    },
    rollupOptions: {
      output: {
        exports: 'named',
      },
    },
    sourcemap: true,
  },
  define: { 'process.env.NODE_ENV': '"production"' },
});
