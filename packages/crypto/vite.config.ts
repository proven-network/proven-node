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
      name: 'ProvenCrypto',
      formats: ['cjs', 'es', 'umd'],
      fileName: (format) => {
        if (format === 'cjs') return 'crypto.js';
        if (format === 'es') return 'crypto.mjs';
        if (format === 'umd') return 'crypto.umd.js';
        return `crypto.${format}.js`;
      },
    },
    rollupOptions: {
      external: ['@radixdlt/radix-engine-toolkit'],
      output: {
        exports: 'named',
      },
    },
    sourcemap: true,
  },
  define: { 'process.env.NODE_ENV': '"production"' },
});
