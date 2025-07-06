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
      entry: {
        index: resolve(__dirname, 'src/index.ts'),
        rollup: resolve(__dirname, 'src/rollup.ts'),
        webpack: resolve(__dirname, 'src/webpack.ts'),
        vite: resolve(__dirname, 'src/vite.ts'),
        'webpack-handler-loader': resolve(__dirname, 'src/webpack-handler-loader.ts'),
      },
      name: 'ProvenBundler',
      formats: ['cjs', 'es'],
      fileName: (format, entryName) => {
        if (format === 'cjs') return `${entryName}.js`;
        if (format === 'es') return `${entryName}.mjs`;
        return `${entryName}.${format}.js`;
      },
    },
    rollupOptions: {
      external: [
        '@proven-network/common',
        'fs',
        'path',
        'fs/promises',
        'crypto',
        'os',
        'rollup',
        'webpack',
        'vite',
        'loader-utils',
        '@babel/core',
        '@babel/types',
        '@babel/traverse',
      ],
      output: {
        exports: 'named',
      },
    },
    sourcemap: true,
  },
});
