import typescript from '@rollup/plugin-typescript';

export default [
  // ESM build
  {
    input: 'src/index.ts',
    output: {
      file: 'dist/index.mjs',
      format: 'esm',
      sourcemap: true,
    },
    plugins: [
      typescript({
        tsconfig: './tsconfig.json',
        declaration: false, // We'll generate declarations separately
      }),
    ],
    external: [
      '@babel/parser',
      '@babel/traverse',
      '@babel/types',
      'fast-glob',
      'semver',
      'fs',
      'path',
    ],
  },
  // CommonJS build
  {
    input: 'src/index.ts',
    output: {
      file: 'dist/index.js',
      format: 'cjs',
      sourcemap: true,
      exports: 'named',
    },
    plugins: [
      typescript({
        tsconfig: './tsconfig.json',
        declaration: true,
        declarationDir: './dist',
        rootDir: './src',
      }),
    ],
    external: [
      '@babel/parser',
      '@babel/traverse',
      '@babel/types',
      'fast-glob',
      'semver',
      'fs',
      'path',
    ],
  },
];
