import commonjs from '@rollup/plugin-commonjs';
import { nodeResolve } from '@rollup/plugin-node-resolve';

export default [
  {
    input: 'node_modules/openai/index.mjs',
    output: {
      file: 'vendor/openai/index.mjs',
      format: 'esm'
    },
    plugins: [nodeResolve()],
    external: ['node-fetch', '@types/node-fetch', 'form-data-encoder', 'formdata-node']
  },
  {
    input: 'node_modules/uuid/dist/esm-browser/index.js',
    output: {
      file: 'vendor/uuid/index.mjs',
      format: 'esm'
    },
    plugins: [nodeResolve()]
  },
  {
    input: 'node_modules/typescript/lib/typescript.js',
    output: {
      file: 'vendor/typescript/index.mjs',
      format: 'esm'
    },
    plugins: [commonjs(), nodeResolve()]
  },
];
