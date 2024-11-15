import { nodeResolve } from '@rollup/plugin-node-resolve';

export default {
  input: 'node_modules/uuid/dist/esm-browser/index.js',
  output: {
    file: 'vendor/uuid/uuid.mjs',
    format: 'esm'
  },
  plugins: [nodeResolve()]
};
