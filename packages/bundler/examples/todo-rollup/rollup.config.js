import { provenRollupPlugin } from '../../dist/rollup.js';
import typescript from '@rollup/plugin-typescript';
import { nodeResolve } from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';

const isProduction = process.env.NODE_ENV === 'production';

export default {
  input: 'src/index.ts',
  output: {
    file: 'dist/bundle.js',
    format: 'iife',
    name: 'ProvenTodoApp',
    sourcemap: !isProduction,
  },
  plugins: [
    nodeResolve({
      preferBuiltins: false,
    }),
    commonjs(),
    typescript({
      tsconfig: './tsconfig.json',
      sourceMap: !isProduction,
    }),
    provenRollupPlugin({
      output: 'development',
      mode: isProduction ? 'production' : 'development',
      sourceMaps: !isProduction,
      includeDevDependencies: true,
    }),
  ],
  external: [],
};
