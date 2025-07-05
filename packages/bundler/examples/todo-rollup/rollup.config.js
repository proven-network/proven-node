// import { provenRollupPlugin } from '../../dist/index.js';
import typescript from '@rollup/plugin-typescript';
import { nodeResolve } from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';

const isProduction = process.env.NODE_ENV === 'production';

export default {
  input: 'src/index.ts',
  output: {
    file: 'dist/bundle.js',
    format: 'es',
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
    // TODO: Add provenRollupPlugin when bundler is working
    // provenRollupPlugin({
    //   output: 'development',
    //   mode: isProduction ? 'production' : 'development',
    //   entryPatterns: ['./src/**/*.ts'],
    //   sourceMaps: !isProduction,
    //   includeDevDependencies: true,
    // }),
  ],
  external: [],
};
