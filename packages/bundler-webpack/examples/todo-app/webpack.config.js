const path = require('path');

module.exports = {
  entry: './src/index.ts',
  output: {
    path: path.resolve(__dirname, 'dist'),
    filename: 'bundle.js',
    clean: true,
    library: {
      name: 'ProvenTodoApp',
      type: 'window',
    },
  },
  module: {
    rules: [
      {
        test: /\.ts$/,
        use: 'ts-loader',
        exclude: /node_modules/,
      },
    ],
  },
  resolve: {
    extensions: ['.ts', '.js'],
  },
  plugins: [
    // TODO: Add ProvenWebpackPlugin when bundler is working
    // new ProvenWebpackPlugin({
    //   output: 'development', 
    //   mode: 'development',
    //   entryPatterns: ['./src/**/*.ts'],
    //   sourceMaps: true,
    //   includeDevDependencies: true,
    // }),
  ],
  devtool: 'source-map',
  mode: 'development',
};