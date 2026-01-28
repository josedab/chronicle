const path = require('path');

module.exports = {
  mode: 'production',
  entry: './src/module.ts',
  output: {
    filename: 'module.js',
    path: path.resolve(__dirname, 'dist'),
    libraryTarget: 'amd',
  },
  externals: [
    '@grafana/data',
    '@grafana/runtime',
    '@grafana/ui',
    'react',
    'react-dom',
  ],
  resolve: {
    extensions: ['.ts', '.tsx', '.js'],
  },
  module: {
    rules: [
      {
        test: /\.tsx?$/,
        use: 'ts-loader',
        exclude: /node_modules/,
      },
    ],
  },
};
