module.exports = {
  entry: './src/main.ts',
  output: {
    path: __dirname + '/build/webpack',
    filename: 'bundle.js'
  },
  resolve: {
    extensions: ['.ts', '.tsx', '.js']
  },
  module: {
    rules: [
      {
        test: /\.tsx?$/,
        loader: 'ts-loader',
        exclude: /node_modules/,
        options: {
          transpileOnly: true
        }
      }
    ]
  },
  devServer: {
    contentBase: [
      'public'
    ]
  },
  mode: 'development'
};
