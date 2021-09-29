const path = require('path');

module.exports = {
  target: "webworker",
  entry: "./js/webworker/webpack.js",
  output: {
    filename: "index.js",
    path: path.resolve(__dirname, "./js/webworker/"),
  },
  resolve: {
    fallback: {
      crypto: false
    }
  }
};
