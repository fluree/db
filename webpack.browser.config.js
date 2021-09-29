const path = require('path');

module.exports = {
  target: "web",
  entry: "./js/browser/webpack.js",
  output: {
    filename: "index.js",
    path: path.resolve(__dirname, "./js/browser/"),
  },
  resolve: {
    fallback: {
      crypto: false
    }
  }
};
