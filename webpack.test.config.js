const path = require('path');

module.exports = {
  target: "web",
  entry: "./js/test/webpack.js",
  output: {
    filename: "index.js",
    path: path.resolve(__dirname, "./js/test/"),
  },
  resolve: {
    fallback: {
      crypto: false
    }
  }
};
