const path = require('path');

module.exports = {
  target: "webworker",
  entry: "./out/webworker/index.js",
  output: {
    filename: "main.js",
    path: path.resolve(__dirname, "./out/webworker/"),
  },
  resolve: {
    fallback: {
      crypto: false
    }
  }
};
