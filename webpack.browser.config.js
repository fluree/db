const path = require('path');

module.exports = {
  target: "web",
  entry: "./out/browser/index.js",
  output: {
    filename: "main.js",
    path: path.resolve(__dirname, "./out/browser/"),
  },
  resolve: {
    fallback: {
      crypto: false
    }
  }
};
