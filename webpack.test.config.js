const path = require('path');

module.exports = {
  target: "web",
  entry: "./out/test/index.js",
  output: {
    filename: "main.js",
    path: path.resolve(__dirname, "./out/test/"),
  },
  resolve: {
    fallback: {
      crypto: false
    }
  }
};
