module.exports = function (config) {
  config.set({
    browsers: ['ChromeHeadless'],
    // The directory where the output file lives
    basePath: 'out/browser-test/',
    // The file itself
    files: ['browser-tests.js'],
    frameworks: ['cljs-test'],
    plugins: ['karma-cljs-test', 'karma-chrome-launcher'],
    colors: true,
    logLevel: config.LOG_INFO,
    client: {
      args: ["shadow.test.karma.init"],
      singleRun: true
    }
  })
};
