
// This is a modified web worker loading script for Closure Compiler to be used with `lein cljsbuild once worker-dev`
// It allows the additional JS files to be imported using importScripts instead of the default document.write('<script... calls
// that would only work if there was a web page loaded, which doesn't exist in a web worker.

var CLOSURE_UNCOMPILED_DEFINES = {};
var CLOSURE_NO_DEPS = true;
var CLOSURE_BASE_PATH = "build/flureeworker-dev-out/goog/"

importScripts('closure-webworker.js');
importScripts('build/flureeworker-dev-out/goog/base.js');
importScripts('build/flureeworker-dev-out/goog/deps.js');
importScripts('build/flureeworker-dev-out/cljs_deps.js');

goog.require("process.env");
goog.require("flureeworker");


// if(typeof goog == "undefined") document.write('<script src="js/goog/base.js"></script>');
// document.write('<script src="js/goog/deps.js"></script>');
// document.write('<script src="js/cljs_deps.js"></script>');
// document.write('<script>if (typeof goog == "undefined") console.warn("ClojureScript could not load :main, did you forget to specify :asset-path?");</script>');
// document.write('<script>goog.require("process.env");</script>');
// document.write('<script>goog.require("flureeworker");</script>');
