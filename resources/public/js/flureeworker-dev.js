
// This script modifies the google closure compiler file import process to
// work with the Fluree webworker. When we compile the web worker using :advanced
// compile option it combines all of Flureedb into a single JS file, but if
// we want to use it uncompiled for debugging, it leaves the files separate and attempts
// to use HTML <script> tags for importing... which obviously won't work in a web worker

// place directory (relative to this file) that the compiled code exists in
var FLUREE_DIR = "build/flureeworker-dev-out/";

var CLOSURE_UNCOMPILED_DEFINES = {};
var CLOSURE_NO_DEPS = true;
var CLOSURE_BASE_PATH = FLUREE_DIR + "goog/"

/**
 * Imports a script using the Web Worker importScript API.
 *
 * @param {string} src The script source.
 * @return {boolean} True if the script was imported, false otherwise.
 */
var CLOSURE_IMPORT_SCRIPT = (function(global) {
    return function(src, opt_sourceText) {
      if (opt_sourceText) {
        eval(opt_sourceText)
      } else {
        global['importScripts'](src);
      }
      return true;
    };
  })(this);

importScripts(CLOSURE_BASE_PATH + 'base.js');
importScripts(CLOSURE_BASE_PATH + 'deps.js');
importScripts(FLUREE_DIR + 'cljs_deps.js');

goog.require("process.env");
goog.require("flureeworker");


// if(typeof goog == "undefined") document.write('<script src="js/goog/base.js"></script>');
// document.write('<script src="js/goog/deps.js"></script>');
// document.write('<script src="js/cljs_deps.js"></script>');
// document.write('<script>if (typeof goog == "undefined") console.warn("ClojureScript could not load :main, did you forget to specify :asset-path?");</script>');
// document.write('<script>goog.require("process.env");</script>');
// document.write('<script>goog.require("flureeworker");</script>');