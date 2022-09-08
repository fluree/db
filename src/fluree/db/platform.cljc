(ns fluree.db.platform
  "This is intended to help differentiate between the browser and node platforms while
  developing in cljs. `goog-define` defines a compile-time constant that allows advanced
  tree-shaking during the cljs compilation process, which allows us to include
  node-specific libraries and functionality as long as it is downstream of a
  compile-time constant that will remove it before compilation is attempted. This
  compilation behavior is controlled via the shadow-cljs.edn :closure-defines
  configuration, and is dynamically overridden at compile time by shadow-cljs according
  to the intended build platform.")

#?(:cljs (goog-define BROWSER true)
   :clj (def BROWSER false))
