(ns fluree.db.util.cljs-shim)

(defmacro inline-resource
  "Macro allowing ClojureScript to inline a SMALL bundle of resource file(s) (< 1mb)
  at compile time.  If inline content grows, need to consider publishing to
  and downloading from a cdn."

  [resource-path]
  (slurp (clojure.java.io/resource resource-path)))

