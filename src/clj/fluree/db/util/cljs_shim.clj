(ns fluree.db.util.cljs-shim
  (:require [clojure.java.io :as io]))

(set! *warn-on-reflection* true)

(defmacro inline-resource
  "Macro allowing ClojureScript to inline a SMALL bundle of resource file(s) (< 1mb)
  at compile time.  If inline content grows, need to consider publishing to
  and downloading from a cdn."

  [resource-path]
  (slurp (io/resource resource-path)))

