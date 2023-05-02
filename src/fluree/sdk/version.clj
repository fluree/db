(ns fluree.sdk.version
  (:require [clojure.java.shell :refer [sh]]
            [clojure.edn :as edn]))


(defmacro version
  "Compiles in the current version in CLJS code. Returns a map like:
  {:version \"1.2.3\"}"
  []
  (-> (sh "clojure" "-T:build" "print-version")
      :out
      edn/read-string))
