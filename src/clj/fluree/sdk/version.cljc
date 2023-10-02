(ns fluree.sdk.version
  #?(:clj  (:require [clojure.java.shell :refer [sh]]
                     [clojure.edn :as edn])
     :cljs (:require-macros [fluree.sdk.version :refer [version]])))


(defmacro version
  "Compiles in the current version in CLJS code. Returns a map like:
  {:version \"1.2.3\"}"
  []
  (-> (sh "clojure" "-T:build" "print-version")
      :out
      edn/read-string))
