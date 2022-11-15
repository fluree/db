(ns fluree.resource
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io])
  (:import (java.io PushbackReader)))

(defn load-edn-resource
  [resource-path]
  (with-open [r (-> resource-path io/resource io/reader PushbackReader.)]
    (edn/read r)))

(defmacro inline-edn-resource
  [resource-path]
  (load-edn-resource resource-path))
