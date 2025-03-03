(ns fluree.db.reasoner.util
  (:require [fluree.json-ld :as json-ld]
            [fluree.db.util.core :as util]))

(defn parse-rules-graph
  [rules-graph]
  (-> rules-graph
      json-ld/expand
      util/sequential))
