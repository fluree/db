(ns fluree.db.reasoner.util
  (:require [fluree.db.util.core :as util]
            [fluree.json-ld :as json-ld]))

(defn parse-rules-graph
  [rules-graph]
  (-> rules-graph
      json-ld/expand
      util/sequential))
