(ns fluree.db.util.parse
  (:require [fluree.db.util :as util]))

(defn normalize-values
  "Normalize the structure of the values clause to
  [[vars...] [[val1..] [val2...] ...]], handling nil properly."
  [values]
  (when-let [[vars vals] values]
    [(into [] (when vars (util/sequential vars)))
     (mapv util/sequential vals)]))
