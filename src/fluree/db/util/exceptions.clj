(ns fluree.db.util.exceptions)

(set! *warn-on-reflection* true)

(defn find-clause [clause body]
  (some #(when (and
                 (list? %)
                 (= clause (first %)))
           %)
        body))