(ns fluree.db.util.exceptions)

(defn find-clause [clause body]
  (some #(when (and
                 (list? %)
                 (= clause (first %)))
           %)
        body))