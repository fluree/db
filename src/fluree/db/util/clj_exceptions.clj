(ns fluree.db.util.clj-exceptions
  (:require [fluree.db.util.exceptions :refer [find-clause]]))

(set! *warn-on-reflection* true)

(defmacro try* [& body]
  (let [try-body       (remove #(and
                                  (list? %)
                                  (or
                                    (= 'catch* (first %))
                                    (= 'finally (first %))))
                               body)
        [catch err & catch-body] (find-clause 'catch* body)
        finally-clause (find-clause 'finally body)
        finally-form   (when finally-clause (list finally-clause))]
    (assert (symbol? err))
    `(try
       ~@try-body
       (catch Exception ~err ~@catch-body)
       ~@finally-form)))
