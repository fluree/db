(ns fluree.db.query.optimize
  (:require [fluree.db.query.exec.where :as where]))

(def triple-pattern-types
  #{:tuple :class})

(defn triple-pattern?
  [x]
  (contains? triple-pattern-types (where/pattern-type x)))

(defn try-coerce-triple
  "Returns the triple data if x is a triple pattern (:class, :tuple),
  otherwise returns nil."
  [x]
  (when (triple-pattern? x)
    (where/pattern-data x)))

(defn coerce-triple
  [x]
  (or (try-coerce-triple x)
      (throw (ex-info "Optimization failed on non triple pattern type"
                      {:status   500
                       :error    :db/optimization-failure
                       ::pattern x}))))

(defn compare-component
  [cmp-a cmp-b]
  (if (where/matched-value? cmp-a)
    (if (where/matched-value? cmp-b)
      0
      -1)
    (if (where/matched-value? cmp-b)
      1
      0)))

(defn compare-triples
  [a b]
  (let [a' (coerce-triple a)
        b' (coerce-triple b)]
    (reduce (fn [_ nxt]
              (if (zero? nxt)
                nxt
                (reduced nxt)))
            (map compare-component a' b'))))

(defn sort-triples
  [triple-coll]
  (sort compare-triples triple-coll))
