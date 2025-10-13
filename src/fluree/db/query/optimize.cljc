(ns fluree.db.query.optimize
  (:require [fluree.db.json-ld.iri :as iri]
            [fluree.db.query.exec.where :as where]))

;; Selectivity score constants
;; Lower score = more selective = execute first
(def ^:const highly-selective 0)
(def ^:const moderately-selective 10)
(def ^:const default-selectivity 1000)
(def ^:const full-scan ##Inf)

(defn compare-component
  [cmp-a cmp-b]
  (if (where/matched-value? cmp-a)
    (if (where/matched-value? cmp-b)
      0
      -1)
    (if (where/matched-value? cmp-b)
      1
      0)))

(def triple-pattern-types
  #{:tuple :class})

(defn triple-pattern?
  [x]
  (contains? triple-pattern-types (where/pattern-type x)))

(defn coerce-triple
  [x]
  (if (triple-pattern? x)
    (where/pattern-data x)
    (throw (ex-info "Optimization failed on non triple pattern type"
                    {:status   500
                     :error    :db/optimization-failure
                     ::pattern x}))))

(defn compare-triples
  [a b]
  (let [a' (coerce-triple a)
        b' (coerce-triple b)]
    (reduce (fn [_ nxt]
              (if (zero? nxt)
                nxt
                (reduced nxt)))
            (map compare-component a' b'))))

(defn optimizable-pattern?
  "Returns true for patterns we know how to optimize.
   Any pattern not recognized here is treated as an optimization boundary."
  [pattern]
  (let [ptype (where/pattern-type pattern)]
    (#{:tuple :class :id} ptype)))

(defn get-iri
  "Extract IRI from a matched component"
  [component]
  (where/get-iri component))

(defn encode-iri-to-sid
  "Convert IRI to SID using database namespace table"
  [db iri]
  (when iri
    (iri/encode-iri db iri)))

(defn get-property-count
  "Get count for a property from stats"
  [stats sid]
  (get-in stats [:properties sid :count]))

(defn get-class-count
  "Get count for a class from stats"
  [stats sid]
  (get-in stats [:classes sid :count]))

(defn matched?
  "Check if a component is matched (has a bound value, IRI, or SID)"
  [component]
  (where/matched? component))

(defn unmatched?
  "Check if a component is unmatched (variable)"
  [component]
  (not (matched? component)))

(defn all-matched?
  "Check if all components are matched"
  [[s p o]]
  (and (matched? s) (matched? p) (matched? o)))

(defn calculate-selectivity
  "Calculate selectivity score for a pattern.
   Lower score = more selective = execute first."
  [db stats pattern]
  (let [pattern-type (where/pattern-type pattern)
        pattern-data (where/pattern-data pattern)]

    (cond
      (or (nil? stats) (empty? stats))
      nil

      (= :id pattern-type)
      (if (matched? pattern-data)
        highly-selective
        moderately-selective)

      :else
      (let [[s p o] pattern-data]
        (cond
          ;; Class patterns use class count from stats
          (= :class pattern-type)
          (let [class-iri (get-iri o)
                class-sid (encode-iri-to-sid db class-iri)]
            (or (get-class-count stats class-sid) default-selectivity))

          ;; Specific s-p-o triple lookup
          (all-matched? [s p o])
          highly-selective

          ;; s-p-? lookup uses property count
          (and (matched? s) (matched? p) (unmatched? o))
          (let [pred-iri (get-iri p)
                pred-sid (encode-iri-to-sid db pred-iri)]
            (or (get-property-count stats pred-sid) moderately-selective))

          ;; ?-p-? property scan uses property count
          (and (unmatched? s) (matched? p) (unmatched? o))
          (let [pred-iri (get-iri p)
                pred-sid (encode-iri-to-sid db pred-iri)]
            (or (get-property-count stats pred-sid) default-selectivity))

          ;; ?-p-o reverse lookup (find subjects with specific value)
          (and (unmatched? s) (matched? p) (matched? o))
          highly-selective

          (and (unmatched? s) (unmatched? p) (unmatched? o))
          full-scan

          :else
          default-selectivity)))))

(defn split-by-optimization-boundaries
  "Split where clause into segments separated by optimization boundaries.
   Optimizable patterns (:tuple, :class, :id) are grouped together.
   All other patterns (boundaries) separate the segments."
  [where-clause]
  (reduce
   (fn [segments pattern]
     (if (optimizable-pattern? pattern)
       (if (and (seq segments)
                (= :optimizable (:type (peek segments))))
         (update segments (dec (count segments))
                 update :data conj pattern)
         (conj segments {:type :optimizable :data [pattern]}))
       ;; Boundary - add as separate segment
       (conj segments {:type :boundary :data pattern})))
   []
   where-clause))

(defn optimize-segment
  "Optimize a single segment by sorting patterns by selectivity"
  [db stats patterns]
  (if (or (nil? stats) (empty? stats))
    ;; No stats, return as-is
    patterns
    ;; Sort by selectivity (lower = more selective = execute first)
    (let [with-scores (mapv (fn [pattern]
                              {:pattern pattern
                               :score (or (calculate-selectivity db stats pattern)
                                          default-selectivity)})
                            patterns)
          sorted      (sort-by :score with-scores)]
      (mapv :pattern sorted))))

(defn optimize-patterns
  "Reorder patterns for optimal execution based on statistics.
   Splits on optimization boundaries and optimizes each segment independently."
  [db where-clause]
  (let [stats (:stats db)]
    (if (or (nil? stats) (empty? stats))
      ;; No stats available, return as-is
      where-clause
      ;; Split into segments and optimize each
      (let [segments (split-by-optimization-boundaries where-clause)]
        (into []
              (mapcat (fn [segment]
                        (if (= :optimizable (:type segment))
                          (optimize-segment db stats (:data segment))
                          [(:data segment)])))
              segments)))))

(defprotocol Optimizable
  "Protocol for query optimization based on database statistics."

  (-reorder [db parsed-query]
    "Reorder query patterns based on database statistics.

    Returns a channel that will contain the optimized query with patterns
    reordered for optimal execution. If the database has no statistics
    available, returns the query unchanged.

    Parameters:
      db - The database (FlakeDB, AsyncDB, etc.)
      parsed-query - The parsed query from fql/parse-query

    Returns:
      Channel containing optimized query")

  (-explain [db parsed-query]
    "Generate an execution plan for the query showing optimization details.

    Returns a channel that will contain a query plan map with:
    - :query - The optimized query
    - :plan - Execution plan details including:
      - :optimization - :reordered, :unchanged, or :none
      - :statistics - Available statistics info
      - :original - Original pattern order with selectivity
      - :optimized - Optimized pattern order with selectivity
      - :segments - Pattern segments with boundaries
      - :changed? - Boolean indicating if patterns were reordered

    Parameters:
      db - The database (FlakeDB, AsyncDB, etc.)
      parsed-query - The parsed query from fql/parse-query

    Returns:
      Channel containing query plan map"))
