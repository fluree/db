(ns fluree.db.query.optimize
  (:require [fluree.db.query.exec.where :as where]))

(defn try-coerce-triple
  "Returns the triple data if x is a triple pattern (:class, :tuple),
  otherwise returns nil."
  [x]
  (when (where/triple-pattern? x)
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

;; Post-parse triple grouping for property joins

(defn triple-subject
  "Extract the subject from a triple pattern."
  [pattern]
  (some->> pattern try-coerce-triple first))

(defn triple-variable-subject?
  "Check if a pattern is a triple with a variable subject."
  [pattern]
  (when-let [subj (triple-subject pattern)]
    (and (map? subj)
         (where/get-variable subj))))

(defn triple-predicate
  "Extract the predicate from a triple pattern."
  [pattern]
  (some->> pattern try-coerce-triple second))

(defn specified-predicate?
  "Check if a predicate is specified (not a variable)."
  [pred-match]
  (and (map? pred-match)
       (not (where/get-variable pred-match))))

(defn has-filter-function?
  "Check if a pattern component has a filter function (e.g., language matcher, datatype matcher)."
  [component]
  (and (map? component)
       (contains? component ::where/fn)))

(defn groupable-triple?
  "Check if a pattern is a regular triple that can be grouped for property joins.
  A triple is groupable if it has:
  - A variable subject
  - A specified (non-variable) predicate
  - Exactly 3 components
  - No filter functions on any component (language/datatype matchers, etc.)"
  [pattern]
  (and (triple-variable-subject? pattern)
       (specified-predicate? (triple-predicate pattern))
       (not-any? has-filter-function? pattern)))

(defn property-join-candidate?
  "Check if a group of triples should become a property join.
  Must have at least 2 triples with the same variable subject and
  all different predicates (same predicate with different objects should not be grouped)."
  [triples]
  (and (>= (count triples) 2)
       (every? groupable-triple? triples)
       (let [subjects   (map triple-subject triples)
             predicates (map triple-predicate triples)]
         (and (apply = subjects)
              (= (count predicates) (count (distinct predicates)))))))

(defn group-consecutive-triples
  "Group consecutive triples by their subject variable.
  Returns a sequence of groups, where each group is:
  - A vector of triples (for groupable triples with the same subject)
  - A map-entry (for higher-order patterns like :union, :optional, etc.)
  - A single triple wrapped in a marker (for non-groupable triples)"
  [patterns]
  (reduce (fn [groups pattern]
            (if (groupable-triple? pattern)
              (let [subj (triple-subject pattern)
                    last-group (peek groups)]
                ;; If the last group is a triple group with the same subject, add to it
                (if (and (vector? last-group)
                         (not (map-entry? last-group))
                         (groupable-triple? (first last-group))
                         (= subj (triple-subject (first last-group))))
                  (conj (pop groups) (conj last-group pattern))
                  ;; Otherwise start a new group
                  (conj groups [pattern])))
              ;; Non-groupable pattern (map-entry or non-groupable triple)
              ;; Add as its own "group" without wrapping
              (conj groups pattern)))
          []
          patterns))

(defn create-property-join-or-triples
  "Convert a group of triples into a property join if eligible,
  otherwise return the triples sorted.

  Returns a sequence of patterns that will be concatenated by mapcat."
  [triple-group]
  (if (property-join-candidate? triple-group)
    [(where/->pattern :property-join triple-group)]
    (sort compare-triples triple-group)))

(declare group-patterns)

(defn process-pattern-group
  "Process a single pattern group, recursively grouping higher-order patterns
  or creating property joins from triple groups.

  Returns a sequence of patterns that will be concatenated by mapcat."
  [group]
  (cond
    ;; Higher-order pattern (map-entry)
    (where/compound-pattern? group)
    (let [typ (where/pattern-type group)
          data (where/pattern-data group)]
      (cond
        ;; Recursively process union branches
        (= typ :union)
        [(where/->pattern :union (mapv group-patterns data))]

        ;; Recursively process optional, exists, not-exists, minus
        (contains? #{:optional :exists :not-exists :minus} typ)
        [(where/->pattern typ (group-patterns data))]

        ;; Recursively process graph patterns
        ;; BUT: don't group patterns inside virtual graphs (e.g., vector indexes)
        (= typ :graph)
        (let [[graph-alias where-patterns] data]
          (if (where/virtual-graph? graph-alias)
            ;; Virtual graphs (##...) are handled specially - don't group their inner patterns
            ;; Return the graph pattern with ungrouped inner patterns
            [(where/->pattern :graph [graph-alias where-patterns])]
            ;; Regular named graphs can have their patterns grouped
            [(where/->pattern :graph [graph-alias (group-patterns where-patterns)])]))

        ;; Other patterns pass through unchanged
        :else
        [group]))

    ;; Group of triples - check if first element is a triple
    (and (vector? group) (not-empty group) (vector? (first group)))
    (create-property-join-or-triples group)

    ;; Single non-groupable triple (vector but not a group)
    (vector? group)
    [group]

    :else
    [group]))

(defn group-patterns
  "Recursively group triples with the same subject into property joins.
  Respects higher-order patterns (union, optional, filter, etc.) by
  recursively processing their contents but not grouping across them."
  [patterns]
  (->> patterns
       group-consecutive-triples
       (mapcat process-pattern-group)
       vec))
