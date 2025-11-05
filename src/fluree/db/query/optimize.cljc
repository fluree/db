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

(defn variable-triple-subject?
  "Check if a pattern is a triple with a variable subject."
  [pattern]
  (when-let [subj (triple-subject pattern)]
    (and (map? subj)
         (where/get-variable subj))))

(defn triple-predicate
  "Extract the predicate from a triple pattern."
  [pattern]
  (some->> pattern try-coerce-triple second))

(defn specified-iri?
  "Check if a predicate is specified (not a variable)."
  [pred-match]
  (and (map? pred-match)
       (not (where/get-variable pred-match))))

(defn specified-predicate?
  "Check if a pattern has a specified (non-variable) predicate."
  [pattern]
  (->> pattern triple-predicate specified-iri?))

(defn has-filter-function?
  "Check if a pattern component has a filter function (e.g., language matcher,
  datatype matcher)."
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
  (and (coll? pattern)
       (= 3 (count pattern))
       (variable-triple-subject? pattern)
       (specified-predicate? pattern)
       (not-any? has-filter-function? pattern)))

(defn extract-subject-variable
  "Extract the subject variable from a triple pattern."
  [triple]
  (-> triple triple-subject where/get-variable))

(defn matching-subject-variables?
  "Check if all triples have the same subject variable."
  [triples]
  (->> triples (map extract-subject-variable) (apply =)))

(defn property-join-candidate?
  "Check if a group of triples should become a property join. Must have at least 2
  triples with the same variable subject and all different predicates (same
  predicate with different objects should not be grouped)."
  [triples]
  (and (>= (count triples) 2)
       (every? groupable-triple? triples)
       (matching-subject-variables? triples)))

(defn group-subject-triples
  "Build a map of subject variable -> all triples with that subject. Only includes
  groupable triples."
  [patterns]
  (reduce (fn [acc pattern]
            (if (groupable-triple? pattern)
              (let [subj (extract-subject-variable pattern)]
                (update acc subj (fnil conj []) pattern))
              acc))
          {}
          patterns))

(defn group-all-subject-triples
  "Group all triples by their subject variable throughout the pattern sequence.
  Returns a sequence where each group is placed at the position of the first triple
  with that subject. Subsequent triples with the same subject are omitted from their
  original positions. Non-groupable patterns remain in their original positions.

  Returns a sequence of groups, where each group is:
  - A vector of triples (for groupable triples with the same subject)
  - A compound pattern (for higher-order patterns like :union, :optional, etc.)
  - A single pattern (for non-groupable triples)"
  [patterns]
  (let [subject->triples (group-subject-triples patterns)]
    (loop [[pattern & r] patterns
           result        []
           seen-subjects #{}]
      (if pattern
        (if (groupable-triple? pattern)
          (let [subj (extract-subject-variable pattern)]
            ;; Only output this subject's group at its first occurrence
            (if (contains? seen-subjects subj)
              (recur r result seen-subjects)
              (recur r
                     (conj result (get subject->triples subj))
                     (conj seen-subjects subj))))
          ;; Non-groupable pattern - keep as-is
          (recur r (conj result pattern) seen-subjects))
        result))))

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
    (where/compound-pattern? group)
    (let [typ  (where/pattern-type group)
          data (where/pattern-data group)]
      (cond
        (= typ :union)
        [(where/->pattern :union (mapv group-patterns data))]

        (contains? #{:optional :exists :not-exists :minus} typ)
        [(where/->pattern typ (group-patterns data))]

        (= typ :graph)
        (let [[graph-alias where-patterns] data]
          (if (where/virtual-graph? graph-alias)
            ;; Virtual graphs (##...) are handled specially - don't group their inner patterns
            ;; Return the graph pattern with ungrouped inner patterns
            [(where/->pattern :graph [graph-alias where-patterns])]
            ;; Regular named graphs can have their patterns grouped
            [(where/->pattern :graph [graph-alias (group-patterns where-patterns)])]))

        :else
        [group]))

    (and (seq group)
         (every? where/triple-pattern? group))
    (create-property-join-or-triples group)

    :else
    [group]))

(defn group-patterns
  "Recursively group all triples with the same subject into property joins.
  Triples are grouped by subject throughout the entire pattern sequence, not
  just consecutive ones. Respects higher-order patterns (union, optional,
  filter, etc.) by recursively processing their contents but not grouping across
  them."
  [patterns]
  (->> patterns
       group-all-subject-triples
       (mapcat process-pattern-group)
       vec))
