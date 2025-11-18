(ns fluree.db.query.optimize
  (:require [fluree.db.query.exec.where :as where]
            [fluree.db.util.async :refer [<? go-try]]))

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

(defn property-join-candidate?
  "Check if a pattern is a regular triple that can be grouped for property joins.
  A triple is groupable if it has:
  - A variable subject
  - A specified (non-variable) predicate
  - Exactly 3 components
  - No filter functions on any component (language/datatype matchers, etc.)"
  [pattern]
  (and (variable-triple-subject? pattern)
       (specified-predicate? pattern)
       (not-any? has-filter-function? pattern)))

(defn extract-subject-variable
  "Extract the subject variable from a triple pattern."
  [triple]
  (-> triple triple-subject where/get-variable))

(defn group-subject-triples
  "Group all triples by their subject variable throughout the pattern sequence.
  Groupable triples are grouped by subject variable, and all grouped triples
  appear before non-groupable patterns in the result.

  Returns a sequence of groups, where each group is:
  - A vector of triples (for groupable triples with the same subject)
  - A compound pattern (for higher-order patterns like :union, :optional, etc.)
  - A single pattern (for non-groupable triples)"
  [patterns]
  (let [grouped       (group-by (fn [pattern]
                                  (if (property-join-candidate? pattern)
                                    (extract-subject-variable pattern)
                                    ::ungrouped))
                                patterns)
        ungrouped     (get grouped ::ungrouped [])
        triple-groups (-> grouped
                          (dissoc ::ungrouped)
                          vals)]
    (-> triple-groups
        (concat ungrouped)
        vec)))

(defn triple-group?
  "Determine if a group is a non-empty sequence consisting only of triple
  patterns."
  [group]
  (and (seq group)
       (every? where/triple-pattern? group)))

(declare group-patterns)

(defn build-property-joins
  "Transform a pattern group into a sequence of patterns with property joins.

  Takes a single group which can be:
  - A vector of triple patterns sharing the same subject variable
    -> creates a property join pattern to enable more efficient query execution
       by retrieving all properties for a subject in parallel
  - A compound pattern (union, optional, graph, etc.)
    -> recursively processes inner patterns to build property joins within them
  - Any other pattern
    -> returns as-is

  Returns a sequence (for mapcat) containing the transformed pattern(s)."
  [group]
  (cond
    (triple-group? group)
    [(where/->pattern :property-join group)]

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
       group-subject-triples
       (mapcat build-property-joins)
       vec))

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

    Returns a channel that will contain a query plan map

    Parameters:
      db - The database (FlakeDB, AsyncDB, etc.)
      parsed-query - The parsed query from fql/parse-query

    Returns:
      Channel containing query plan map"))

(defn optimize
  [db parsed-query]
  (go-try
    (let [parsed-query* (<? (-reorder db parsed-query))]
      (update parsed-query* :where group-patterns))))
