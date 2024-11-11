(ns fluree.db.query.subject-crawl.reparse
  (:require [fluree.db.util.log :as log :include-macros true]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            #?(:clj [fluree.db.query.select]
               :cljs [fluree.db.query.select :refer [SubgraphSelector]])
            [fluree.db.query.where :as where])
  #?(:clj (:import [fluree.db.query.select SubgraphSelector])))

#?(:clj (set! *warn-on-reflection* true))

(defn fill-fn-params
  "A filtering function in the :o space may utilize other supplied variables
  from {:vars {}} in the original query. This places those vars into the proper
  calling order of the function parameters that was generated during parsing."
  [params obj-val obj-var supplied-vars]
  (reduce (fn [acc param]
            (if (= param obj-var)
              (conj acc obj-val)
              (if (contains? supplied-vars param)
                (conj acc (get supplied-vars param))
                (throw (ex-info (str "Variable used in filter function not included in 'vars' map: " param)
                                {:status 400 :error :db/invalid-query})))))
          [] params))

(defn mergeable-where-clause?
  "Returns true if a `where` clause is eligible for merging
  with other clauses to create a filter for the simple-subject-crawl
  strategy"
  [where-clause]
  (let [type (where/pattern-type where-clause)]
    (case type
      :class (let [pattern (val where-clause)]
               (some ::where/val pattern))
      :tuple (some ::where/val where-clause)
      false)))

(defn clause-subject-var
  [where-clause]
  (-> where-clause
      first
      ::where/var))

(defn merge-wheres-to-filter
  "Merges all subsequent where clauses (rest where) for simple-subject-crawl
  into a map containing predicate filters.

  A simple-subject crawl will get a list of subjects and then pull all
  flakes for that subject in one lookup, instead of traditionally with multiple
  where clauses doing individual lookups per clauses.

  Instead of a lookup for every where clause, we therefore filter the subject
  flakes for the criteria of each where clause. This generates a single data
  structure that allows that filtering to happen. It looks like:

  {:required-p #{1001 1002 ...} - each where statement has a predicate that *must* be present
   1001 [(> % 42) (< % 10)] - for predicate 1001, list of filtering fns for the .-o value of each flake
   1002 [(= % hi)] }

   Note that for multi-cardinality predicates, the prediate filters must pass for just one flake
  "
  [db first-s rest-where supplied-vars]
  (loop [[where-smt & r] rest-where
         required-p #{} ;; set of 'p' values that are going to be required for a subject to have
         filter-map {}] ;; key 'p' value, val is list of filtering fns
    (if where-smt
      (let [[s p o] where-smt
            p*      (iri/encode-iri db (::where/iri p))
            type (where/pattern-type where-smt)]
        (when (and (= :tuple type)
                   (= first-s (clause-subject-var where-smt)))
          (let [{::where/keys [val var]} o
                f (cond
                    val
                    (fn [flake _] (= val (flake/o flake)))

                    ;;TODO: filters are not yet supported
                    #_#_filter
                    (let [{:keys [params variable function]} filter]
                      (if (= 1 (count params))
                        (fn [flake _] (function (flake/o flake)))
                        (fn [flake vars]
                          (let [params (fill-fn-params params (flake/o flake) variable vars)]
                            (log/debug (str "Calling query-filter fn: " ("fn-str" filter)
                                            "with params: " params "."))
                            (apply function params)))))
                    ;;TODO: vars are not yet supported
                    #_#_(and var (get supplied-vars var))
                    (fn [flake vars]
                      (= (flake/o flake) (get vars var))))]
            (recur r
                   (conj required-p p*)
                   (if f
                     (update filter-map p* util/conjv f)
                     filter-map)))))
      (assoc filter-map :required-p required-p))))

(defn reparse-component
  [component]
  (let [{::where/keys [var val]} component]
    (cond
      var {:variable var}
      val {:value val})))

(defn reparse-subject-component
  [subj]
  (if-let [s-iri (::where/iri subj)]
    {:value s-iri}
    (reparse-component subj)))

(defn reparse-predicate-component
  [db pred]
  (if-let [p-iri (::where/iri pred)]
    {:value (iri/encode-iri db p-iri)}
    (reparse-component pred)))

(defn re-parse-pattern
  "Re-parses a pattern into the format recognized
  by downstream simple-subject-crawl code"
  [db pattern]
  (let [type (where/pattern-type pattern)
        [s p o] (if (= :tuple type)
                  pattern
                  (let [[_type-kw tuple] pattern]
                    tuple))]
    {:type type
     :s (reparse-subject-component s)
     :p (reparse-predicate-component db p)
     :o (-> o
            reparse-component
            (assoc :datatype (some->> o
                                      where/get-datatype-iri
                                      (iri/encode-iri db))))}))

(defn simple-subject-merge-where
  "Revises where clause for simple-subject-crawl query to optimize processing.
  If where does not end up meeting simple-subject-crawl criteria, returns nil
  so other strategies can be tried."
  [db {:keys [where vars] :as parsed-query}]
  (let [[first-pattern & rest-patterns] where
        reparsed-first-clause (re-parse-pattern db first-pattern)]
    (when-let [first-s (and (mergeable-where-clause? first-pattern)
                            (clause-subject-var first-pattern))]
      (if (empty? rest-patterns)
        (assoc parsed-query
               :where [reparsed-first-clause]
               :strategy :simple-subject-crawl)
        (when-let [subj-filter-map (merge-wheres-to-filter db first-s rest-patterns vars)]
          (assoc parsed-query :where [reparsed-first-clause
                                      {:s-filter subj-filter-map}]
                              :strategy :simple-subject-crawl))))))

(defn has-equivalent-properties?
  [db pattern]
  (if-let [p-val (get-in pattern [1 ::where/val])]
    (some? (where/get-child-properties db p-val))
    false))

(defn simple-subject-crawl?
  "Simple subject crawl is where the same variable is used in the leading
  position of each where statement."
  [{:keys [where select vars] :as _parsed-query} db]
  (and (instance? SubgraphSelector select)
       ;;TODO, filtering not supported yet
       (->> where
            (filter (fn [pattern]
                      (= :filter (where/pattern-type pattern))))
            empty?)
       ;;TODO: vars support not complete
       (empty? vars)
       (when-let [{select-var :subj} select]
         (every? (fn [pattern]
                   (and (mergeable-where-clause? pattern)
                        (let [pred (second pattern)]
                          (and (= select-var (clause-subject-var pattern))
                               (not (has-equivalent-properties? db pattern))
                               (not (::where/fullText pred))))))
                 where))))

(defn re-parse-as-simple-subj-crawl
  "Returns true if query contains a single subject crawl.
  e.g.
  {:select {?subjects ['*']}
   :where [...]}"
  [{:keys [order-by group-by] :as parsed-query} db]
  (when (and (not group-by)
             (not order-by)
             (simple-subject-crawl? parsed-query db))
    ;; following will return nil if parts of where clause exclude it from being a simple-subject-crawl
    (let [merged (simple-subject-merge-where db parsed-query)]
      merged)))
