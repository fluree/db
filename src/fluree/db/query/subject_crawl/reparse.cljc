(ns fluree.db.query.subject-crawl.reparse
  (:require [fluree.db.util.log :as log]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util :refer [try* catch*]]))

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
  [first-s rest-where supplied-vars]
  (log/debug "merge-wheres-to-filter first-s:" first-s "\nrest-where:" rest-where
             "\nsupplied-vars:" supplied-vars)
  (loop [[{:keys [type s p o] :as where-smt} & r] rest-where
         required-p #{} ;; set of 'p' values that are going to be required for a subject to have
         filter-map {}] ;; key 'p' value, val is list of filtering fns
    (if where-smt
      (when (and (= :tuple type)
                 (= first-s (:variable s)))
        (let [{:keys [value filter variable]} o
              f (cond
                  value
                  (fn [flake _] (= value (flake/o flake)))

                  filter
                  (let [{:keys [params variable function]} filter]
                    (if (= 1 (count params))
                      (fn [flake _] (function (flake/o flake)))
                      (fn [flake vars]
                        (let [params (fill-fn-params params (flake/o flake) variable vars)]
                          (log/debug (str "Calling query-filter fn: " (:fn-str filter)
                                          "with params: " params "."))
                          (apply function params)))))

                  (and variable (supplied-vars variable))
                  (do
                    (log/debug "Returning var resolution filter fn")
                    (fn [flake vars]
                      (log/debug "Running var resolution filter fn")
                      (= (flake/o flake) (get vars variable)))))]
          (recur r
                 (conj required-p p)
                 (if f
                   (update filter-map p util/conjv f)
                   filter-map))))
      (assoc filter-map :required-p required-p))))


(defn simple-subject-merge-where
  "Revises where clause for simple-subject-crawl query to optimize processing.
  If where does not end up meeting simple-subject-crawl criteria, returns nil
  so other strategies can be tried."
  [{:keys [where supplied-vars] :as parsed-query}]
  (let [first-where (first where)
        rest-where  (rest where)
        first-type  (:type first-where)
        first-s     (when (and (#{:rdf/type :_id :tuple} first-type)
                               (-> first-where :s :variable))
                      (-> first-where :s :variable))]
    (when first-s
      (log/debug "simple-subject-merge-where first-s:" first-s)
      (if (empty? rest-where)
        (assoc parsed-query :strategy :simple-subject-crawl)
        (if-let [subj-filter-map (merge-wheres-to-filter first-s rest-where supplied-vars)]
          (assoc parsed-query :where [first-where
                                      {:s-filter subj-filter-map}]
                              :strategy :simple-subject-crawl))))))

(defn subject-crawl?
  "Returns true if, when given parsed query, the select statement is a
  subject crawl - meaning there is nothing else in the :select except a
  graph crawl on a list of subjects"
  [{:keys [select] :as _parsed-query}]
  (and (:expandMaps? select)
       (not (:inVector? select))))

(defn simple-subject-crawl?
  "Simple subject crawl is where the same variable is used in the leading
  position of each where statement."
  [{:keys [where select] :as _parsed-query}]
  (let [select-var (-> select :select first :variable)]
    (when select-var ;; for now exclude any filters on the first where, not implemented
      (every? #(and (= select-var (-> % :s :variable))
                    ;; exclude if any recursion specified in where statement (e.g. person/follows+3)
                    (not (:recur %)))
              where))))

(defn re-parse-as-simple-subj-crawl
  "Returns true if query contains a single subject crawl.
  e.g.
  {:select {?subjects ['*']
   :where [...]}"
  [parsed-query]
  (log/debug "re-parse-as-simple-subj-crawl parsed-query:" parsed-query)
  (when (and (subject-crawl? parsed-query)
             (simple-subject-crawl? parsed-query)
             (not (:group-by parsed-query))
             (not= :variable (some-> parsed-query :order-by :type))
             (empty? (:supplied-vars parsed-query)))
    (log/debug "re-parse-as-simple-subj-crawl might be SSC if where clause passes muster")
    ;; following will return nil if parts of where clause exclude it from being a simple-subject-crawl
    (simple-subject-merge-where parsed-query)))
