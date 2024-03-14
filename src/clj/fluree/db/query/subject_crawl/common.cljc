(ns fluree.db.query.subject-crawl.common
  (:require [clojure.core.async :refer [go >!] :as async]
            [fluree.db.util.async :refer [<?]]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.query.json-ld.response :as json-ld-resp]))

#?(:clj (set! *warn-on-reflection* true))

(defn result-af
  [{:keys [db cache context compact-fn select-spec error-ch] :as _opts}]
  (fn [flakes port]
    (go
      (try*
        (let [result (<? (json-ld-resp/flakes->res db cache context compact-fn select-spec 0 flakes))]
          (when (not-empty result)
            (>! port result)))
        (async/close! port)
        (catch* e
                (log/error e "Error processing subject query result")
                (>! error-ch e))))))


(defn passes-filter?
  [filter-fn vars pred-flakes]
  (some #(filter-fn % vars) pred-flakes))

(defn pass-all-filters?
  "For a group of predicate flakes (all same .-p value)
  and a list of filter-functions, returns true if at least
  one of the predicates passes every function, else returns false."
  [filter-fns vars pred-flakes]
  (loop [[filter-fn & r-fns] filter-fns]
    (if filter-fn
      (if (passes-filter? filter-fn vars pred-flakes)
        (recur r-fns)
        false)
      true)))


(defn filter-subject
  "Filters a set of flakes for a single subject and returns true if
  the subject meets the filter map.

  filter-map is a map where pred-ids are keys and values are a list of filtering functions
  where each flake of pred-id must return a truthy value if the subject is allowed."
  [vars filter-map flakes]
  ;; TODO - fns with multiple vars will have to re-calc vars every time, this could be done once for the entire query
  (loop [[p-flakes & r] (partition-by flake/p flakes)
         required-p (:required-p filter-map)]
    (if p-flakes
      (let [p (-> p-flakes first flake/p)]
        (if-let [filter-fns (get filter-map p)]
          (when (pass-all-filters? filter-fns vars p-flakes)
            (recur r (disj required-p p)))
          (recur r (disj required-p p))))
      ;; only return flakes if all required-p values were found
      (when (empty? required-p)
        flakes))))


(defn order-results
  "If order-by exists in query, orders final results.
  order-by is defined by a map with keys (see analytical-parse for code):
  - :type - :variable or :predicate
  - :order - :asc or :desc
  - :predicate - if type = :predicate, contains predicate pid or name
  - :variable - if type = :variable, contains variable name (not supported for simple subject crawl)"
  [results {:keys [type order predicate]} limit offset]
  (if (= :variable type)
    (throw (ex-info "Ordering by a variable not supported in this type of query."
                    {:status 400 :error :db/invalid-query}))
    (let [sorted (cond-> (sort-by (fn [result] (get result predicate)) results)
                         (= :desc order) reverse)]
      (into []
            (comp (drop offset)
                  (take limit))
            sorted))))
