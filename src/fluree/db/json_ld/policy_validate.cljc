(ns fluree.db.json-ld.policy-validate
  (:require [fluree.db.dbproto :as dbproto]
            [fluree.db.util.async :refer [<? go-try]]
            [clojure.core.async :as async]
            [fluree.db.query.range :as query-range]
            [fluree.db.flake :as flake]
            [fluree.db.constants :as const]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]))


#?(:clj (set! *warn-on-reflection* true))


(defn subids
  "Returns a vector of subids from the input collection as a single result async chan.
  If any exception occurs during resolution, returns the error immediately."
  [db subjects]
  (async/go-loop [[next-sid & r] (map #(dbproto/-subid db %) subjects)
                  acc []]
    (if next-sid
      (let [next-res (async/<! next-sid)]
        (if (util/exception? next-res)
          next-res
          (recur r (conj acc (async/<! next-sid)))))
      acc)))


(defn resolve-equals-rule
  "When using an equals rule, calculates a given path's value and stores in local cache.

  Equals should return a single value result. If anywhere along the path multiple results
  are returned, it will choose the first one and log out a warning that equals is being
  used with data that is not compliant (prefer f:contains)."
  [{:keys [permissions] :as db} path-pids equals-rule]
  (go-try
    (let [{:keys [cache ident]} permissions
          db-root (dbproto/-rootdb db)]
      (loop [[next-pid & r] path-pids
             last-result ident]
        (if next-pid
          (let [next-res (<? (query-range/index-range db-root :spot = [last-result next-pid]))
                ;; in case of mixed data types, take the first IRI result - unless we
                ;; are at the end of the path in which case take the first value regardless
                next-val (some #(if (= const/$xsd:anyURI (flake/dt %))
                                  (flake/o %)) next-res)]
            (when (> (count next-res) 1)
              (log/warn (str "f:equals used for identity " ident " and path: " equals-rule
                             " however the query produces more than one result, the first one "
                             " is being used which can product unpredictable results. "
                             "Prefer f:contains when comparing with multiple results.")))
            (recur r next-val))
          (do
            (swap! cache assoc equals-rule last-result)
            last-result))))))


(defn resolve-contains-rule
  "When using a contains rule, calculates a given path's value and stores in local cache.

  Contains, unlike 'equals' will return a set of all possible results at the leaf of the
  defined path."
  [{:keys [permissions] :as db} path-pids equals-rule]
  (go-try
    (let [{:keys [cache ident]} permissions]
      (loop [[next-pid & rest-path] path-pids
             last-results #{ident}]
        (if next-pid
          (loop [[next-result & r] last-results
                 acc #{}]
            (if next-result
              (let [next-res (<? (query-range/index-range db :spot = [next-result next-pid]))]
                (recur r (reduce (fn [acc* res-flake]
                                   (if (= const/$xsd:anyURI (flake/dt res-flake))
                                     (conj acc* (flake/o res-flake))
                                     acc*))
                                 acc next-res)))
              (recur rest-path acc)))
          (do
            (swap! cache assoc equals-rule last-results)
            last-results))))))