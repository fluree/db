(ns fluree.db.json-ld.policy-validate
  (:require [fluree.db.dbproto :as dbproto]
            [fluree.db.util.async :refer [<? go-try]]
            [clojure.core.async :as async]
            [fluree.db.query.range :as query-range]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.constants :as const]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn resolve-equals-rule
  "When using an equals rule, calculates a given path's value and stores in local cache.

  Equals should return a single value result. If anywhere along the path multiple results
  are returned, it will choose the first one and log out a warning that equals is being
  used with data that is not compliant (prefer f:contains)."
  [{:keys [policy] :as db} path equals-rule]
  (go-try
    (let [{:keys [cache ident]} policy
          db-root (dbproto/-rootdb db)]
      (loop [[next-iri & r] path
             last-result    ident]
        (if (and last-result next-iri)
          (let [next-pid (iri/encode-iri db next-iri)
                next-res (<? (query-range/index-range db-root :spot = [last-result next-pid]))
                ;; in case of mixed data types, take the first IRI result - unless we
                ;; are at the end of the path in which case take the first value regardless
                next-val (some #(when (= const/$xsd:anyURI (flake/dt %))
                                  (flake/o %))
                               next-res)]
            (when (> (count next-res) 1)
              (log/warn (str "f:equals used for identity " ident " and path: " equals-rule
                             " however the query produces more than one result, the first one "
                             " is being used which can produce unpredictable results. "
                             "Prefer f:contains when comparing with multiple results.")))
            (recur r next-val))
          (do
            (swap! cache assoc equals-rule last-result)
            last-result))))))

(defn cache-store-value
  "Caches path lookup result into the policy map cache. Returns original value."
  [db cache-key value]
  (swap! (get-in db [:policy :cache])
         assoc cache-key value)
  value)

(defn cache-get-value
  "Attempts to return cached result in policy key. Cache implemented to work correctly
  only with non-boolean result values - and thus can avoid having to do additional logic (e.g. contains? or some)"
  [db cache-key]
  (get @(get-in db [:policy :cache]) cache-key))

(defn generate-equals-fn
  "Returns validating function for :f/equals rule.

  Validating functions take two arguments, the db and the flake to be validated.

  Returns two-tuple of [async? policy-fn] where async? is boolean if policy-fn returns an async channel
  which must be resolved to get the final value.

  All policy functions are evaluated for a truthy or falsey result which determines if the provided flake
  can be operated on/viewed."
  [rule property-path]
  (if (= const/iri-$identity (first property-path))
    ;; make certain first element of path is :f/$identity which following fn only
    ;; considers. Will support other path constructs in the future
    ;; remove :f/$identity - following logic will "substitute" the user's actual identity in its place
    (let [path-no-identity (rest property-path)
          f                (fn [db flake]
                             (go-try
                               ;; because same 'path' is likely used in many flake
                               ;; evaluations, keep a local cache of results so
                               ;; expensive lookup only happens once per
                               ;; query/transaction.
                               (let [path-val (or (cache-get-value db property-path)
                                                  (->> (async/<! (resolve-equals-rule db path-no-identity rule))
                                                       (cache-store-value db property-path)))]
                                 (if (util/exception? path-val)
                                   (do
                                     (log/warn "Exception while processing path in policy rule, not allowing flake for subject " (flake/s flake)
                                               " through policy enforcement for rule: " rule)
                                     false)
                                   (= (flake/s flake) path-val)))))]
      [true f])
    (do
      (log/warn (str "Policy f:equals only supports equals paths that start with f:$identity currently. "
                     "Ignoring provided rule: " rule))
      [false (constantly false)])))
