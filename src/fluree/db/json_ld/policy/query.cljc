(ns fluree.db.json-ld.policy.query
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.json-ld.policy.enforce :as enforce]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log :include-macros true]))

#?(:clj (set! *warn-on-reflection* true))

(defn unrestricted?
  [db]
  (enforce/unrestricted-view? (:policy db)))

(defn allow-flake?
  "Returns one of:
  (a) exception if there was an error
  (b) truthy value if flake is allowed
  (c) falsey value if flake not allowed

  Note: does not check here for unrestricted-view? as that should
  happen upstream. Assumes this is a policy-wrapped db if it ever
  hits this fn.

  Class policies are stored directly in [:view :property pid] with a :class-policy? flag.
  This enables a single O(1) lookup. Class applicability filtering is handled lazily
  inside policies-allow-viewing? using cached class membership lookups."
  [{:keys [policy] :as db} tracker flake]
  (go-try
    (let [pid      (flake/p flake)
          sid      (flake/s flake)
          ;; Single O(1) lookup - gets both regular and class-derived policies
          property-policies (enforce/view-policies-for-property policy pid)
          ;; Collect all applicable policies
          policies (concat property-policies
                           (enforce/view-policies-for-subject policy sid)
                           (enforce/view-policies-for-flake db flake))]
      (if-some [required-policies (not-empty (filter :required? policies))]
        (<? (enforce/policies-allow-viewing? db tracker sid required-policies))
        (<? (enforce/policies-allow-viewing? db tracker sid policies))))))

(defn allow-iri?
  "Returns async channel with truthy value if iri is visible for query results"
  [db tracker iri]
  (if (unrestricted? db)
    (go true)
    (try*
      (let [sid      (iri/encode-iri db iri)
            id-flake (flake/create sid const/$id nil nil nil nil nil)]
        (allow-flake? db tracker id-flake))
      (catch* e
        (log/error e "Unexpected exception in allow-iri? checking permission for iri: " iri)
        (go (ex-info (str "Unexpected exception in allow-iri? checking permission for iri: " iri
                          "Exception encoding IRI to internal format.")
                     {:status 500
                      :error :db/unexpected-error}))))))
