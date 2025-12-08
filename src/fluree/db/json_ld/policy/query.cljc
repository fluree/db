(ns fluree.db.json-ld.policy.query
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
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

(defn- filter-applicable-class-policies
  "Filters class-derived policies to only those applicable to the subject's classes.
   For property-indexed class policies, we need to verify the subject is of the target class.
   Uses the policy cache to avoid redundant class lookups."
  [{:keys [policy] :as db} tracker sid class-derived-policies]
  (go-try
    (when (seq class-derived-policies)
      (let [;; Get subject's classes (using cache if available)
            subject-classes (or (get @(:cache policy) sid)
                                (let [classes (<? (dbproto/-class-ids db tracker sid))]
                                  (swap! (:cache policy) assoc sid classes)
                                  classes))]
        ;; Filter to only policies where subject is of a target class
        ;; Convert subject-classes to set to use as predicate in some
        (filter (fn [{:keys [for-classes]}]
                  (some (set subject-classes) for-classes))
                class-derived-policies)))))

(defn allow-flake?
  "Returns one of:
  (a) exception if there was an error
  (b) truthy value if flake is allowed
  (c) falsey value if flake not allowed

  Note: does not check here for unrestricted-view? as that should
  happen upstream. Assumes this is a policy-wrapped db if it ever
  hits this fn.

  Class policies are stored directly in [:view :property pid] with a :class-policy? flag.
  This enables a single O(1) lookup - class-derived policies are filtered inline based
  on subject's classes (cached)."
  [{:keys [policy] :as db} tracker flake]
  (go-try
    (let [pid      (flake/p flake)
          sid      (flake/s flake)
          ;; Single O(1) lookup - gets both regular and class-derived policies
          all-property-policies (enforce/view-policies-for-property policy pid)
          ;; Separate regular vs class-derived policies
          {class-derived-policies true
           regular-property-policies false} (group-by #(boolean (:class-policy? %))
                                                      (or all-property-policies []))
          ;; Filter class-derived policies to only those where subject is of target class
          applicable-class-policies (when (seq class-derived-policies)
                                      (<? (filter-applicable-class-policies db tracker sid class-derived-policies)))
          policies (concat regular-property-policies
                           (enforce/view-policies-for-subject policy sid)
                           applicable-class-policies
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
