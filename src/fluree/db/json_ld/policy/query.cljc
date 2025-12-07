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

;; TODO - could cache resolved policies and not  just classes
;; TODO - need to look for any other use of (:cache policy) to see
(defn cached-class-policies
  [policy sid]
  (when-let [classes (get @(:cache policy) sid)]
    (enforce/view-policies-for-classes policy classes)))

(defn class-policies
  [{:keys [policy] :as db} tracker sid]
  (go-try
    (let [class-sids (<? (dbproto/-class-ids db tracker sid))]
      (swap! (:cache policy) assoc sid class-sids)
      (enforce/view-policies-for-classes policy class-sids))))

(defn allow-flake?
  "Returns one of:
  (a) exception if there was an error
  (b) truthy value if flake is allowed
  (c) falsey value if flake not allowed

  Note: does not check here for unrestricted-view? as that should
  happen upstream. Assumes this is a policy-wrapped db if it ever
  hits this fn."
  [{:keys [policy] :as db} tracker flake]
  (go-try
    (let [pid      (flake/p flake)
          sid      (flake/s flake)
          policies (concat (enforce/view-policies-for-property policy pid)
                           (enforce/view-policies-for-subject policy sid)
                           (or (cached-class-policies policy sid)
                               (when (-> policy :view :class not-empty)
                                 ;; only do range scan if we have /any/ class policies
                                 (<? (class-policies db tracker sid))))
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
