(ns fluree.db.json-ld.policy.query
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.policy.enforce :as enforce]
            [fluree.db.util.async :refer [<? go-try]]))

#?(:clj (set! *warn-on-reflection* true))

(defn unrestricted?
  [db]
  (enforce/unrestricted-view? (:policy db)))

;; TODO - could cache resolved policies and not  just classes
;; TODO - need to look for any other use of (:cache policy) to see
(defn cached-class-policies
  [policy sid]
  (when-let [classes (get @(:cache policy) sid)]
    (enforce/policies-for-classes policy false classes)))

(defn class-policies
  [{:keys [policy] :as db} sid]
  (go-try
   (let [class-sids (<? (dbproto/-class-ids db sid))]
     (swap! (:cache policy) assoc sid class-sids)
     (enforce/policies-for-classes policy false class-sids))))

(defn allow-flake?
  "Returns one of:
  (a) exception if there was an error
  (b) truthy value if flake is allowed
  (c) falsey value if flake not allowed

  Note: does not check here for unrestricted-view? as that should
  happen upstream. Assumes this is a policy-wrapped db if it ever
  hits this fn."
  [{:keys [policy] :as db} flake]
  (go-try
   (let [pid     (flake/p flake)
         sid     (flake/s flake)]
     (if-let [p-policies (enforce/policies-for-property policy false pid)]
       (<? (enforce/policies-allow? db false sid p-policies))
       (if-let [c-policies (or (cached-class-policies policy sid)
                               (<? (class-policies db sid)))]
         (<? (enforce/policies-allow? db false sid c-policies))
         (if-let [d-policies (enforce/default-policies policy false)]
           (<? (enforce/policies-allow? db false sid d-policies))
           false))))))

(defn allow-iri?
  "Returns async channel with truthy value if iri is visible for query results"
  [db iri]
  (if (unrestricted? db)
    (go true)
    (try*
      (let [sid      (iri/encode-iri db iri)
            id-flake (flake/create sid const/$id nil nil nil nil nil)]
        (allow-flake? db id-flake))
      (catch* e
        (log/error e "Unexpected exception in allow-iri? checking permission for iri: " iri)
        (go (ex-info (str "Unexpected exception in allow-iri? checking permission for iri: " iri
                          "Exception encoding IRI to internal format.")
                     {:status 500
                      :error :db/unexpected-error}))))))

(defn filter-flakes
  "Iterates over multiple flakes and returns the allowed flakes from policy, or
  an empty sequence if none are allowed."
  [db error-ch flakes]
  (go-try
   (let [parellelism 4
         from-ch     (async/chan parellelism) ;; keep to parallelism, so if exception occurs can close prematurely
         to-ch       (async/chan)]
     (async/onto-chan! from-ch flakes)
     (async/pipeline-async parellelism
                           to-ch
                           (fn [flake ch]
                             (async/go
                               (try*
                                (let [allow? (<? (allow-flake? db flake))]
                                  (if allow?
                                    (async/>! ch flake)
                                    (async/>! ch ::restricted))
                                  (async/close! ch))
                                (catch* e
                                        (log/error e "Exception in allow-flakes? checking permission for flake: " flake)
                                        (async/>! error-ch e)))))
                           from-ch)
     (async/reduce (fn [acc result]
                     (if (= ::restricted result)
                       acc
                       (conj acc result)))
                   [] to-ch))))

(defn filter-subject-flakes
  "Takes multiple flakes for the *same* subject and optimizes evaluation
  for the group. Returns the allowed flakes, or an empty vector if none
  are allowed.

  This function is here to take advantage of some possible optimization
  for same-subject flakes - however without some additional work to
  analyze the policy queries and determine dependencies on properties or
  values of flakes it cannot be had yet.

  Leaving it here as some code leverages this function, and optimization
  work can be done here in the future."
  [db flakes]
  (let [error-ch  (async/chan)
        result-ch (filter-flakes db error-ch flakes)]
    (async/go
     (async/alt!
      result-ch ([r] r)
      error-ch ([e] e)))))
