(ns fluree.db.json-ld.policy.query
  (:require [clojure.core.async :as async]
            [fluree.db.constants :as const]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.policy.enforce :as enforce]
            [fluree.db.util.async :refer [<? go-try]]))

#?(:clj (set! *warn-on-reflection* true))

(defn class-restrictions?
  [policy]
  (get-in policy [const/iri-view :class]))

(defn property-restrictions?
  [policy]
  (get-in policy [const/iri-view :property]))

(defn unrestricted?
  [db]
  (enforce/unrestricted-view? (:policy db)))

(defn allow-flake?
  "Returns one of:
  (a) exception if there was an error
  (b) truthy value if flake is allowed
  (c) falsey value if flake not allowed"
  [{:keys [policy] :as db} flake]
  (go-try
   (cond

     (enforce/unrestricted-view? policy)
     true

     ;; currently property-restrictions override class restrictions if present
     (property-restrictions? policy)
     (<? (enforce/property-allow? db false flake))

     (class-restrictions? policy)
     (let [sid (flake/s flake)]
       (<? (enforce/class-allow? db sid false nil)))

     :else ;; no restrictions, use default
     (:default-allow? policy))))

(defn allow-iri?
  [db sid]
  (let [id-flake (flake/create sid const/$id nil nil nil nil nil)]
    (allow-flake? db id-flake)))

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
