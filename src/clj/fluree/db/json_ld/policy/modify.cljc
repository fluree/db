(ns fluree.db.json-ld.policy.modify
  (:require [fluree.db.constants :as const]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]
            [fluree.db.json-ld.policy.enforce :as enforce]))

#?(:clj (set! *warn-on-reflection* true))

(defn classes-for-sid
  [sid mods {:keys [schema] :as _db}]
  ;; TODO - get parent classes
  (->> (get mods sid)
       (filter #(= const/$rdf:type (flake/p %)))
       (map flake/o)
       seq))

(defn allowed?
  "Checks if all 'adds' are allowed by the policy. If so
  returns final db.

  If encounters a policy error, will throw with policy error
  message (if available)."
  [{:keys [db-after add mods]}]
  (go-try
   (let [{:keys [policy]} db-after]
     (if (enforce/unrestricted-modify? policy)
       db-after
       (loop [[flake & r] add]
         (if flake
           (let [sid (flake/s flake)]
             (if-let [p-policies (enforce/policies-for-property policy true (flake/p flake))]
               (<? (enforce/policies-allow? db-after true sid (:values-map policy) p-policies))
               (if-let [c-policies (->> (classes-for-sid sid mods db-after)
                                        (enforce/policies-for-classes policy true))]
                 (<? (enforce/policies-allow? db-after true sid (:values-map policy) c-policies))
                 (if-let [d-policies (enforce/default-policies policy true)]
                   (<? (enforce/policies-allow? db-after true sid (:values-map policy) d-policies))
                   false)))

             (recur r))
           ;; no more flakes, all passed so return final db
           db-after))))))
