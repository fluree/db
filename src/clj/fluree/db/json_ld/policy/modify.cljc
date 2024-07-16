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
           (if-let [p-policies (enforce/policies-for-property policy true (flake/p flake))]
             (let [result (<? (enforce/policies-allow? db-after true (flake/s flake) (:values-map policy) p-policies))]
               (if (true? result)
                 (recur r)
                 (enforce/default-val policy true p-policies)))
             (if-let [classes (classes-for-sid (flake/s flake) mods db-after)]
               (<? (enforce/class-allow? db-after (flake/s flake) true classes))
               (let [default-result (enforce/default-val policy true nil)]
                 (if (util/exception? default-result)
                   default-result
                   (recur r)))))
           db-after))))))
