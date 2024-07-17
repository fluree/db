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
           (let [p-policies (enforce/policies-for-property policy true (flake/p flake))
                 classes    (when (nil? p-policies)
                              (classes-for-sid (flake/s flake) mods db-after))]

             ;; all items in 'cond' below with throw if policy not allowed
             ;; and loop will terminate on first such exception
             (cond
               ;; property policies override all others
               p-policies
               (<? (enforce/policies-allow? db-after true (flake/s flake) (:values-map policy) p-policies))

               ;; if not property policies, check class policies
               classes
               (<? (enforce/class-allow? db-after (flake/s flake) true classes))

               ;; if no class policies, check if default-allow?, else deny
               :else
               (let [default (enforce/default-val policy true nil)]
                 (when (util/exception? default)
                   (throw default))))

             (recur r))
           ;; no more flakes, all passed so return final db
           db-after))))))
