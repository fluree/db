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
            (let [sid      (flake/s flake)
                  pid      (flake/p flake)
                  policies (concat (enforce/policies-for-property policy true pid)
                                   (->> (classes-for-sid sid mods db-after)
                                        (enforce/policies-for-classes policy true))
                                   (enforce/policies-for-flake db-after flake true))]
              ;; policies-allow? will throw if access forbidden
              (if-some [required-policies (not-empty (filter :required? policies))]
                (<? (enforce/policies-allow? db-after true sid required-policies))
                (<? (enforce/policies-allow? db-after true sid policies)))
              (recur r))
            ;; no more flakes, all passed so return final db
            db-after))))))

(defn deny-all?
  "Returns true if policy allows no modification."
  [{:keys [policy] :as _db}]
  (empty? (get policy const/iri-modify)))
