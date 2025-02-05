(ns fluree.db.json-ld.policy.modify
  (:require [fluree.db.constants :as const]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]
            [fluree.db.json-ld.policy.enforce :as enforce]
            [fluree.db.json-ld.policy.rules :as policy.rules]))

#?(:clj (set! *warn-on-reflection* true))

(defn classes-for-sid
  [sid mods {:keys [schema] :as _db}]
  ;; TODO - get parent classes
  (->> (get mods sid)
       (filter #(= const/$rdf:type (flake/p %)))
       (map flake/o)
       seq))

(defn refresh-policy
  [db-after policy-values {:keys [target-subject target-property] :as policy}]
  (go-try
    (cond-> policy
      target-subject  (update :s-targets into (<? (policy.rules/parse-targets db-after policy-values target-subject)))
      target-property (update :p-targets into (<? (policy.rules/parse-targets db-after policy-values target-subject))))))

(defn refresh-modify-policies
  "Update targets to include newly created targets."
  [db-after]
  (go-try
    (let [policy-values (-> db-after :policy :policy-values)]
      (loop [[policy & r] (-> db-after :policy :modify :default)
             refreshed []]
        (if policy
          (let [{:keys [target-subject target-property]} policy]
            (if (or target-subject target-property)
              (let [policy* (<? (refresh-policy db-after policy-values policy))]
                (recur r (conj refreshed policy*)))
              (recur r (conj refreshed policy))))
          (assoc-in db-after [:policy :modify :default] refreshed))))))

(defn allowed?
  "Checks if all 'adds' are allowed by the policy. If so
  returns final db.

  If encounters a policy error, will throw with policy error
  message (if available)."
  [{:keys [db-after add mods]}]
  (go-try
    (let [{:keys [policy]} (<? (refresh-modify-policies db-after))]
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
  (empty? (:modify policy)))
