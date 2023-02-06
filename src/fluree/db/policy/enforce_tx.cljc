(ns fluree.db.policy.enforce-tx
  (:require [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.flake :as flake]
            [fluree.db.permissions-validate :as validate]))

#?(:clj (set! *warn-on-reflection* true))

(defn root?
  "Returns true if policy has root modify permissions."
  [policy]
  (= {:root? true} (get policy :f/modify)))


(defn- check-property-policies
  "Checks property policies, if they exist for a given flake's property and
  will reject entire transaction if any fail. If they don't exist, default to
  default-allow?, where it will continue if true, else reject entire transaction if false."
  [db property-policies default-allow? flakes]
  (go-try
    (let [policies-by-pid (validate/group-property-policies property-policies)]
      (loop [[flake & r] flakes]
        (if flake
          (if-let [p-policies (->> flake flake/p (get policies-by-pid))]
            (let [allow? (loop [[[async? f] & r] p-policies]
                           ;; return first truthy response, else false
                           (if f
                             (let [res (if async?
                                         (<? (f db flake))
                                         (f db flake))]
                               (or res
                                   (recur r)))
                             ;; always default to false! (deny)
                             false))]
              (if allow?
                (recur r)
                (throw (ex-info "Policy enforcement prevents modification."
                                {:status 400 :error :db/policy-exception}))))
            (if default-allow?
              (recur r)
              (throw (ex-info "Policy enforcement prevents modification."
                              {:status 400 :error :db/policy-exception}))))
          ;; passed all property policies, allow everything!
          true)))))


(defn allowed?
  "Returns true if all policy enforcement passes, else exception related to
  first policy the fails."
  [{:keys [db-after add]} {:keys [subj-mods] :as _tx-state}]
  (let [{:keys [policy]} db-after
        subj-mods' @subj-mods]
    (go-try
      (if (root? policy)
        db-after
        (loop [[s-flakes & r] (partition-by flake/s add)]
          (if s-flakes
            (let [fflake         (first s-flakes)
                  sid            (flake/s fflake)
                  {:keys [classes]} (get subj-mods' sid)
                  {defaults :default props :property} (validate/group-policies-by-default policy :f/modify classes)
                  default-allow? (<? (validate/default-allow? db-after fflake defaults))
                  allow?         (if props
                                   (<? (check-property-policies db-after props default-allow? s-flakes))
                                   default-allow?)]
              (if allow?
                (recur r)
                (throw (ex-info "Policy enforcement prevents modification."
                                {:status 400 :error :db/policy-exception}))))
            ;; all flakes processed and passed! return final db
            db-after))))))