(ns fluree.db.json-ld.policy.modify
  (:require [clojure.core.async :as async]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.policy.enforce :as enforce]
            [fluree.db.json-ld.policy.rules :as policy.rules]
            [fluree.db.util :as util]
            [fluree.db.util.async :refer [<? go-try]]))

#?(:clj (set! *warn-on-reflection* true))

(defn refresh-policy
  [db-after tracker error-ch policy-values {:keys [subject-specs target-property on-property-specs] :as policy}]
  (go-try
    (cond-> policy
      ;; subject-specs combines both onSubject (preferred) and targetSubject (legacy)
      ;; Refreshes :on-subject for O(1) indexed lookup
      subject-specs     (update :on-subject into (<? (policy.rules/parse-targets db-after tracker error-ch policy-values subject-specs)))
      target-property   (update :p-targets into (<? (policy.rules/parse-targets db-after tracker error-ch policy-values target-property)))
      on-property-specs (update :on-property into (<? (policy.rules/parse-targets db-after tracker error-ch policy-values on-property-specs))))))

(defn refresh-modify-policies
  "Update targets to include newly created targets."
  [db-after tracker]
  (go-try
    (let [error-ch (async/chan)
          policy-values (-> db-after :policy :policy-values)]
      (loop [[policy & r] (-> db-after :policy :modify :default)
             refreshed []]
        (if policy
          (let [{:keys [subject-specs target-property on-property-specs]} policy]
            (if (or subject-specs target-property on-property-specs)
              (let [[policy* _] (async/alts! [error-ch
                                              (refresh-policy db-after tracker error-ch policy-values policy)])]
                (if (util/exception? policy*)
                  (throw policy*)
                  (recur r (conj refreshed policy*))))
              (recur r (conj refreshed policy))))
          (assoc-in db-after [:policy :modify :default] refreshed))))))

(defn has-class-policies?
  [policy]
  (boolean (-> policy enforce/modify-class-policy-map not-empty)))

;; TODO - get parent classes
(defn subject-class-policies
  [db-after tracker policy class-policy-cache sid]
  (go-try
    (let [classes  (<? (dbproto/-class-ids db-after tracker sid))
          policies (or (enforce/modify-policies-for-classes classes policy)
                       [])]
      (swap! class-policy-cache assoc sid policies)
      policies)))

(defn allowed?
  "Checks if all 'adds' are allowed by the policy. If so
  returns final db.

  If encounters a policy error, will throw with policy error
  message (if available)."
  [tracker {:keys [db-after add]}]
  (go-try
    (let [{:keys [policy]} (<? (refresh-modify-policies db-after tracker))
          class-policies? (has-class-policies? policy)
          class-policy-cache (atom {})]
      (if (enforce/unrestricted-modify? policy)
        db-after
        (loop [[flake & r] add]
          (if flake
            (let [sid      (flake/s flake)
                  pid      (flake/p flake)
                  policies (concat (enforce/modify-policies-for-property policy pid)
                                   (enforce/modify-policies-for-subject policy sid)
                                   (when class-policies?
                                     (or (get @class-policy-cache sid)
                                         (<? (subject-class-policies db-after tracker policy class-policy-cache sid))))
                                   (enforce/modify-policies-for-flake db-after flake))]
              ;; policies-allow-modification? will throw if access forbidden
              (if-some [required-policies (not-empty (filter :required? policies))]
                (<? (enforce/policies-allow-modification? db-after tracker sid required-policies))
                (<? (enforce/policies-allow-modification? db-after tracker sid policies)))
              (recur r))
            ;; no more flakes, all passed so return final db
            db-after))))))

(defn deny-all?
  "Returns true if policy allows no modification."
  [{:keys [policy] :as _db}]
  (empty? (:modify policy)))
