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

(defn- filter-applicable-modify-class-policies
  "Filters class-derived policies to only those applicable to the subject's classes.
   For property-indexed class policies, we need to verify the subject is of the target class.
   Uses the class-policy-cache to avoid redundant class lookups."
  [db-after tracker class-policy-cache sid class-derived-policies]
  (go-try
    (when (seq class-derived-policies)
      (let [;; Get subject's classes (using cache if available)
            subject-classes (or (get @class-policy-cache sid)
                                (let [classes (<? (dbproto/-class-ids db-after tracker sid))]
                                  (swap! class-policy-cache assoc sid classes)
                                  classes))]
        ;; Filter to only policies where subject is of a target class
        (filter (fn [{:keys [for-classes]}]
                  (some (set subject-classes) for-classes))
                class-derived-policies)))))

(defn allowed?
  "Checks if all 'adds' are allowed by the policy. If so
  returns final db.

  If encounters a policy error, will throw with policy error
  message (if available).

  Class policies are stored directly in [:modify :property pid] with a :class-policy? flag.
  This enables a single O(1) lookup - class-derived policies are filtered inline based
  on subject's classes (cached)."
  [tracker {:keys [db-after add]}]
  (go-try
    (let [{:keys [policy]} (<? (refresh-modify-policies db-after tracker))
          class-policy-cache (atom {})]
      (if (enforce/unrestricted-modify? policy)
        db-after
        (loop [[flake & r] add]
          (if flake
            (let [sid      (flake/s flake)
                  pid      (flake/p flake)
                  ;; Single O(1) lookup - gets both regular and class-derived policies
                  all-property-policies (enforce/modify-policies-for-property policy pid)
                  ;; Separate regular vs class-derived policies
                  {class-derived-policies true
                   regular-property-policies false} (group-by #(boolean (:class-policy? %))
                                                              (or all-property-policies []))
                  ;; Filter class-derived policies to only those where subject is of target class
                  applicable-class-policies (when (seq class-derived-policies)
                                              (<? (filter-applicable-modify-class-policies
                                                   db-after tracker class-policy-cache sid class-derived-policies)))
                  policies (concat regular-property-policies
                                   (enforce/modify-policies-for-subject policy sid)
                                   applicable-class-policies
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
