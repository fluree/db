(ns fluree.db.json-ld.policy.enforce
  (:require [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.json-ld.policy :as policy :refer [root]]
            [fluree.db.track :as track]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.parse :as util.parse]))

#?(:clj (set! *warn-on-reflection* true))

(defn unrestricted-modify?
  [policy]
  (true? (get-in policy [:modify :root?])))

(defn unrestricted-view?
  [policy]
  (true? (get-in policy [:view :root?])))

(defn modify-property-policy-map
  [policy]
  (get-in policy [:modify :property]))

(defn view-property-policy-map
  [policy]
  (get-in policy [:view :property]))

(defn modify-policies-for-property
  [policy-map property]
  (let [prop-policies (modify-property-policy-map policy-map)]
    (get prop-policies property)))

(defn view-policies-for-property
  [policy-map property]
  (let [prop-policies (view-property-policy-map policy-map)]
    (get prop-policies property)))

(defn view-subject-policy-map
  [policy]
  (get-in policy [:view :subject]))

(defn modify-subject-policy-map
  [policy]
  (get-in policy [:modify :subject]))

(defn view-policies-for-subject
  "O(1) lookup for subject-targeted view policies."
  [policy-map subject]
  (let [subj-policies (view-subject-policy-map policy-map)]
    (get subj-policies subject)))

(defn modify-policies-for-subject
  "O(1) lookup for subject-targeted modify policies."
  [policy-map subject]
  (let [subj-policies (modify-subject-policy-map policy-map)]
    (get subj-policies subject)))

(defn default-view-policies
  [policy-map]
  (get-in policy-map [:view :default]))

(defn default-modify-policies
  [policy-map]
  (get-in policy-map [:modify :default]))

(defn applies-to-flake?
  [{:keys [s-targets p-targets default?] :as _policy} [s p _o :as _flake]]
  (or (and (or (nil? s-targets) (contains? s-targets s))
           (or (nil? p-targets) (contains? p-targets p)))
      default?))

(defn view-policies-for-flake
  [{:keys [policy] :as _db} flake]
  (filter (fn [policy]
            (applies-to-flake? policy flake))
          (default-view-policies policy)))

(defn modify-policies-for-flake
  [{:keys [policy] :as _db} flake]
  (filter (fn [policy]
            (applies-to-flake? policy flake))
          (default-modify-policies policy)))

(defn policy-query
  [db sid query]
  (let [policy-values (-> db :policy :policy-values)
        this-val      (iri/decode-sid db sid)
        values        (-> (util.parse/normalize-values policy-values)
                          (policy/inject-value-binding "?$this" {"@value" this-val "@type" const/iri-id}))]
    (policy/inject-where-pattern query ["values" values])))

(def ^:const deny-query-result false)

(defn- policies-allow?
  "Once narrowed to a specific set of policies, execute and return
  appropriate policy response. If no policies apply, returns the value
  of :default-allow? from the db's policy (defaults to false)."
  [db tracker sid policies]
  (go-try
    (if (empty? policies)
      ;; no policies apply - use default-allow? setting
      (get-in db [:policy :default-allow?] false)
      (loop [[{:keys [id query allow?] :as policy} & r] policies]
        ;; return first truthy response, else false
        (if policy
          (let [result (cond
                         ;; f:allow true - unconditional allow, no query needed
                         (true? allow?)
                         true

                         ;; f:allow false - unconditional deny, no query needed
                         (false? allow?)
                         false

                         ;; query exists - execute it
                         query
                         (seq (<? (dbproto/-query (root db) tracker (policy-query db sid query))))

                         ;; no allow? and no query - deny
                         :else
                         deny-query-result)]
            (track/policy-exec! tracker id)
            (if result
              (do (track/policy-allow! tracker id)
                  true)
              (recur r)))
          ;; policies exist but all returned false - deny
          false)))))

(defn policies-allow-viewing?
  [db tracker sid policies]
  (policies-allow? db tracker sid policies))

(defn policies-allow-modification?
  [db tracker sid policies]
  (go-try (or (<? (policies-allow? db tracker sid policies))
              (ex-info (or (some :ex-message policies)
                           "Policy enforcement prevents modification.")
                       {:status 403 :error :db/policy-exception}))))
