(ns fluree.db.json-ld.policy.enforce
  (:require [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.json-ld.policy :as policy :refer [root]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.parse :as util.parse]))

#?(:clj (set! *warn-on-reflection* true))

(defn unrestricted-modify?
  [policy]
  (true? (get-in policy [:modify :root?])))

(defn unrestricted-view?
  [policy]
  (true? (get-in policy [:view :root?])))

(defn view-class-policy-map
  [policy]
  (get-in policy [:view :class]))

(defn modify-class-policy-map
  [policy]
  (get-in policy [:modify :class]))

(defn modify-property-policy-map
  [policy]
  (get-in policy [:modify :property]))

(defn view-property-policy-map
  [policy]
  (get-in policy [:view :property]))

(defn view-policies-for-classes
  [policy classes]
  (let [class-policies (view-class-policy-map policy)]
    (seq (apply concat (keep #(get class-policies %) classes)))))

(defn modify-policies-for-classes
  [policy classes]
  (let [class-policies (modify-class-policy-map policy)]
    (seq (apply concat (keep #(get class-policies %) classes)))))

(defn modify-policies-for-property
  [policy-map property]
  (let [prop-policies (modify-property-policy-map policy-map)]
    (get prop-policies property)))

(defn view-policies-for-property
  [policy-map property]
  (let [prop-policies (view-property-policy-map policy-map)]
    (get prop-policies property)))

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
  appropriate policy response."
  [db tracker sid policies]
  (let [tracer (-> db :policy :trace)]
    (go-try
      (loop [[policy & r] policies]
        ;; return first truthy response, else false
        (if policy
          (let [{exec-counter :executed
                 allowed-counter :allowed} (get tracer (:id policy))

                query   (when-let [query (:query policy)]
                          (policy-query db sid query))
                result  (if query
                          (seq (<? (dbproto/-query (root db) tracker query)))
                          deny-query-result)]
            (swap! exec-counter inc)
            (if result
              (do (swap! allowed-counter inc)
                  true)
              (recur r)))
          ;; no more policies left to evaluate - all returned false
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
