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

(defn class-policy-map
  "Returns class policy map"
  [policy modify?]
  (if modify?
    (get-in policy [:modify :class])
    (get-in policy [:view :class])))

(defn property-policy-map
  "Returns property policy map"
  [policy modify?]
  (if modify?
    (get-in policy [:modify :property])
    (get-in policy [:view :property])))

(defn policies-for-classes
  "Returns sequence of policies that apply to the provided classes."
  [policy modify? classes]
  (let [class-policies (class-policy-map policy modify?)]
    (seq (apply concat (keep #(get class-policies %) classes)))))

(defn policies-for-property
  "Returns policy properties if they exist for the provided property
  else nil"
  [policy-map modify? property]
  (let [prop-policies (property-policy-map policy-map modify?)]
    (get prop-policies property)))

(defn default-policies
  "Returns default policies if they exist else nil"
  [policy-map modify?]
  (if modify?
    (get-in policy-map [:modify :default])
    (get-in policy-map [:view :default])))

(defn policies-for-flake
  [{:keys [policy] :as _db} [s p _o :as _flake] modify?]
  (->> (default-policies policy modify?)
       (keep (fn [{:keys [s-targets p-targets default?] :as policy}]
               (when (or (and (or (nil? s-targets) (contains? s-targets s))
                              (or (nil? p-targets) (contains? p-targets p)))
                         default?)
                 policy)))))

(defn policy-query
  [db sid query]
  (let [policy-values (-> db :policy :policy-values)
        this-val      (iri/decode-sid db sid)
        values        (-> (util.parse/normalize-values policy-values)
                          (policy/inject-value-binding "?$this" {"@value" this-val "@type" const/iri-id}))]
    (policy/inject-where-pattern query ["values" values])))

(defn modify-exception
  [policies]
  (ex-info (or (some :ex-message policies)
               "Policy enforcement prevents modification.")
           {:status 403 :error :db/policy-exception}))

(def ^:const deny-query-result false)

(defn policies-allow?
  "Once narrowed to a specific set of policies, execute and return
  appropriate policy response."
  [db tracker modify? sid policies-to-eval]
  (let [tracer (-> db :policy :trace)]
    (go-try
      (loop [[policy & r] policies-to-eval]
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
          (if modify?
            (modify-exception policies-to-eval)
            false))))))
