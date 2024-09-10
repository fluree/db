(ns fluree.db.json-ld.policy.enforce
  (:require [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.json-ld.policy :refer [root]]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn unrestricted-modify?
  [policy]
  (true? (get-in policy [const/iri-modify :root?])))

(defn unrestricted-view?
  [policy]
  (true? (get-in policy [const/iri-view :root?])))

(defn class-policy-map
  "Returns class policy map"
  [policy modify?]
  (if modify?
    (get-in policy [const/iri-modify :class])
    (get-in policy [const/iri-view :class])))

(defn property-policy-map
  "Returns property policy map"
  [policy modify?]
  (if modify?
    (get-in policy [const/iri-modify :property])
    (get-in policy [const/iri-view :property])))

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
    (get-in policy-map [const/iri-modify :default])
    (get-in policy-map [const/iri-view :default])))

(defn policy-query
  [db sid values-map policy]
  (let [query    (:query policy)
        this-var (iri/decode-sid db sid)
        values   (if-let [existing-values (get query "values")]
                   ;; TODO - merge existing values with new values
                   :TODO
                   [(into ["?$this"] (keys values-map))
                    [(into [{"@value" this-var
                             "@type"  const/iri-id}]
                           (vals values-map))]])]
    (assoc query "values" values)))

(defn modify-exception
  [policies]
  (ex-info (or (some :ex-message policies)
               "Policy enforcement prevents modification.")
           {:status 403 :error :db/policy-exception}))


(defn policies-allow?
  "Once narrowed to a specific set of policies, execute and return
  appropriate policy response."
  [db modify? sid values-map policies-to-eval]
  (go-try
   (loop [[p-map & r] policies-to-eval]
     ;; return first truthy response, else false
     (if p-map
       (let [query  (policy-query db sid values-map p-map)
             result (<? (dbproto/-query (root db) query))]
         (if (seq result)
           true
           (recur r)))
       ;; no more policies left to evaluate - all returned false
       (if modify?
         (modify-exception policies-to-eval)
         false)))))
