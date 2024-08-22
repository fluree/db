(ns fluree.db.json-ld.policy.enforce
  (:require [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.flake :as flake]
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
  [policy-map modify? classes]
  (let [class-policies (class-policy-map policy-map modify?)]
    (seq (apply concat (keep #(get class-policies %) classes)))))

(defn policies-for-property
  "Returns policy properties if they exist for the provided property
  else nil"
  [policy-map modify? property]
  (let [prop-policies (property-policy-map policy-map modify?)]
    (get prop-policies property)))


(defn policy-query
  [db sid values-map policy]
  (let [query    (:query policy)
        this-var (iri/decode-sid db sid)
        values   (if-let [existing-values (get query "values")]
                   ;; TODO - merge existing values with new values
                   :TODO
                   [(into ["?$this"] (keys values-map))
                    [(into [{"@value" this-var
                             "@type" const/iri-id}]
                           (vals values-map))]])]
    (assoc query "values" values)))

(defn modify-exception
  [policies]
  (ex-info (or (some :ex-message policies)
               "Policy enforcement prevents modification.")
           {:status 403 :error :db/policy-exception}))

(defn default-val
  "Returns the default policy value if no policies are found. (true/false)
  If false and a transactions/modification, an exception is thrown."
  [{:keys [default-allow?] :as _policy} modify? policies]
  (if (true? default-allow?)
    true
    (if modify?
      (modify-exception policies)
      false)))

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

(defn class-allow?
  "Evaluates if a class policy allows access to the provided subject.
  Returns true if class policy allows, else false.
  If no class policy exists, returns the value of `default-allow?`
  Optional 'classes' argument is a list of class sids to check for policies.

  If not passed in, the policy cache is checked for classes for the given
  subject, or a query is executed to retrieve and cache them.

  There is querying here that is expensive, so it should be checked
  before here if class policies exist, if not, then there is no need
  to utilize this function."
  [{:keys [policy] :as db} sid modify? classes]
  (go-try
   (let [classes*   (or classes
                        (get @(:cache policy) sid)
                        (let [class-sids (<? (dbproto/-class-ids db sid))]
                          (swap! (:cache policy) assoc sid class-sids)
                          class-sids))
         c-policies (policies-for-classes policy modify? classes*)]
     (if c-policies
       (<? (policies-allow? db modify? sid (:values-map policy) c-policies))
       (default-val policy modify? c-policies)))))


(defn property-allow?
  [{:keys [policy] :as db} modify? flake]
  (go-try
   (let [pid        (flake/p flake)
         sid        (flake/s flake)
         p-policies (policies-for-property policy modify? pid)]
     (if p-policies
       (<? (policies-allow? db modify? sid (:values-map policy) p-policies))
       (default-val policy modify? p-policies)))))
