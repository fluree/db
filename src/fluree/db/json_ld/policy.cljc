(ns fluree.db.json-ld.policy
  (:require [clojure.core.async :refer [go <!]]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]
            [fluree.db.util.parse :as util.parse]
            [fluree.json-ld :as json-ld]))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol Restrictable
  (wrap-policy [db fuel-tracker policy-rules policy-values])
  (root [db]))

(def root-policy-map
  "Base policy (permissions) map that will give access to all flakes."
  {:view   {:root? true}
   :modify {:root? true}})

(defn root-db
  [db]
  (assoc db :policy root-policy-map))

;; TODO - For now, extracting a policy from a `select` clause does not retain the
;  @value: 'x', @type: '@json' structure for the value of `f:query` which
;  then creates an issue with JSON-LD parsing. This adds back the
;  explicit @type declaration for the query itself. Once there is a way
;  to have the query result come back as raw json-ld, then this step can
;  be removed.
(defn policy-from-query
  "Recasts @type: @json from a raw query result which
  would looses the @type information."
  [query-results]
  (mapv
   (fn [{q   const/iri-query
         t-s const/iri-targetSubject
         t-p const/iri-targetProperty :as policy}]
     (cond-> policy
       q   (assoc const/iri-query {"@value" q "@type" "@json"})
       t-s (assoc const/iri-targetSubject  (mapv #(if (get % "@id") % {"@value" % "@type" "@json"}) (util/sequential t-s)))
       t-p (assoc const/iri-targetProperty (mapv #(if (get % "@id") % {"@value" % "@type" "@json"}) (util/sequential t-p)))))
   query-results))

(defn wrap-class-policy
  "Given one or more policy classes, queries for policies
  containing those classes and calls `wrap-policy`"
  [db fuel-tracker classes policy-values]
  (go
    (let [c-values  (->> classes ;; for passing in classes as query `values`
                         util/sequential
                         (mapv (fn [c] {"@value" c
                                        "@type"  const/iri-id})))
          policies  (<! (dbproto/-query db fuel-tracker {"select" {"?policy" ["*"]}
                                                         "where"  [{"@id"   "?policy"
                                                                    "@type" "?classes"}]
                                                         "values" ["?classes" c-values]}))
          policies* (if (util/exception? policies)
                      policies
                      (policy-from-query policies))]
      (log/trace "wrap-class-policy - extracted policy from classes: " classes
                 " policy: " policies*)
      (if (util/exception? policies*)
        (ex-info (str "Unable to extract policies for classes: " classes
                      " with error: " (ex-message policies*))
                 {:status 400 :error :db/policy-exception}
                 policies*)
        (<! (wrap-policy db fuel-tracker (json-ld/expand policies*) policy-values))))))

(defn inject-value-binding
  "Inject the given var and value into a normalized values clause."
  [values var v]
  (let [[vars vals] values]
    [(into [var] (when vars (util/sequential vars)))
     (if (seq vals)
       (mapv (partial into [v]) vals)
       [[v]])]))

(defn inject-where-pattern
  [q pattern]
  (update q "where" (fn [where-clause]
                      (into [pattern]
                            (when where-clause (util/sequential where-clause))))))

(defn wrap-identity-policy
  "Given an identity (@id) that exists in the db which contains a
  property `f:policyClass` listing policy classes associated with
  that identity, queries for those classes and calls `wrap-policy`"
  [db fuel-tracker identity policy-values]
  (go
    (let [policies  (<! (dbproto/-query db fuel-tracker {"select" {"?policy" ["*"]}
                                                         "where"  [{"@id"                 identity
                                                                    const/iri-policyClass "?classes"}
                                                                   {"@id"   "?policy"
                                                                    "@type" "?classes"}]}))
          policies* (if (util/exception? policies)
                      policies
                      (policy-from-query policies))

          policy-values* (inject-value-binding policy-values "?$identity" {"@value" identity "@type" const/iri-id})]
      (log/trace "wrap-identity-policy - extracted policy from identity: " identity " policy: " policies*)
      (if (util/exception? policies*)
        (ex-info (str "Unable to extract policies for identity: " identity
                      " with error: " (ex-message policies*))
                 {:status 400 :error :db/policy-exception}
                 policies*)
        (<! (wrap-policy db fuel-tracker (json-ld/expand policies*) policy-values*))))))

(defn policy-enforced-opts?
  "Tests 'options' for a query or transaction to see if the options request
  policy enforcement."
  [opts]
  (or (:identity opts)
      (:policy-class opts)
      (:policy opts)))

(defn policy-enforce-db
  "Policy enforces a db based on the query/transaction options"
  ([db parsed-context opts]
   (policy-enforce-db db nil parsed-context opts))
  ([db fuel-tracker parsed-context opts]
   (go-try
     (let [{:keys [identity policy-class policy policy-values]} opts
           policy-values* (util.parse/normalize-values policy-values)]
       (cond

         identity
         (<? (wrap-identity-policy db fuel-tracker identity policy-values*))

         policy-class
         (let [classes (map #(json-ld/expand-iri % parsed-context) (util/sequential policy-class))]
           (<? (wrap-class-policy db fuel-tracker classes policy-values*)))

         policy
         (let [expanded-policy (json-ld/expand policy parsed-context)]
           (<? (wrap-policy db fuel-tracker expanded-policy policy-values*))))))))
