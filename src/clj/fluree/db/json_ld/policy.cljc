(ns fluree.db.json-ld.policy
  (:require [clojure.core.async :refer [go <!]]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld]))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol Restrictable
  (wrap-policy [db policy-rules values-map])
  (root [db]))

(def root-policy-map
  "Base policy (permissions) map that will give access to all flakes."
  {const/iri-view   {:root? true}
   const/iri-modify {:root? true}})

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
   #(if-let [query (get % const/iri-query)]
      (assoc % const/iri-query {"@value" query
                                "@type"  "@json"})
      %)
   query-results))

(defn wrap-class-policy
  "Given one or more policy classes, queries for policies
  containing those classes and calls `wrap-policy`"
  [db classes values-map]
  (go
    (let [c-values  (->> classes ;; for passing in classes as query `values`
                         util/sequential
                         (mapv (fn [c] {"@value" c
                                        "@type"  const/iri-id})))
          policies  (<! (dbproto/-query db {"select" {"?policy" ["*"]}
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
        (<! (wrap-policy db (json-ld/expand policies*) values-map))))))


(defn wrap-identity-policy
  "Given an identity (@id) that exists in the db which contains a
  property `f:policyClass` listing policy classes associated with
  that identity, queries for those classes and calls `wrap-policy`"
  [db identity values-map]
  (go
   (let [policies  (<! (dbproto/-query db {"select" {"?policy" ["*"]}
                                           "where"  [{"@id"                 identity
                                                      const/iri-policyClass "?classes"}
                                                     {"@id"   "?policy"
                                                      "@type" "?classes"}]}))
         policies* (if (util/exception? policies)
                     policies
                     (policy-from-query policies))
         val-map   (assoc values-map "?$identity" {"@value" identity
                                                   "@type"  const/iri-id})]
     (log/trace "wrap-identity-policy - extracted policy from identity: " identity
                " policy: " policies*)
     (if (util/exception? policies*)
       (ex-info (str "Unable to extract policies for identity: " identity
                     " with error: " (ex-message policies*))
                {:status 400 :error :db/policy-exception}
                policies*)
       (<! (wrap-policy db (json-ld/expand policies*) val-map))))))

(defn policy-enforced-opts?
  "Tests 'options' for a query or transaction to see if the options request
  policy enforcement."
  [opts]
  (or (:identity opts)
      (:policyClass opts)
      (:policy opts)))

(defn policy-enforce-db
  "Policy enforces a db based on the query/transaction options"
  [db parsed-context opts]
  (go-try
    (let [{:keys [identity policyClass policy policyValues]} opts]
     (cond

       identity
       (<? (wrap-identity-policy db identity policyValues))

       policyClass
       (let [classes (map #(json-ld/expand-iri % parsed-context) (util/sequential policyClass))]
         (<? (wrap-class-policy db classes policyValues)))

       policy
       (let [expanded-policy (json-ld/expand policy parsed-context)]
         (<? (wrap-policy db expanded-policy policyValues)))))))
