(ns fluree.db.json-ld.policy.rules
  (:require [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.reasoner.util :refer [parse-rules-graph]]
            [fluree.db.util.core :as util]
            [fluree.db.util.async :refer [go-try <?]]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn property-restriction?
  [restriction-map]
  (seq (:on-property restriction-map)))

(defn class-restriction?
  [restriction-map]
  (nil? (:on-property restriction-map)))

(defn view-restriction?
  [restriction-map]
  (:view? restriction-map))

(defn modify-restriction?
  [restriction-map]
  (:modify? restriction-map))

(defn extract-query
  [restriction]
  (let [query (util/get-first-value restriction const/iri-query)]
    (if (map? query)
      (assoc query "select" "?$this")
      (throw (ex-info (str "Invalid policy, unable to extract query from restriction: " restriction)
                      {:status 400
                       :error :db/invalid-policy})))))

(defn policy-cids
  "Returns class subject ids for a given policy restriction map.

  Relevant classes are specified in the :on-class key of the restriction map."
  [db restriction-map]
  (when-let [classes (:on-class restriction-map)]
    (->> classes
         (map #(iri/encode-iri db %))
         set)))

(defn add-view-prop-restriction
  [restriction pid policy]
  (update-in policy [const/iri-view :property pid] util/conjv restriction))

(defn add-modify-prop-restriction
  [restriction pid policy]
  (update-in policy [const/iri-modify :property pid] util/conjv restriction))

(defn add-view-class-restriction
  [restriction cid policy]
  (update-in policy [const/iri-view :class cid] util/conjv restriction))

(defn add-modify-class-restriction
  [restriction cid policy]
  (update-in policy [const/iri-modify :class cid] util/conjv restriction))

(defn parse-class-restriction
  [restriction-map db policy-map]
  (let [cids (policy-cids db restriction-map)]
    (reduce
     (fn [acc cid]
       (let [restriction-map* (assoc restriction-map :cid cid)]
         (cond->> acc

                  (view-restriction? restriction-map)
                  (add-view-class-restriction restriction-map* cid)

                  (modify-restriction? restriction-map)
                  (add-modify-class-restriction restriction-map* cid))))
     policy-map
     cids)))

(defn parse-property-restriction
  [restriction-map db policy-map]
  (let [cids (policy-cids db restriction-map)]
    (reduce
     (fn [acc property]
       (let [pid              (iri/encode-iri db property)
             restriction-map* (assoc restriction-map :pid pid
                                                     :cids cids)]
         (cond->> acc

                  (view-restriction? restriction-map)
                  (add-view-prop-restriction restriction-map* pid)

                  (modify-restriction? restriction-map)
                  (add-modify-prop-restriction restriction-map* pid))))
     policy-map
     (:on-property restriction-map))))

(defn restriction-map
  [restriction]
  (let [id          (util/get-id restriction) ;; @id name of restriction
        on-property (util/get-all-ids restriction const/iri-onProperty) ;; can be multiple properties
        on-class    (when-let [classes (util/get-all-ids restriction const/iri-onClass)]
                      (set classes))
        query       (extract-query restriction)
        actions     (set (util/get-all-ids restriction const/iri-action))
        view?       (or (empty? actions) ;; if actions is not specified, default to all actions
                        (contains? actions const/iri-view))
        modify?     (or (empty? actions)
                        (contains? actions const/iri-modify))]
    (cond
      ;; valid restriction must have at least one of view or modify, and on-property or on-class
      (and (or view? modify?)
           (or on-property on-class))
      {:id          id
       :on-property on-property
       :on-class    on-class
       :ex-message  (util/get-first-value restriction const/iri-exMessage)
       :view?       view?
       :modify?     modify?
       :query       query}

      ;; no property or class specified
      (or on-property on-class)
      (do
        (log/warn "Policy Restriction contain f:on-property or f:on-class, ignoring restriction: " id)
        ;; log returns nil, but explicit here to show intended nil value for downstream checks
        nil)

      :else ;; no view or modify specified
      (do
        (log/warn "Policy Restriction must be of type view or modify, ignoring restriction: " id)
        nil))))

(defn parse-policy-rules
  [db policy-rules]
  (reduce
   (fn [acc rule]
     (let [parsed-restriction (restriction-map rule)] ;; will return nil if formatting is not valid
       (cond

         (property-restriction? parsed-restriction)
         (parse-property-restriction parsed-restriction db acc)

         (class-restriction? parsed-restriction)
         (parse-class-restriction parsed-restriction db acc)

         :else
         acc)))
   {}
   policy-rules))

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

(defn wrap-policy
  [db policy-rules default-allow? values-map]
  (go-try
   (let [policy-rules (->> (parse-rules-graph policy-rules)
                           (parse-policy-rules db))]
     (log/trace "policy-rules: " policy-rules)
     (assoc db :policy (assoc policy-rules :cache (atom {})
                                           :values-map values-map
                                           :default-allow? default-allow?)))))

(defn wrap-identity-policy
  [db identity default-allow? values-map]
  (go-try
   (let [policies  (<? (dbproto/-query db {"select" {"?policy" ["*"]}
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
       (throw (ex-info (str "Unable to extract policies for identity: " identity
                            " with error: " (ex-message policies*))
                       {:status 400 :error :db/policy-exception}
                       policies*))
       (<? (wrap-policy db policies* default-allow? val-map))))))
