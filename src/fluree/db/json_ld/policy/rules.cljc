(ns fluree.db.json-ld.policy.rules
  (:require [clojure.core.async :as async]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [<?]]))

#?(:clj (set! *warn-on-reflection* true))

(defn view-restriction?
  [restriction-map]
  (:view? restriction-map))

(defn modify-restriction?
  [restriction-map]
  (:modify? restriction-map))

(defn policy-cids
  "Returns class subject ids for a given policy restriction map.

  Relevant classes are specified in the :on-class key of the restriction map."
  [db restriction-map]
  (when-let [classes (:on-class restriction-map)]
    (->> classes
         (map #(iri/encode-iri db %))
         set)))

(defn add-default-restriction
  [restriction policy]
  (cond-> policy

    (view-restriction? restriction)
    (update-in [:view :default] util/conjv restriction)

    (modify-restriction? restriction)
    (update-in [:modify :default] util/conjv restriction)))

(defn add-class-restriction
  [restriction-map db policy-map]
  (let [cids (policy-cids db restriction-map)]
    (reduce
     (fn [policy cid]
       (let [restriction-map* (assoc restriction-map :cid cid)]
         (cond-> policy

           (view-restriction? restriction-map*)
           (update-in [:view :class cid] util/conjv restriction-map*)

           (modify-restriction? restriction-map*)
           (update-in [:modify :class cid] util/conjv restriction-map*))))
     policy-map
     cids)))

(defn add-property-restriction
  [restriction-map db policy-map]
  (let [cids (policy-cids db restriction-map)]
    (reduce
     (fn [policy property]
       (let [pid              (if (iri/sid? property) property (iri/encode-iri db property))
             restriction-map* (assoc restriction-map :pid pid
                                     :cids cids)]
         (cond-> policy

           (view-restriction? restriction-map*)
           (update-in [:view :property pid] util/conjv restriction-map*)

           (modify-restriction? restriction-map*)
           (update-in [:modify :property pid] util/conjv restriction-map*))))
     policy-map
     (:on-property restriction-map))))

(defn query-target?
  "A target-expr can either be a static IRI or a query map."
  [target-expr]
  (map? target-expr))

(defn parse-targets
  [db tracker error-ch policy-values target-exprs]
  (let [in-ch  (async/to-chan! target-exprs)
        out-ch (async/chan 2 (map (fn [iri] (iri/iri->sid iri (:namespaces db)))))]
    (async/pipeline-async 2
                          out-ch
                          (fn [target-expr ch]
                            (async/go
                              (try*
                                (if (query-target? target-expr)
                                  (let [target-q (cond-> (assoc target-expr
                                                                "select" "?$target"
                                                                :selection-context {}) ;; don't compact selection results
                                                   policy-values (policy/inject-where-pattern ["values" policy-values]))]
                                    (->> (<? (dbproto/-query db tracker target-q))
                                         (async/onto-chan! ch)))
                                 ;; non-maps are literals
                                  (async/onto-chan! ch [target-expr]))
                                (catch* e
                                  (async/>! error-ch e)))))
                          in-ch)
    (async/into #{} out-ch)))

(defn unwrap
  [targets]
  (not-empty (mapv #(or (util/get-id %) (util/get-value %)) targets)))

(defn parse-policy
  [db tracker error-ch policy-values policy-doc]
  (async/go
    (try*
      (let [id (util/get-id policy-doc) ;; @id name of policy-doc

            target-subject      (unwrap (get policy-doc const/iri-targetSubject))
            subject-targets-ch  (when target-subject
                                  (parse-targets db tracker error-ch policy-values target-subject))
            target-property     (unwrap (get policy-doc const/iri-targetProperty))
            property-targets-ch (when target-property
                                  (parse-targets db tracker error-ch policy-values target-property))

            on-property (when-let [p-iris (util/get-all-ids policy-doc const/iri-onProperty)]
                          (set p-iris))
            on-class    (when-let [classes (util/get-all-ids policy-doc const/iri-onClass)]
                          (set classes))

            allow?    (util/get-first-value policy-doc const/iri-allow)

            src-query (util/get-first-value policy-doc const/iri-query)
            query     (cond
                        ;; f:allow takes precedence - no query needed
                        (some? allow?)
                        nil

                        (map? src-query)
                        (assoc src-query "select" "?$this" "limit" 1)

                        (nil? src-query)
                        nil

                        :else
                        (throw (ex-info (str "Invalid policy query. Query must be a map, instead got: " src-query)
                                        {:status 400
                                         :error  :db/invalid-policy})))
            actions   (set (util/get-all-ids policy-doc const/iri-action))
            view?     (or (empty? actions) ;; if actions is not specified, default to all actions
                          (contains? actions const/iri-view))
            modify?   (or (empty? actions)
                          (contains? actions const/iri-modify))

            subject-targets  (when subject-targets-ch (<? subject-targets-ch))
            property-targets (when property-targets-ch (<? property-targets-ch))]

        (when (and (nil? allow?)
                   (nil? query)
                   (nil? target-subject)
                   (nil? target-property)
                   (nil? on-property)
                   (nil? on-class))
          (throw (ex-info (str "Invalid policy, unable to extract a target subject, property, or on-property."
                               "Did you forget @context?. Parsed restriction: " policy-doc)
                          {:status 400
                           :error  :db/invalid-policy})))

        (if (or view? modify?)
          (cond-> {:id          id
                   :on-property on-property
                   :on-class    on-class
                   :required?   (util/get-first-value policy-doc const/iri-required)
                 ;; with no class or property restrictions, becomes a default policy-doc
                   :default?    (and (nil? on-property)
                                     (nil? on-class)
                                     (nil? subject-targets)
                                     (nil? property-targets))
                   :ex-message  (util/get-first-value policy-doc const/iri-exMessage)
                   :view?       view?
                   :modify?     modify?
                   :allow?      allow?
                   :query       query}
            target-subject               (assoc :target-subject target-subject)
            target-property              (assoc :target-property target-property)
            (not-empty subject-targets)  (assoc :s-targets subject-targets)
            (not-empty property-targets) (assoc :p-targets property-targets))
        ;; policy-doc has incorrectly formatted view? and/or modify?
        ;; this might allow data through that was intended to be restricted, so throw.
          (throw (ex-info (str "Invalid policy definition. Policies must have f:action of {@id: f:view} or {@id: f:modify}. "
                               "Policy data that failed: " policy-doc)
                          {:status 400
                           :error  :db/invalid-policy}))))
      (catch* e
        (async/put! error-ch e)))))

(defn enforcement-report
  [db]
  (some-> db
          :policy
          :trace
          (update-vals (fn [p-report]
                         (update-vals p-report deref)))))

(defn build-wrapper
  [db]
  (fn [wrapper policy]
    (cond
      (seq (:on-property policy))
      (add-property-restriction policy db wrapper)

      (or (:s-targets policy)
          (:p-targets policy)
          (:o-targets policy))
      (add-default-restriction policy wrapper)

      (seq (:on-class policy))
      (add-class-restriction policy db wrapper)

      (:default? policy)
      (add-default-restriction policy wrapper)

      :else
      wrapper)))

(defn parse-policies
  [db tracker error-ch policy-values policy-docs]
  (let [policy-ch     (async/chan)]
    ;; parse policies and put them on the policy-ch, output to error-ch in case of error
    (->> policy-docs
         async/to-chan!
         (async/pipeline-async 2
                               policy-ch
                               (fn [policy-doc ch]
                                 (-> (parse-policy db tracker error-ch policy-values policy-doc)
                                     (async/pipe ch)))))

    ;; build policy wrapper attached to db containing parsed policies
    (async/reduce (build-wrapper db) {} policy-ch)))

(defn ensure-ground-identity
  "A policy must never have a \"fresh\" ?$identity variable, otherwise it may match any
  identity in the db. This ensures the ?$identity is always provided as a \"ground\"
  value."
  [[vars :as policy-values]]
  (if (contains? (set vars) "?$identity")
    ;; already has a ground value for $?identity
    policy-values
    ;; bind ?$identity to a ground value that will never match anything
    (policy/inject-value-binding policy-values "?$identity" {const/iri-value (str ":" (random-uuid))
                                                             const/iri-type const/iri-id})))

(defn wrap-policy
  ([db policy-rules policy-values]
   (wrap-policy db nil policy-rules policy-values nil))
  ([db tracker policy-rules policy-values]
   (wrap-policy db tracker policy-rules policy-values nil))
  ([db tracker policy-rules policy-values default-allow?]
   (async/go
     (let [error-ch        (async/chan)
           policy-values*  (ensure-ground-identity policy-values)
           [wrapper _]     (async/alts! [error-ch (parse-policies db tracker error-ch policy-values*
                                                                  (util/sequential policy-rules))])]
       (if (util/exception? wrapper)
         wrapper
         (assoc db :policy (assoc wrapper
                                  :cache (atom {})
                                  :policy-values policy-values*
                                  :default-allow? default-allow?)))))))
