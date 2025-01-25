(ns fluree.db.json-ld.policy.rules
  (:require [clojure.core.async :refer [<! go]]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.reasoner.util :refer [parse-rules-graph]]
            [fluree.db.util.core :as util]
            [fluree.db.util.async :refer [go-try <?]]
            [fluree.db.util.log :as log]))

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
          (update-in [const/iri-view :default] util/conjv restriction)

          (modify-restriction? restriction)
          (update-in [const/iri-modify :default] util/conjv restriction)))

(defn add-class-restriction
  [restriction-map db policy-map]
  (let [cids (policy-cids db restriction-map)]
    (reduce
     (fn [policy cid]
       (let [restriction-map* (assoc restriction-map :cid cid)]
         (cond-> policy

                 (view-restriction? restriction-map*)
                 (update-in [const/iri-view :class cid] util/conjv restriction-map*)

                 (modify-restriction? restriction-map*)
                 (update-in [const/iri-modify :class cid] util/conjv restriction-map*))))
     policy-map
     cids)))

(defn add-property-restriction
  [restriction-map db policy-map]
  (let [cids (policy-cids db restriction-map)]
    (reduce
     (fn [policy property]
       (let [pid              (iri/encode-iri db property)
             restriction-map* (assoc restriction-map :pid pid
                                                     :cids cids)]
         (cond-> policy

                 (view-restriction? restriction-map*)
                 (update-in [const/iri-view :property pid] util/conjv restriction-map*)

                 (modify-restriction? restriction-map*)
                 (update-in [const/iri-modify :property pid] util/conjv restriction-map*))))
     policy-map
     (:on-property restriction-map))))

(defn parse-policy
  [db policy-doc]
  (go-try
    (let [id (util/get-id policy-doc) ;; @id name of policy-doc

          on-property (when-let [props (util/get-all-ids policy-doc const/iri-onProperty)]
                        (set props)) ;; can be multiple properties
          on-class    (when-let [classes (util/get-all-ids policy-doc const/iri-onClass)]
                        (set classes))

          src-query (util/get-first-value policy-doc const/iri-query)
          query     (if (map? src-query)
                      (assoc src-query "select" "?$this" "limit" 1)
                      (throw (ex-info (str "Invalid policy, unable to extract query from f:query. "
                                           "Did you forget @context?. Parsed restriction: " policy-doc)
                                      {:status 400
                                       :error  :db/invalid-policy})))
          actions   (set (util/get-all-ids policy-doc const/iri-action))
          view?     (or (empty? actions) ;; if actions is not specified, default to all actions
                        (contains? actions const/iri-view))
          modify?   (or (empty? actions)
                        (contains? actions const/iri-modify))]
      (if (or view? modify?)
        {:id          id
         :on-property on-property
         :on-class    on-class
         :required?   (util/get-first-value policy-doc const/iri-required)
         :default?    (and (nil? on-property) (nil? on-class)) ;; with no class or property restrictions, becomes a default policy-doc
         :ex-message  (util/get-first-value policy-doc const/iri-exMessage)
         :view?       view?
         :modify?     modify?
         :query       query}
        ;; policy-doc has incorrectly formatted view? and/or modify?
        ;; this might allow data through that was intended to be restricted, so throw.
        (throw (ex-info (str "Invalid policy definition. Policies must have f:action of {@id: f:view} or {@id: f:modify}. "
                             "Policy data that failed: " policy-doc)
                        {:status 400
                         :error  :db/invalid-policy}))))))

(defn build-wrapper
  [db]
  (fn [wrapper policy]
    (cond
      (seq (:on-property policy))
      (add-property-restriction policy db wrapper)

      (seq (:on-class policy))
      (add-class-restriction policy db wrapper)

      (:default? policy)
      (add-default-restriction policy wrapper)

      :else
      wrapper)))

(defn parse-policies
  [db policy-docs]
  (let [policy-ch     (async/chan)
        policy-doc-ch (async/to-chan! policy-docs)]
    (async/pipeline-async 2
                          policy-ch
                          (fn [policy-doc ch]
                            (-> (parse-policy db policy-doc)
                                (async/pipe ch)))
                          policy-doc-ch)
    (async/reduce (build-wrapper db) {} policy-ch)))

(defn wrap-policy
  [db policy-rules policy-values]
  (go-try
    (let [wrapper (<? (parse-policies db (util/sequential policy-rules)))]
      (-> db
          (assoc :policy (assoc wrapper :cache (atom {}) :policy-values policy-values))))))
