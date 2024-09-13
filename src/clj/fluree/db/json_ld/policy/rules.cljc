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

(defn property-restriction?
  [restriction-map]
  (seq (:on-property restriction-map)))

(defn class-restriction?
  [restriction-map]
  (seq (:on-class restriction-map)))

(defn default-restriction?
  [restriction-map]
  (:default? restriction-map))

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

(defn restriction-map
  [restriction]
  (let [id          (util/get-id restriction) ;; @id name of restriction
        on-property (when-let [props (util/get-all-ids restriction const/iri-onProperty)]
                      (set props)) ;; can be multiple properties
        on-class    (when-let [classes (util/get-all-ids restriction const/iri-onClass)]
                      (set classes))
        src-query   (util/get-first-value restriction const/iri-query)
        query       (if (map? src-query)
                      (assoc src-query "select" "?$this")
                      (throw (ex-info (str "Invalid policy, unable to extract query from f:query. "
                                           "Did you forget @context?. Parsed restriction: " restriction)
                                      {:status 400
                                       :error  :db/invalid-policy})))
        actions     (set (util/get-all-ids restriction const/iri-action))
        view?       (or (empty? actions) ;; if actions is not specified, default to all actions
                        (contains? actions const/iri-view))
        modify?     (or (empty? actions)
                        (contains? actions const/iri-modify))]
    (if (or view? modify?)
      {:id          id
       :on-property on-property
       :on-class    on-class
       :default?    (and (nil? on-property) (nil? on-class)) ;; with no class or property restrictions, becomes a default policy
       :ex-message  (util/get-first-value restriction const/iri-exMessage)
       :view?       view?
       :modify?     modify?
       :query       query}
      ;; policy has incorrectly formatted view? and/or modify?
      ;; this might allow data through that was intended to be restricted, so throw.
      (throw (ex-info (str "Invalid policy definition. Policies must have f:action of {@id: f:view} or {@id: f:modify}. "
                           "Policy restriction data that failed: " restriction)
                      {:status 400
                       :error  :db/invalid-policy})))))

(defn parse-policy-rules
  [db policy-rules]
  (reduce
   (fn [acc rule]
     (let [parsed-restriction (restriction-map rule)] ;; will return nil if formatting is not valid
       (cond

         (property-restriction? parsed-restriction)
         (add-property-restriction parsed-restriction db acc)

         (class-restriction? parsed-restriction)
         (add-class-restriction parsed-restriction db acc)

         (default-restriction? parsed-restriction)
         (add-default-restriction parsed-restriction acc)

         :else
         acc)))
   {}
   policy-rules))

(defn validate-values-map
  [values-map]
  (or (map? values-map)
      (throw (ex-info (str "Invalid policy values map. Must be a map. Received: " values-map)
                      {:status 400
                       :error  :db/invalid-values-map}))))

(defn wrap-policy
  [db policy-rules values-map]
  (go-try
   (when values-map
     (validate-values-map values-map))
   (let [policy-rules (->> policy-rules
                           util/sequential
                           (parse-policy-rules db))]
     (log/trace "policy-rules: " policy-rules)
     (assoc db :policy (assoc policy-rules :cache (atom {})
                                           :values-map values-map)))))
