(ns fluree.db.transact
  (:require [fluree.db.constants :as const]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util]
            [fluree.json-ld :as json-ld]))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol Transactable
  (-stage-txn [db fuel-tracker context identity author annotation raw-txn parsed-txn])
  (-merge-commit [db commit-jsonld commit-data-jsonld]))

(defn nested-nodes?
  "Returns truthy if the provided node has any nested nodes."
  [node]
  (->> node
       (into []
             (comp (remove (fn [[k _]] (keyword? k)))  ; remove :id :idx :type
                   (mapcat rest)                      ; discard keys
                   (mapcat (partial remove
                                    (fn [v]
                                      ;; remove value objects unless they have type @id
                                      (and
                                       (some? (:value v))
                                       (not= (:type v) const/iri-id)))))))
       not-empty))

(defn expand-annotation
  [_parsed-txn parsed-opts context]
  (some-> (:annotation parsed-opts)
          (json-ld/expand context)
          util/sequential))

(defn validate-annotation
  [[annotation :as expanded]]
  (when-let [specified-id (:id annotation)]
    (throw (ex-info "Commit annotation cannot specify a subject identifier."
                    {:status 400, :error :db/invalid-annotation :id specified-id})))
  (when (> (count expanded) 1)
    (throw (ex-info "Commit annotation must only have a single subject."
                    {:status 400, :error :db/invalid-annotation})))
  (when (nested-nodes? annotation)
    (throw (ex-info "Commit annotation cannot reference other subjects."
                    {:status 400, :error :db/invalid-annotation})))
  expanded)

(defn extract-annotation
  [context parsed-txn parsed-opts]
  (-> parsed-txn
      (expand-annotation parsed-opts context)
      validate-annotation))

(defn stage
  ([db identity txn parsed-opts]
   (stage db nil identity txn parsed-opts))
  ([db fuel-tracker identity parsed-txn parsed-opts]
   (go-try
     (let [{:keys [context raw-txn author]} parsed-opts

           annotation (extract-annotation context parsed-txn parsed-opts)]
       (<? (-stage-txn db fuel-tracker context identity author annotation raw-txn parsed-txn))))))
