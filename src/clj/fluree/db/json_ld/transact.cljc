(ns fluree.db.json-ld.transact
  (:require [fluree.db.json-ld.policy :as perm]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util]
            [fluree.json-ld :as json-ld]))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol Transactable
  (-stage-db [db fuel-tracker context identity annotation raw-txn parsed-txn]))

(defn nested-nodes?
  "Returns truthy if the provided node has any nested nodes."
  [node]
  (->> node
       (into []
             (comp (remove (fn [[k _]] (keyword? k)))  ; remove :id :idx :type
                   (mapcat rest)                      ; discard keys
                   (mapcat (partial remove :value)))) ; remove value objects
       not-empty))

(defn extract-annotation
  [context parsed-txn parsed-opts]
  (let [[annotation :as expanded]
        (some-> (or (:annotation parsed-txn) (:annotation parsed-opts))
                (json-ld/expand context)
                util/sequential)]
    (when-let [specified-id (:id annotation)]
      (throw (ex-info "Commit annotation cannot specify a subject identifier."
                      {:status 400, :error :db/invalid-annotation :id specified-id})))
    (when (or (> (count expanded) 1)
              (nested-nodes? annotation))
      (throw (ex-info "Commit annotation must only have a single subject."
                      {:status 400, :error :db/invalid-annotation})))
    ;; everything is good
    expanded))

(defn stage
  ([db txn parsed-opts]
   (stage db nil txn parsed-opts))
  ([db fuel-tracker parsed-txn parsed-opts]
   (go-try
     (let [{:keys [context raw-txn]} parsed-opts

           identity   (perm/parse-policy-identity parsed-opts context)
           annotation (extract-annotation context parsed-txn parsed-opts)]
       (<? (-stage-db db fuel-tracker context identity annotation raw-txn parsed-txn))))))
