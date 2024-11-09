(ns fluree.db.transact
  (:require [fluree.db.fuel :as fuel]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.json-ld :as json-ld]
            [fluree.db.constants :as const]))

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

(defn extract-annotation
  [context parsed-txn parsed-opts]
  (let [[annotation :as expanded]
        (some-> (or (:annotation parsed-txn) (:annotation parsed-opts))
                (json-ld/expand context)
                util/sequential)]
    (when-let [specified-id (:id annotation)]
      (throw (ex-info "Commit annotation cannot specify a subject identifier."
                      {:status 400, :error :db/invalid-annotation :id specified-id})))
    (when (> (count expanded) 1)
      (throw (ex-info "Commit annotation must only have a single subject."
                      {:status 400, :error :db/invalid-annotation})))
    (when (nested-nodes? annotation)
      (throw (ex-info "Commit annotation cannot reference other subjects."
                      {:status 400, :error :db/invalid-annotation})))
    ;; everything is good
    expanded))

(defn stage
  ([db identity txn parsed-opts]
   (stage db nil identity txn parsed-opts))
  ([db fuel-tracker identity parsed-txn parsed-opts]
   (go-try
     (let [{:keys [context raw-txn author]} parsed-opts

           annotation (extract-annotation context parsed-txn parsed-opts)]
       (<? (-stage-txn db fuel-tracker context identity author annotation raw-txn parsed-txn))))))

(defn stage-triples
  "Stages a new transaction that is already parsed into the
   internal Fluree triples format."
  [db parsed-txn parsed-opts]
  (go-try
    (let [identity    (:identity parsed-opts)
          policy-db   (if (policy/policy-enforced-opts? parsed-opts)
                        (let [parsed-context (:context parsed-opts)]
                          (<? (policy/policy-enforce-db db parsed-context parsed-opts)))
                        db)]
      (if (fuel/track? parsed-opts)
        (let [start-time #?(:clj (System/nanoTime)
                            :cljs (util/current-time-millis))
              fuel-tracker       (fuel/tracker (:max-fuel parsed-opts))]
          (try*
            (let [result (<? (stage policy-db fuel-tracker identity parsed-txn parsed-opts))]
              {:status 200
               :result result
               :time   (util/response-time-formatted start-time)
               :fuel   (fuel/tally fuel-tracker)})
            (catch* e
                    (throw (ex-info "Error staging database"
                                    {:time (util/response-time-formatted start-time)
                                     :fuel (fuel/tally fuel-tracker)}
                                    e)))))
        (<? (stage policy-db identity parsed-txn parsed-opts))))))
