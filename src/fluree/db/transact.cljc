(ns fluree.db.transact
  (:require [fluree.db.constants :as const]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.track :as track]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.trace :as trace]
            [fluree.json-ld :as json-ld]))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol Transactable
  (-stage-txn [db tracker context identity author annotation raw-txn parsed-txn])
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
                                      (and (some? (util/get-value v))
                                           (not= (util/get-types v) const/iri-id)))))))
       not-empty))

(defn expand-annotation
  [_parsed-txn parsed-opts context]
  (some-> (:annotation parsed-opts)
          (json-ld/expand context)
          util/sequential))

(defn validate-annotation
  [[annotation :as expanded]]
  (when-let [specified-id (util/get-id annotation)]
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
  ([db tracker identity parsed-txn parsed-opts]
   (go-try
     (let [{:keys [context raw-txn author]} parsed-opts

           annotation (extract-annotation context parsed-txn parsed-opts)]
       (<? (-stage-txn db tracker context identity author annotation raw-txn parsed-txn))))))

(defn stage-triples
  "Stages a new transaction that is already parsed into the
   internal Fluree triples format."
  [db parsed-txn]
  (trace/async-form ::stage-triples {}
    (go-try
      (let [parsed-opts    (:opts parsed-txn)
            parsed-context (:context parsed-opts)
            identity       (:identity parsed-opts)]
        (if (track/track-txn? parsed-opts)
          (let [tracker   (track/init parsed-opts)
                policy-db (if (policy/policy-enforced-opts? parsed-opts)
                            (<? (policy/policy-enforce-db db tracker parsed-context parsed-opts))
                            db)]
            (track/register-policies! tracker policy-db)
            (try*
              (let [staged-db (<? (stage policy-db tracker identity parsed-txn parsed-opts))
                    tally     (track/tally tracker)]
                (assoc tally :status 200, :db staged-db))
              (catch* e
                (throw (ex-info (ex-message e)
                                (let [tally (track/tally tracker)]
                                  (merge (ex-data e) tally))
                                e)))))
          (let [policy-db (if (policy/policy-enforced-opts? parsed-opts)
                            (<? (policy/policy-enforce-db db parsed-context parsed-opts))
                            db)]
            (<? (stage policy-db identity parsed-txn parsed-opts))))))))
