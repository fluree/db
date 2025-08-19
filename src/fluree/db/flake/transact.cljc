(ns fluree.db.flake.transact
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.db.flake :as flake]
            [fluree.db.flake.commit-data :as commit-data]
            [fluree.db.flake.index.novelty :as novelty]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.json-ld.policy.modify :as policy.modify]
            [fluree.db.json-ld.shacl :as shacl]
            [fluree.db.json-ld.vocab :as vocab]
            [fluree.db.query.exec.update :as update]
            [fluree.db.query.exec.where :as where]
            [fluree.db.track :as track]
            [fluree.db.util :as util]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.virtual-graph.index-graph :as vg]))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol Transactable
  (-stage-txn [db tracker context identity author annotation raw-txn parsed-txn])
  (-merge-commit [db commit-jsonld commit-data-jsonld]))

;; TODO - can use transient! below
(defn stage-update-novelty
  "If a db is staged more than once, any retractions in a previous stage will
  get completely removed from novelty. This returns flakes that must be added and removed
  from novelty."
  [novelty-flakes new-flakes]
  (loop [[f & r] new-flakes
         adds    new-flakes
         removes (empty new-flakes)]
    (if f
      (if (true? (flake/op f))
        (recur r adds removes)
        (let [flipped (flake/flip-flake f)]
          (if (contains? novelty-flakes flipped)
            (recur r (disj adds f) (conj removes flipped))
            (recur r adds removes))))
      [(not-empty adds) (not-empty removes)])))

(defn ->tx-state
  "Generates a state map for transaction processing. When optional
  reasoned-from-IRI is provided, will mark any new flakes as reasoned from the
  provided value in the flake's metadata (.-m) as :reasoned key."
  [& {:keys [db context txn author annotation reasoned-from-iri]}]
  (let [{:keys [policy], db-t :t} db

        commit-t  (-> db :commit commit-data/t)
        t         (flake/next-t commit-t)
        db-before (policy/root db)]
    {:db-before     db-before
     :context       context
     :txn           txn
     :annotation    annotation
     :author        author
     :policy        policy
     :stage-update? (= t db-t) ; if a previously staged db is getting updated again before committed
     :t             t
     :reasoner-max  10 ; maximum number of reasoner iterations before exception
     :reasoned      reasoned-from-iri}))

(defn into-flakeset
  [tracker error-ch flake-ch]
  (let [flakeset (flake/sorted-set-by flake/cmp-flakes-spot)
        error-xf (halt-when util/exception?)
        flake-xf (if-let [track-fuel (track/track-fuel! tracker error-ch)]
                   (comp error-xf track-fuel)
                   error-xf)]
    (async/transduce flake-xf (completing conj) flakeset flake-ch)))

(defn generate-flakes
  [db tracker parsed-txn tx-state]
  (go
    (let [error-ch  (async/chan)
          db-vol    (volatile! db)
          update-ch (->> (where/search db parsed-txn tracker error-ch)
                         (update/modify db-vol parsed-txn tx-state tracker error-ch)
                         (into-flakeset tracker error-ch))]
      (async/alt!
        error-ch ([e] e)
        update-ch ([result]
                   (if (util/exception? result)
                     result
                     [@db-vol result]))))))

(defn final-db
  "Returns map of all elements for a stage transaction required to create an
  updated db."
  [db new-flakes {:keys [stage-update? policy t txn author annotation db-before context] :as _tx-state}]
  (go-try
    (let [[add remove] (if stage-update?
                         (stage-update-novelty (get-in db [:novelty :spot]) new-flakes)
                         [new-flakes nil])
          db-after     (-> db
                           (assoc :t t
                                  :staged {:txn txn, :author author, :annotation annotation}
                                  :policy policy) ; re-apply policy to db-after
                           (commit-data/update-novelty add remove)
                           (commit-data/add-tt-id)
                           (vocab/hydrate-schema add)
                           (vg/check-virtual-graph add remove))]
      {:add       add
       :remove    remove
       :db-after  db-after
       :db-before db-before
       :context   context})))

(defn validate-db-update
  [tracker {:keys [db-after add context] :as staged-map}]
  (go-try
    (<? (shacl/validate! (policy/root db-after) tracker add context))
    (let [allowed-db (<? (policy.modify/allowed? tracker staged-map))]
      allowed-db)))

(defn stage
  [db tracker context identity author annotation raw-txn parsed-txn]
  (go-try
    (when (novelty/max-novelty? db)
      (throw (ex-info "Maximum novelty exceeded, no transactions will be processed until indexing has completed."
                      {:status 503 :error :db/max-novelty-exceeded})))
    (when (policy.modify/deny-all? db)
      (throw (ex-info "Database policy denies all modifications."
                      {:status 403 :error :db/policy-exception})))
    (let [tx-state   (->tx-state :db db
                                 :context context
                                 :txn raw-txn
                                 :author (or author identity)
                                 :annotation annotation)
          [db** new-flakes] (<? (generate-flakes db tracker parsed-txn tx-state))
          staged-map (<? (final-db db** new-flakes tx-state))]
      (<? (validate-db-update tracker staged-map)))))
