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

(defn remove-cancelable-flakes
  "If an identical flake is retracted and re-asserted, removes both flakes as they can be cancelled out.
   If not removed, the replay of flakes to a time 't' can result in flakes not appearing properly."
  [flakeset retractions]
  (reduce (fn [s r]
            (let [a (flake/flip-flake r)]
              (if (contains? s a)
                (-> s
                    (disj a)
                    (disj r))
                s)))
          flakeset
          retractions))

(defn into-flakeset
  [tracker error-ch flake-ch]
  (let [flakeset    (flake/sorted-set-by flake/cmp-flakes-spot)
        error-xf    (halt-when util/exception?)
        flake-xf    (if-let [track-fuel (track/track-fuel! tracker error-ch)]
                      (comp error-xf track-fuel)
                      error-xf)
        retractions (volatile! [])]
    (async/transduce
     flake-xf
     (completing
      (fn [acc f]
        (when (false? (flake/op f))
          (vswap! retractions conj f))
        (conj acc f))
      (fn [flakeset]
        (if (seq @retractions)
          (remove-cancelable-flakes flakeset @retractions)
          flakeset)))
     flakeset
     flake-ch)))

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

(defn max-novelty-error
  "Returns an ExceptionInfo for max novelty exceeded with MBs in message."
  [db]
  (let [novelty-bytes     (long (get-in db [:novelty :size] 0))
        max-novelty-bytes (long (:reindex-max-bytes db))
        round2-mb         (fn [bytes]
                            (let [mb (/ (double bytes) 1000000.0)]
                              (/ (double (int (+ 0.5 (* mb 100.0)))) 100.0)))
        novelty-mb-r      (round2-mb novelty-bytes)
        max-novelty-mb-r  (round2-mb max-novelty-bytes)
        msg               (str "Maximum novelty exceeded ("
                               novelty-mb-r " MB > max " max-novelty-mb-r
                               " MB). No transactions will be processed until indexing has completed.")]
    (ex-info msg {:status 503, :error :db/max-novelty-exceeded})))

(defn stage
  [db tracker context identity author annotation raw-txn parsed-txn]
  (go-try
    (when (novelty/max-novelty? db)
      (throw (max-novelty-error db)))
    (when (policy.modify/deny-all? db)
      (throw (ex-info "Database policy denies all modifications."
                      {:status 403, :error :db/policy-exception})))
    (let [tx-state   (->tx-state :db db
                                 :context context
                                 :txn raw-txn
                                 :author (or author identity)
                                 :annotation annotation)
          [db** new-flakes] (<? (generate-flakes db tracker parsed-txn tx-state))
          staged-map (<? (final-db db** new-flakes tx-state))]
      (<? (validate-db-update tracker staged-map)))))
