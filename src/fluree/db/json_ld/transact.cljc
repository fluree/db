(ns fluree.db.json-ld.transact
  (:require [fluree.json-ld :as json-ld]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake #?@(:cljs [:refer [Flake]])]
            [clojure.string :as str]
            [fluree.db.json-ld.vocab :as vocab]
            [fluree.db.json-ld.ledger :as jld-ledger]
            [fluree.db.json-ld.reify :as jld-reify]
            [fluree.db.util.async :refer [<? go-try channel?]]
            #?(:clj  [clojure.core.async :refer [go <!] :as async]
               :cljs [cljs.core.async :refer [go <!] :as async])
            [fluree.db.query.range :as query-range]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log]
            [fluree.db.json-ld.branch :as branch])
  #?(:clj (:import (fluree.db.flake Flake))))

#?(:clj (set! *warn-on-reflection* true))

(declare json-ld-node->flakes)

(defn node?
  "Returns true if a nested value is itself another node in the graph.
  Only need to test maps that have :id - and if they have other properties they
  are defining then we know it is a node and have additional data to include."
  [mapx]
  (and (contains? mapx :id)
       (> (count mapx) 1)))


(defn json-ld-type-data
  "Returns two-tuple of [class-subject-ids class-flakes]
  where class-flakes will only contain newly generated class
  flakes if they didn't already exist."
  [class-iris {:keys [t next-pid iris] :as tx-state}]
  (loop [[class-iri & r] class-iris
         class-sids   []
         class-flakes []]
    (if class-iri
      (if-let [existing (get @iris class-iri)]
        (recur r (conj class-sids existing) class-flakes)
        (let [type-sid (if-let [predefined-pid (get jld-ledger/predefined-properties class-iri)]
                         predefined-pid
                         (next-pid))]
          (vswap! iris assoc class-iri type-sid)
          (recur r
                 (conj class-sids type-sid)
                 (into class-flakes
                       [(flake/->Flake type-sid const/$iri class-iri t true nil)
                        (flake/->Flake type-sid const/$rdf:type const/$rdfs:Class t true nil)]))))
      [class-sids class-flakes])))

(defn process-retractions
  "Processes all retractions at once from set of [sid pid] registered
  in retractions volatile! while creating new flakes."
  [{:keys [db-before retractions t] :as tx-state}]
  (go-try
    (loop [acc []
           [[sid pid] & r] @retractions]
      (if sid
        (->> (<? (query-range/index-range db-before :spot = [sid pid]))
             (map #(flake/flip-flake % t))
             (recur r))
        acc))))

(defn- newly-added?
  "Returns true if provided sid is newly added during this staging/transaction,
  meaning it did not exist in the db-before.

  Takes sid to check, and @new-sids volatile used in the tx-state."
  [sid new-sids]
  (contains? @new-sids sid))

(defn- new-pid
  "Generates a new property id (pid)"
  [property ref? {:keys [iris new-sids next-pid refs] :as tx-state}]
  (let [new-id (jld-ledger/generate-new-pid property iris next-pid ref? refs)]
    (vswap! new-sids conj new-id)
    new-id))

(defn add-property
  "Adds property. Parameters"
  [sid new-sid? property {:keys [id value] :as v-map}
   {:keys [iris next-sid t db-before new-sids] :as tx-state}]
  (go-try
    (let [ref?           (boolean id)                       ;; either a ref or a value
          existing-pid   (jld-reify/get-iri-sid property db-before iris)
          pid            (or existing-pid
                             (get jld-ledger/predefined-properties property)
                             (new-pid property ref? tx-state))
          property-flake (when-not existing-pid
                           (flake/->Flake pid const/$iri property t true nil))
          ;; only process retractions if the pid existed previously (in the db-before)
          retractions    (when (and (not new-sid?)          ;; don't need to check if sid is new
                                    existing-pid            ;; don't need to check if just generated pid
                                    (not (newly-added? existing-pid new-sids))) ;; don't need to check if generated pid during this transaction
                           (->> (<? (query-range/index-range db-before :spot = [sid pid]))
                                (map #(flake/flip-flake % t))))
          flakes         (if ref?
                           (if (node? v-map)
                             (let [node-flakes (<? (json-ld-node->flakes v-map tx-state))
                                   node-sid    (get @iris id)]
                               (conj node-flakes (flake/->Flake sid pid node-sid t true nil)))
                             (let [[id-sid id-flake] (if-let [existing (get @iris id)]
                                                       [existing nil]
                                                       (let [id-sid (next-sid)]
                                                         (vswap! iris assoc id id-sid)
                                                         (if (str/starts-with? id "_:") ;; blank node
                                                           [id-sid nil]
                                                           [id-sid (flake/->Flake id-sid const/$iri id t true nil)])))]
                               (cond-> [(flake/->Flake sid pid id-sid t true nil)]
                                       id-flake (conj id-flake))))
                           [(flake/->Flake sid pid value t true nil)])]
      (cond-> (into flakes retractions)
              property-flake (conj property-flake)))))

(defn json-ld-node->flakes
  [{:keys [id] :as node}
   {:keys [t next-pid next-sid iris db-before new-sids] :as tx-state}]
  (go-try
    (let [existing-sid (when id (jld-reify/get-iri-sid id db-before iris))
          new?         (not existing-sid)
          sid          (if new?
                         (let [new-sid (jld-ledger/generate-new-sid node iris next-pid next-sid)]
                           (vswap! new-sids conj new-sid)
                           new-sid)
                         existing-sid)
          id*          (if (and new? (nil? id))
                         (str "_:f" sid)                    ;; create a blank node id
                         id)
          id-flake     (if new?
                         [(flake/->Flake sid const/$iri id* t true nil)]
                         [])]
      (loop [[[k v] & r] node
             flakes id-flake]
        (if k
          (recur r
                 (case k
                   (:id :idx) flakes
                   :type (let [[type-sids class-flakes] (json-ld-type-data v tx-state)
                               type-flakes (map #(flake/->Flake sid const/$rdf:type % t true nil) type-sids)]
                           (into flakes (concat class-flakes type-flakes)))
                   ;;else
                   (loop [[v* & r] (if (sequential? v) v [v])
                          flakes* flakes]
                     (if v*
                       (recur r (into flakes* (<? (add-property sid new? k v* tx-state))))
                       flakes*))))
          flakes)))))

(defn ->tx-state
  [db {:keys [bootstrap?] :as _opts}]
  (let [{:keys [block ecount schema branch]} db
        last-pid (volatile! (jld-ledger/last-pid db))
        last-sid (volatile! (jld-ledger/last-sid db))
        commit-t (branch/latest-commit branch)
        t        (if bootstrap?
                   (if (not (zero? commit-t))
                     (throw (ex-info (str "Bootstrap? option only available for db-t value of 0, t is: " commit-t ".")
                                     {:status 500 :error :db/invalid-commit}))
                     0)
                   (dec commit-t))]
    {:db-before     db
     :bootstrap?    bootstrap?
     :stage-update? (= t (:t db))                           ;; if a previously staged db is getting updated again before committed
     :refs          (volatile! (or (:refs schema) #{const/$rdf:type}))
     :t             t
     :new?          (zero? t)
     :block         block
     :last-pid      last-pid
     :last-sid      last-sid
     :new-sids      (volatile! #{})
     :next-pid      (fn [] (vswap! last-pid inc))
     :next-sid      (fn [] (vswap! last-sid inc))
     :iris          (volatile! {})}))

(defn final-ecount
  [tx-state]
  (let [{:keys [db-before last-pid last-sid]} tx-state
        {:keys [ecount]} db-before]
    (assoc ecount const/$_predicate @last-pid
                  const/$_default @last-sid)))

(defn add-tt-id
  "Associates a unique tt-id for any in-memory staged db in their index roots.
  tt-id is used as part of the caching key, by having this in place it means
  that even though the 't' value hasn't changed it will cache each stage db
  data as its own entity."
  [db]
  (let [tt-id   (util/random-uuid)
        indexes [:spot :psot :post :opst :tspo]]
    (-> (reduce
          (fn [db* idx]
            (let [{:keys [children] :as node} (get db* idx)
                  children* (reduce-kv
                              (fn [children* k v]
                                (assoc children* k (assoc v :tt-id tt-id)))
                              {} children)]
              (assoc db* idx (assoc node :tt-id tt-id
                                         :children children*))))
          db indexes)
        (assoc :tt-id tt-id))))

(defn remove-tt-id
  "Removes a tt-id placed on indexes (opposite of add-tt-id)."
  [db]
  (let [indexes [:spot :psot :post :opst :tspo]]
    (reduce
      (fn [db* idx]
        (let [{:keys [children] :as node} (get db* idx)
              children* (reduce-kv
                          (fn [children* k v]
                            (assoc children* k (dissoc v :tt-id)))
                          {} children)]
          (assoc db* idx (-> node
                             (dissoc :tt-id)
                             (assoc :children children*)))))
      (dissoc db :tt-id) indexes)))

(defn update-novelty-idx
  [novelty-idx add remove]
  (-> (reduce disj novelty-idx remove)
      (into add)))

(defn final-db
  [{:keys [add remove ref-add ref-remove size count schema] :as staged} {:keys [db-before bootstrap? t block] :as tx-state}]
  (let [{:keys [novelty]} db-before
        {:keys [spot psot post opst tspo]} novelty
        new-db (assoc db-before :ecount (final-ecount tx-state)
                                :t t
                                :block block
                                :novelty {:spot (update-novelty-idx spot add remove)
                                          :psot (update-novelty-idx psot add remove)
                                          :post (update-novelty-idx post add remove)
                                          :opst (update-novelty-idx opst ref-add ref-remove)
                                          :tspo (update-novelty-idx tspo add remove)
                                          :size (+ (:size novelty) size)}
                                :stats (-> (:stats db-before)
                                           (update :size + size)
                                           (update :flakes + count))
                                :schema schema)]
    (if bootstrap?
      new-db
      (add-tt-id new-db))))

(defn base-flakes
  "Returns base set of flakes needed in any new ledger."
  [t]
  [(flake/->Flake const/$rdf:type const/$iri "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" t true nil)
   (flake/->Flake const/$rdfs:Class const/$iri "http://www.w3.org/2000/01/rdf-schema#Class" t true nil)])

(defn ref-flakes
  "Returns ref flakes from set of all flakes"
  [flakes schema]
  (->> flakes
       (sort-by flake/p)
       (partition-by flake/p)
       (reduce
         (fn [acc p-flakes]
           (if (get-in schema [:pred (flake/p (first p-flakes)) :ref?])
             (into acc p-flakes)
             acc))
         [])))

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

(defn stage*
  "Returns map of all elements for a stage transaction required to create an updated db."
  [new-flakes {:keys [t stage-update? db-before refs] :as _tx-state}]
  (let [[add remove] (if stage-update?
                       (stage-update-novelty (get-in db-before [:novelty :spot]) new-flakes)
                       [new-flakes nil])
        vocab-flakes (jld-reify/get-vocab-flakes new-flakes)
        schema       (vocab/update-with db-before t @refs vocab-flakes)]
    {:add        add
     :remove     remove
     :ref-add    (ref-flakes add schema)
     :ref-remove (ref-flakes remove schema)
     :count      (cond-> (when add (count add))
                         remove (- (count remove)))
     :size       (cond-> (flake/size-bytes add)
                         remove (- (flake/size-bytes remove)))
     :schema     schema}))

(defn stage-flakes
  [json-ld {:keys [new? t] :as tx-state}]
  (go-try
    (let [ss (cond-> (flake/sorted-set-by flake/cmp-flakes-spot)
                     new? (into (base-flakes t)))]
      (loop [[node & r] (if (sequential? json-ld)
                          json-ld
                          [json-ld])
             flakes* ss]
        (if node
          (recur r (into flakes* (<? (json-ld-node->flakes node tx-state))))
          flakes*)))))

(defn stage
  "Stages changes, but does not commit.
  Returns async channel that will contain updated db or exception."
  [db json-ld opts]
  (go-try
    (let [tx-state (->tx-state db opts)]
      (-> json-ld
          json-ld/expand
          (stage-flakes tx-state)
          <?
          (stage* tx-state)
          (final-db tx-state)))))

