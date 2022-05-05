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

(declare json-ld-node->flakes)

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
  [db]
  (let [{:keys [t block ecount schema branch]} db
        last-pid (volatile! (jld-ledger/last-pid db))
        last-sid (volatile! (jld-ledger/last-sid db))
        commit-t (branch/latest-commit branch)]
    {:db-before db
     :refs      (volatile! (or (:refs schema) #{const/$rdf:type}))
     :t         (dec commit-t)
     :new?      (zero? t)
     :block     block
     :last-pid  last-pid
     :last-sid  last-sid
     :new-sids  (volatile! #{})
     :next-pid  (fn [] (vswap! last-pid inc))
     :next-sid  (fn [] (vswap! last-sid inc))
     :iris      (volatile! {})}))


(defn final-ecount
  [tx-state]
  (let [{:keys [db-before last-pid last-sid]} tx-state
        {:keys [ecount]} db-before]
    (assoc ecount const/$_predicate @last-pid
                  const/$_default @last-sid)))

(defn update-index-tt-id
  "Associates a unique tt-id for any in-memory staged db in their index roots.
  tt-id is used as part of the caching key, by having this in place it means
  that even though the 't' value hasn't changed it will cache each stage db
  data as its own entity."
  [db tt-id]
  (let [indexes [:spot :psot :post :opst :tspo]]
    (reduce
      (fn [db* idx]
        (let [{:keys [children] :as node} (get db* idx)
              children* (reduce-kv
                          (fn [children* k v]
                            (assoc children* k (assoc v :tt-id tt-id)))
                          {} children)]
          (assoc db* idx (assoc node :tt-id tt-id
                                     :children children*))))
      db indexes)))

(defn update-novelty-idx*
  "Updates a specific index flakes with new flakes"
  [existing-flakes new-flakes stage-update?]
  (if stage-update?
    (reduce
      (fn [acc flake]
        (if (false? (flake/op flake))
          (disj acc (flake/flip-flake flake))
          (conj acc flake)))
      existing-flakes new-flakes)
    (into existing-flakes new-flakes)))


(defn update-novelty-idx
  "Updates all novelty values.

  If this is a staged update, it removes any assertions corresponding to retractions
  that occured in the same 't' value (multiple stages between commits), effectively
  'squashing' multiple stages.

  opst flakes only are 'refs'."
  [{:keys [spot psot opst post tspo size]} new-flakes schema stage-update?]
  (let [spot*       (update-novelty-idx* spot new-flakes stage-update?)
        bytes       (if stage-update?                       ;; for staged updates, need to re-calc novelty size
                      #?(:clj  (future (flake/size-bytes spot*)) ;; calculate in separate thread for CLJ
                         :cljs (flake/size-bytes spot*))
                      #?(:clj  (future (flake/size-bytes new-flakes)) ;; calculate in separate thread for CLJ
                         :cljs (flake/size-bytes new-flakes)))
        opst-flakes (->> new-flakes
                         (sort-by flake/p)
                         (partition-by flake/p)
                         (reduce
                           (fn [acc p-flakes]
                             (if (get-in schema [:pred (flake/p (first p-flakes)) :ref?])
                               (into acc p-flakes)
                               acc))
                           []))]
    {:spot spot*
     :psot (update-novelty-idx* psot new-flakes stage-update?)
     :opst (update-novelty-idx* opst opst-flakes stage-update?)
     :post (update-novelty-idx* post new-flakes stage-update?)
     :tspo (update-novelty-idx* tspo new-flakes stage-update?)
     :size (if stage-update?
             #?(:clj @bytes :cljs bytes)
             (+ size #?(:clj @bytes :cljs bytes)))}))

(defn final-db
  [tx-state flakes]
  (let [{:keys [db-before t block refs]} tx-state
        {:keys [novelty stats]} db-before
        vocab-flakes  (jld-reify/get-vocab-flakes flakes)
        schema*       (vocab/update-with db-before t @refs vocab-flakes)
        tt-id         (util/random-uuid)
        stage-update? (= t (:t db-before))                  ;; if a previously staged db is getting updated again before committed
        novelty*      (update-novelty-idx novelty flakes schema* stage-update?)
        db            (-> db-before
                          (update-index-tt-id tt-id)
                          (assoc :ecount (final-ecount tx-state)
                                 :t t
                                 :tt-id tt-id
                                 :block block
                                 :novelty novelty*
                                 :stats (-> stats
                                            (update :size + (- (:size novelty*) (:size novelty)))
                                            (update :flakes + (- (count (:spot novelty*))
                                                                 (count (:spot novelty)))))
                                 :schema schema*))]
    (assoc db :current-db-fn (fn [] (let [pc (async/promise-chan)]
                                      (async/put! pc db)
                                      pc)))))


(defn stage
  "Stages changes, but does not commit.
  Returns promise with new db."
  [db json-ld opts]
  (async/go
    (try*
      (let [expanded    (json-ld/expand json-ld)
            tx-state    (->tx-state db)
            base-flakes (cond-> (flake/sorted-set-by flake/cmp-flakes-spot)
                                (:new? tx-state) (into [(flake/->Flake const/$rdf:type const/$iri "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" (:t tx-state) true nil)
                                                        (flake/->Flake const/$rdfs:Class const/$iri "http://www.w3.org/2000/01/rdf-schema#Class" (:t tx-state) true nil)]))]
        (loop [[node & r] (if (sequential? expanded)
                            expanded
                            [expanded])
               flakes base-flakes]
          (if node
            (recur r (into flakes (<? (json-ld-node->flakes node tx-state))))
            (final-db tx-state flakes))))
      (catch* e e))))

