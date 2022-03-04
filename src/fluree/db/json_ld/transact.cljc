(ns fluree.db.json-ld.transact
  (:require [fluree.json-ld :as json-ld]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake #?@(:cljs [:refer [Flake]])]
            [clojure.string :as str]
            [fluree.db.util.log :as log]
            [fluree.db.json-ld.vocab :as vocab]
            [fluree.db.json-ld-db :as jlddb]
            [fluree.db.json-ld.ledger :as jld-ledger]
            [fluree.db.json-ld.reify :as jld-reify]
            #?(:clj  [clojure.core.async :refer [go <!] :as async]
               :cljs [cljs.core.async :refer [go <!] :as async]))
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

(defn add-property
  [sid property {:keys [id value] :as v-map} {:keys [iris next-pid next-sid t refs db-before new-sids] :as tx-state}]
  (let [existing-pid   (jld-reify/get-iri-sid property db-before iris)
        pid            (or existing-pid
                           (let [new-id (jld-ledger/generate-new-pid property iris next-pid id refs)]
                             (vswap! new-sids conj new-id)
                             new-id))
        property-flake (when-not existing-pid
                         (flake/->Flake pid const/$iri property t true nil))
        retractions    (when existing-pid
                         (let [existing (flake/subrange (-> db-before :novelty :spot)
                                                        >= (flake/->Flake sid pid nil nil nil nil)
                                                        <= (flake/->Flake sid pid nil nil nil nil))]
                           (mapv #(flake/flip-flake % t) existing)))
        flakes         (if id
                         (if (node? v-map)
                           (let [node-flakes (json-ld-node->flakes v-map tx-state)
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
    (cond-> flakes
            property-flake (conj property-flake)
            retractions (into retractions))))



(defn json-ld-node->flakes
  [node {:keys [t next-pid next-sid iris db-before new-sids] :as tx-state}]
  (let [id           (:id node)
        existing-sid (when id (jld-reify/get-iri-sid id db-before iris))
        sid          (or existing-sid
                         (let [new-sid (jld-ledger/generate-new-sid node iris next-pid next-sid)]
                           (vswap! new-sids conj new-sid)
                           new-sid))
        id-flake     (if (or (nil? id)
                             existing-sid
                             (str/starts-with? id "_:"))
                       []
                       [(flake/->Flake sid const/$iri id t true nil)])]
    (reduce-kv
      (fn [flakes k v]
        (case k
          (:id :idx) flakes
          :type (let [[type-sids class-flakes] (json-ld-type-data v tx-state)
                      type-flakes (map #(flake/->Flake sid const/$rdf:type % t true nil) type-sids)]
                  (into flakes (concat class-flakes type-flakes)))
          ;;else
          (if (sequential? v)
            (into flakes (mapcat #(add-property sid k % tx-state) v))
            (into flakes (add-property sid k v tx-state)))))
      id-flake node)))


(defn ->tx-state
  [db]
  (let [{:keys [t block ecount schema]} db
        last-pid (volatile! (jld-ledger/last-pid db))
        last-sid (volatile! (jld-ledger/last-sid db))]
    {:db-before db
     :refs      (volatile! (or (:refs schema) #{const/$rdf:type}))
     :t         (dec t)
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



(defn final-db
  [tx-state flakes]
  (let [{:keys [db-before t block refs]} tx-state
        {:keys [novelty schema stats]} db-before
        {:keys [spot psot opst post tspo size]} novelty
        pred-map      (:pred schema)
        bytes #?(:clj (future (flake/size-bytes flakes))    ;; calculate in separate thread for CLJ
                 :cljs (flake/size-bytes flakes))
        vocab-flakes  (jld-reify/get-vocab-flakes flakes)
        db            (assoc db-before :ecount (final-ecount tx-state)
                                       :t t
                                       :block block
                                       :novelty {:spot (into spot flakes)
                                                 :psot (into psot flakes)
                                                 :post (into post flakes)
                                                 :opst (->> flakes
                                                            (sort-by #(.-p ^Flake %))
                                                            (partition-by #(.-p ^Flake %))
                                                            (reduce
                                                              (fn [opst* p-flakes]
                                                                (if (get-in pred-map [(.-p ^Flake (first p-flakes)) :ref?])
                                                                  (into opst* p-flakes)
                                                                  opst*))
                                                              opst))
                                                 :tspo (into tspo flakes)
                                                 :size (+ size #?(:clj @bytes :cljs bytes))}
                                       :stats (-> stats
                                                  (update :size + #?(:clj @bytes :cljs bytes)) ;; total db ~size
                                                  (update :flakes + (count flakes)))
                                       :schema (vocab/update-with db-before t @refs vocab-flakes))]
    (assoc db :current-db-fn (fn [] (let [pc (async/promise-chan)]
                                      (async/put! pc db)
                                      pc)))))


(defn stage
  [db json-ld]
  (let [db*         (if (string? db)
                      (jlddb/blank-db db)
                      db)
        expanded    (json-ld/expand json-ld)
        tx-state    (->tx-state db*)
        base-flakes (cond-> (flake/sorted-set-by flake/cmp-flakes-spot)
                            (:new? tx-state) (into [(flake/->Flake const/$rdf:type const/$iri "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" (:t tx-state) true nil)
                                                    (flake/->Flake const/$rdfs:Class const/$iri "http://www.w3.org/2000/01/rdf-schema#Class" (:t tx-state) true nil)]))]
    (loop [[node & r] (if (sequential? expanded)
                        expanded
                        [expanded])
           flakes base-flakes]
      (if node
        (recur r (into flakes (json-ld-node->flakes node tx-state)))
        (let [db-after (final-db tx-state flakes)]
          db-after)))))

