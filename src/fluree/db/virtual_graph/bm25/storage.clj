(ns fluree.db.virtual-graph.bm25.storage
  (:require [clojure.set :refer [map-invert]]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.serde.protocol :as serde]
            [fluree.db.storage :as storage]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.virtual-graph :as vg]
            [fluree.db.virtual-graph.bm25.index :as bm25]
            [fluree.db.virtual-graph.bm25.stemmer :as stemmer]
            [fluree.db.virtual-graph.bm25.stopwords :as stopwords]
            [fluree.db.virtual-graph.parse :as parse]))

(set! *warn-on-reflection* true)

(defn compare-term-indexes
  [terms x y]
  (compare (get-in terms [x :idx])
           (get-in terms [y :idx])))

(defn term-data
  [terms]
  (->> terms
       (into (sorted-map-by (partial compare-term-indexes terms)))
       keys
       vec))

(defn reify-terms
  [term-data]
  (into {}
        (map-indexed (fn [idx term]
                       [term {:idx idx, :items #{}}]))
        term-data))

(defn vector-data
  [vectors]
  (map (fn [[sid v]]
         [(iri/serialize-sid sid) v])
       vectors))

(defn reify-vectors
  [vector-data]
  (into {}
        (map (fn [[serialized-sid v]]
               [(iri/deserialize-sid serialized-sid) v]))
        vector-data))

(defn state-data
  [index-state]
  (-> @index-state
      :index
      (select-keys [:terms :vectors :avg-length])
      (update :terms term-data)
      (update :vectors vector-data)))

(defn cross-reference-item
  [term-map term-vec sid v]
  (reduce (fn [m [idx _]]
            (let [term (get term-vec idx)]
              (update-in m [term :items] conj sid)))
          term-map v))

(defn cross-reference-items
  [{:keys [terms vectors] :as state} term-vec]
  (let [terms* (reduce-kv (fn [m sid v]
                            (cross-reference-item m term-vec sid v))
                          terms vectors)]
    (assoc state :terms terms*)))

(defn coerce-double
  [n]
  (double n))

(defn reify-state
  [state-data]
  (let [term-vec   (:terms state-data)
        item-count (-> state-data :vectors count)
        index      (-> state-data
                       (assoc :dimensions (count term-vec)
                              :item-count item-count)
                       (update :terms reify-terms)
                       (update :vectors reify-vectors)
                       (update :avg-length rationalize)
                       (cross-reference-items term-vec))]
    (atom {:index index
           :pending-ch nil
           :pending-status nil})))

(defn vg-data
  [vg]
  (-> vg
      (select-keys [:k1 :b :index-state :initialized :genesis-t :t :alias :db-alias
                    :query :namespace-codes :property-deps :type :lang :id :vg-name])
      (update :index-state state-data)
      (update :type (partial mapv iri/serialize-sid))
      (update :property-deps (partial map iri/serialize-sid))))

(defn get-property-sids
  [namespaces props]
  (into #{}
        (map (fn [prop]
               (iri/iri->sid prop namespaces)))
        props))

(defn reify-bm25
  [{:keys [lang query namespace-codes] :as vg-data}]
  (let [parsed-query (parse/parse-query query)
        namespaces   (map-invert namespace-codes)
        query-props  (parse/get-query-props parsed-query)
        property-deps (get-property-sids namespaces query-props)]
    (-> vg-data
        (assoc :parsed-query parsed-query)
        (assoc :namespaces namespaces)
        (assoc :property-deps property-deps)
        (assoc :stemmer (stemmer/initialize lang))
        (assoc :stopwords (stopwords/initialize lang))
        (update :type (partial mapv iri/deserialize-sid))
        (update :index-state reify-state)
        (update :b coerce-double)
        (update :k1 coerce-double))))

(defmethod vg/write-vg :bm25
  [{:keys [storage serializer] :as _index-catalog} {:keys [alias db-alias] :as vg}]
  (go-try
    (let [data            (vg-data vg)
          serialized-data (serde/-serialize-bm25 serializer data)
          ;; Extract base ledger name for content-addressed storage
          ledger-name     (util.ledger/ledger-base-name db-alias)
          path            (vg/storage-path :bm25 ledger-name (vg/trim-alias-ref alias))
          write-res       (<? (storage/content-write-json storage path serialized-data))]
      (assoc write-res :type "bm25"))))

(defmethod vg/read-vg :bm25
  [{:keys [storage serializer] :as _index-catalog} {:keys [address] :as _vg-meta}]
  (go-try
    (if-let [serialized-data (<? (storage/read-json storage address false))]
      (-> (serde/-deserialize-bm25 serializer serialized-data)
          reify-bm25
          bm25/map->BM25-VirtualGraph)
      (throw (ex-info (str "Could not load bm25 index at address: "
                           address ".")
                      {:status 400, :error :db/unavailable})))))
