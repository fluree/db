(ns fluree.db.virtual-graph.bm25.storage
  (:require [clojure.set :refer [map-invert]]
            [clojure.string :as str]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.storage :as storage]
            [fluree.db.serde :as serde]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.virtual-graph.bm25.stemmer :as stemmer]
            [fluree.db.virtual-graph.bm25.stopwords :as stopwords]
            [fluree.db.virtual-graph.parse :as parse]))

(set! *warn-on-reflection* true)

(defn compare-term-indexes
  [terms x y]
  (compare (get-in terms [x :idx])
           (get-in terms [y :idx])))

(defn serialize-terms
  [terms]
  (->> terms
       (into (sorted-map-by (partial compare-term-indexes terms)))
       keys
       vec))

(defn deserialize-terms
  [serialized-terms]
  (into {}
        (map-indexed (fn [idx term]
                       [term {:idx idx, :items #{}}]))
        serialized-terms))

(defn serialize-vectors
  [vectors]
  (map (fn [[sid v]]
         [(iri/serialize-sid sid) v])
       vectors))

(defn deserialize-vectors
  [serialized-vectors]
  (into {}
        (map (fn [[serialized-sid v]]
               [(iri/deserialize-sid serialized-sid) v]))
        serialized-vectors))

(defn serialize-state
  [index-state]
  (-> @index-state
      :index
      (select-keys [:terms :vectors :avg-length])
      (update :terms serialize-terms)
      (update :vectors serialize-vectors)))

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

(defn deserialize-avg-length
  [avg-length]
  (rationalize avg-length))

(defn deserialize-state
  [serialized-state]
  (let [term-vec   (:terms serialized-state)
        item-count (-> serialized-state :vectors count)
        index      (-> serialized-state
                       (update :terms deserialize-terms)
                       (update :vectors deserialize-vectors)
                       (update :avg-length deserialize-avg-length)
                       (assoc :dimensions (count term-vec))
                       (assoc :item-count item-count)
                       (cross-reference-items term-vec))]
    (atom {:index index})))

(defn vg-data
  [vg]
  (-> vg
      (select-keys [:k1 :b :index-state :initialized :genesis-t :t :alias :db-alias
                    :query :namespace-codes :property-deps :type :lang :id :vg-name])
      (update :index-state serialize-state)
      (update :type (partial mapv iri/serialize-sid))
      (update :property-deps (partial map iri/serialize-sid))))

(defn get-property-sids
  [namespaces props]
  (into #{}
        (map (fn [prop]
               (iri/iri->sid prop namespaces)))
        props))

(defn reconstruct
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
        (update :index-state deserialize-state))))

(defn write-vg
  [{:keys [storage serializer] :as _index-catalog} {:keys [alias db-alias] :as vg}]
  (let [data            (vg-data vg)
        serialized-data (serde/serialize-bm25 serializer data)
        path            (str/join "/" [db-alias "bm25" alias])]
    (storage/content-write-json storage path serialized-data)))

(defn read-vg
  [{:keys [storage serializer] :as _index-catalog} vg-address]
  (go-try
    (if-let [serialized-data (<? (storage/read-json storage vg-address true))]
      (let [vg-data (serde/deserialize-bm25 serializer serialized-data)]
        (reconstruct vg-data))
      (throw (ex-info (str "Could not load bm25 index at address: "
                           vg-address ".")
                      {:status 400, :error :db/unavailable})))))
