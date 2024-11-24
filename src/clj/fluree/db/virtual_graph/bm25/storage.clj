(ns fluree.db.virtual-graph.bm25.storage
  (:require [fluree.db.json-ld.iri :as iri]
            [fluree.db.virtual-graph.bm25.stemmer :as stemmer]
            [fluree.db.virtual-graph.bm25.stopwords :as stopwords]))

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
  (let [term-vec     (:terms serialized-state)
        item-count   (-> serialized-state :vectors count)]
    (-> serialized-state
        (update :terms deserialize-terms)
        (update :vectors deserialize-vectors)
        (update :avg-length deserialize-avg-length)
        (assoc :demensions (count term-vec))
        (assoc :item-count item-count)
        (cross-reference-items term-vec)
        atom)))

(defn serialize
  [{:keys [lang] :as vg}]
  (-> vg
      (select-keys [:k1 :b :index-state :initialized :genesis-t :t :alias :db-alias
                    :query :property-deps :namespace-codes :type :lang :id :vg-name])
      (update :index-state serialize-state)
      (update :property-deps (partial map iri/serialize-sid))))

(defn deserialize
  [{:keys [lang] :as serialized-vg}]
  (-> serialized-vg
      (update :index-state deserialize-state)
      (update :property-deps (partial map iri/deserialize-sid))
      (assoc :stemmer (stemmer/initialize lang))
      (assoc :stopwords (stopwords/initialize lang))))
