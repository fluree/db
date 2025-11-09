(ns fluree.db.flake.index.storage
  (:require [clojure.core.async :as async]
            [clojure.set :refer [map-invert]]
            [clojure.string :as str]
            [fluree.db.cache :as cache]
            [fluree.db.flake :as flake]
            [fluree.db.flake.index :as index]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.json-ld.vocab :as vocab]
            [fluree.db.serde :as serde]
            [fluree.db.storage :as storage]
            [fluree.db.util :as util]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.virtual-graph :as vg]))

#?(:clj (set! *warn-on-reflection* true))

(defrecord IndexStore [storage serializer cache])

(defn index-catalog
  [storage serializer cache]
  (->IndexStore storage serializer cache))

(defn ledger-garbage-prefix
  [ledger-alias]
  (str/join "_" [ledger-alias "garbage"]))

(defn ledger-garbage-key
  [ledger-alias t]
  (let [pre (ledger-garbage-prefix ledger-alias)]
    (str/join "_" [pre t])))

(defn child-data
  "Given a child, unresolved node, extracts just the data that will go into
  storage."
  [child]
  (select-keys child [:id :leaf :first :rhs :size :leftmost?]))

(defn write-index-file
  [storage ledger-alias index-type serialized-data]
  (let [ledger-name (util.ledger/ledger-base-name ledger-alias)
        index-name  (name index-type)
        path        (str/join "/" [ledger-name "index" index-name])]
    (storage/content-write-json storage path serialized-data)))

(defn write-leaf
  "Serializes and writes the index leaf node `leaf` to storage."
  [{:keys [storage serializer] :as _index-catalog} ledger-alias idx-type leaf]
  (let [serialized (serde/-serialize-leaf serializer leaf)]
    (write-index-file storage ledger-alias idx-type serialized)))

(defn write-branch-data
  "Serializes final data for branch and writes it to provided key.
  Returns two-tuple of response output and raw bytes written."
  [{:keys [storage serializer] :as _index-catalog} ledger-alias idx-type data]
  (let [serialized (serde/-serialize-branch serializer data)]
    (write-index-file storage ledger-alias idx-type serialized)))

(defn write-branch
  "Writes the child attributes index branch node `branch` to storage."
  [index-catalog ledger-alias idx-type {:keys [children] :as _branch}]
  (let [child-vals (->> children
                        (map val)
                        (mapv child-data))
        data       {:children child-vals}]
    (write-branch-data index-catalog ledger-alias idx-type data)))

(defn write-garbage
  "Writes garbage record out for latest index."
  [{:keys [storage serializer] :as _index-catalog} ledger-alias t garbage]
  (let [data       {:alias   ledger-alias
                    :t       t
                    :garbage garbage}
        serialized (serde/-serialize-garbage serializer data)]
    (write-index-file storage ledger-alias :garbage serialized)))

(defn write-vg-map
  [index-catalog vg-map]
  (go-try
    (loop [[[vg-alias vg] & r] vg-map
           address-map         {}]
      (if vg-alias
        (let [write-resp (-> (<? (vg/write-vg index-catalog vg))
                             (select-keys [:address :size :type]))]
          (recur r (assoc address-map vg-alias write-resp)))
        address-map))))

(defn write-db-root
  [{:keys [storage serializer] :as index-catalog} db garbage-addr]
  (go-try
    (let [{:keys [alias schema t stats spot post opst tspo vg commit namespace-codes
                  reindex-min-bytes reindex-max-bytes max-old-indexes]}
          db

          prev-idx-t    (-> commit :index :data :t)
          prev-idx-addr (-> commit :index :address)
          prev-idx-v    (-> commit :index :v)

          ;; Version logic:
          ;; - New ledgers (no previous index) use v2
          ;; - Existing ledgers preserve their version (or default to v1 for legacy)
          version       (if prev-idx-t
                          (or prev-idx-v 1)
                          2)

          stats-data    (cond-> (select-keys stats [:flakes :size])
                          ;; HLL-based stats (properties, classes) are only for v2 indexes
                          (= 2 version) (merge (select-keys stats [:properties :classes])))

          vg-addresses  (<? (write-vg-map index-catalog vg))
          data          (cond-> {:ledger-alias alias
                                 :t               t
                                 :v               version
                                 :schema          (vocab/serialize-schema schema)
                                 :stats           stats-data
                                 :spot            (child-data spot)
                                 :post            (child-data post)
                                 :opst            (child-data opst)
                                 :tspo            (child-data tspo)
                                 :vg              vg-addresses
                                 :timestamp       (util/current-time-millis)
                                 :namespace-codes namespace-codes
                                 :config          {:reindex-min-bytes reindex-min-bytes
                                                   :reindex-max-bytes reindex-max-bytes
                                                   :max-old-indexes   max-old-indexes}}
                          prev-idx-t   (assoc :prev-index {:t       prev-idx-t
                                                           :address prev-idx-addr})
                          garbage-addr (assoc-in [:garbage :address] garbage-addr))
          serialized    (serde/-serialize-db-root serializer data)]
      (<? (write-index-file storage alias :root serialized)))))

(defn read-branch
  [{:keys [storage serializer] :as _idx-store} branch-address]
  (go-try
    (when-let [data (<? (storage/read-json storage branch-address true))]
      (serde/-deserialize-branch serializer data))))

(defn read-leaf
  [{:keys [storage serializer] :as _idx-store} leaf-address]
  (go-try
    (when-let [data (<? (storage/read-json storage leaf-address true))]
      (serde/-deserialize-leaf serializer data))))

(defn reify-index-root
  [index-data ledger-alias comparator t]
  (assoc index-data
         :ledger-alias ledger-alias
         :t t
         :comparator comparator))

(defn reify-index-roots
  [{:keys [t ledger-alias] :as root-data}]
  (reduce (fn [root idx]
            (let [comparator (get index/comparators idx)]
              (update root idx reify-index-root ledger-alias comparator t)))
          root-data index/types))

(defn deserialize-preds
  [preds]
  (mapv (fn [p]
          (if (iri/serialized-sid? p)
            (iri/deserialize-sid p)
            (mapv iri/deserialize-sid p)))
        preds))

(defn reify-namespaces
  [root-map]
  (let [namespaces (-> root-map :namespace-codes map-invert)]
    (assoc root-map :namespaces namespaces)))

(defn read-garbage
  "Returns garbage file data for a given index t."
  [{:keys [storage serializer] :as _idx-store} garbage-address]
  (go-try
    (when-let [data (<? (storage/read-json storage garbage-address true))]
      (serde/-deserialize-garbage serializer data))))

(defn delete-garbage-item
  "Deletes an index segment during garbage collection. Returns async chan"
  [{:keys [storage] :as _idx-store} index-segment-address]
  (storage/delete storage index-segment-address))

(defn reify-schema
  [{:keys [namespace-codes v] :as root-map}]
  (if (or (nil? v) (= 0 v))
    (update root-map :preds deserialize-preds) ;; legacy, for now only v0
    (update root-map :schema vocab/deserialize-schema namespace-codes)))

(defn reify-virtual-graphs
  [index-catalog vg-address-map]
  (go-try
    (loop [[[vg-alias storage-meta] & r] vg-address-map
           vg-map         {}]
      (if vg-alias
        (let [vg (<? (vg/read-vg index-catalog storage-meta))]
          (recur r (assoc vg-map (:alias vg) vg)))
        vg-map))))

(defn read-db-root
  "Returns all data for a db index root of a given t."
  [{:keys [storage serializer] :as index-catalog} idx-address]
  (go-try
    (if-let [data (<? (storage/read-json storage idx-address true))]
      (let [{:keys [t vg] :as root-data}
            (serde/-deserialize-db-root serializer data)

            vg-map (<? (reify-virtual-graphs index-catalog vg))]
        (-> root-data
            reify-index-roots
            reify-namespaces
            reify-schema
            (assoc :vg vg-map)
            (update :stats assoc :indexed t)))
      (throw (ex-info (str "Could not load index point at address: "
                           idx-address ".")
                      {:status 400
                       :error  :db/unavailable})))))

(defn fetch-child-attributes
  [idx-store {:keys [id comparator leftmost?] :as branch}]
  (go-try
    (if-let [{:keys [children]} (<? (read-branch idx-store id))]
      (let [branch-metadata (select-keys branch [:comparator :ledger-alias
                                                 :t :tt-id :tempid])
            child-attrs     (map-indexed (fn [i child]
                                           (-> branch-metadata
                                               (assoc :leftmost? (and leftmost?
                                                                      (zero? i)))
                                               (merge child)))
                                         children)
            child-entries   (mapcat (juxt :first identity)
                                    child-attrs)]
        (apply flake/sorted-map-by comparator child-entries))
      (throw (ex-info (str "Unable to retrieve index branch with id "
                           id " from storage.")
                      {:status 500, :error :db/storage-error})))))

(defn fetch-leaf-flakes
  [idx-store {:keys [id comparator]}]
  (go-try
    (if-let [{:keys [flakes] :as _leaf} (<? (read-leaf idx-store id))]
      (apply flake/sorted-set-by comparator flakes)
      (throw (ex-info (str "Unable to retrieve leaf node with id: "
                           id " from storage")
                      {:status 500, :error :db/storage-error})))))

(defn resolve-index-node
  [idx-store {:keys [leaf] :as node}]
  (go-try
    (let [data (if leaf
                 (<? (fetch-leaf-flakes idx-store node))
                 (<? (fetch-child-attributes idx-store node)))
          node* (if leaf
                  (assoc node :flakes data)
                  (assoc node :children data))]
      node*)))

(defn resolve-empty-leaf
  [{:keys [comparator] :as node}]
  (let [ch         (async/chan)
        empty-set  (flake/sorted-set-by comparator)
        empty-node (assoc node :flakes empty-set)]
    (async/put! ch empty-node)
    ch))

(defn resolve-empty-branch
  [{:keys [comparator ledger-alias] :as node}]
  (let [ch         (async/chan)
        child      (index/empty-leaf ledger-alias comparator)
        children   (flake/sorted-map-by comparator child)
        empty-node (assoc node :children children)]
    (async/put! ch empty-node)
    ch))

(defn resolve-empty-node
  [node]
  (if (index/resolved? node)
    (doto (async/chan)
      (async/put! node))
    (if (index/leaf? node)
      (resolve-empty-leaf node)
      (resolve-empty-branch node))))

(extend-type IndexStore
  index/Resolver
  (resolve [{:keys [cache] :as this} {:keys [id tempid] :as node}]
    (let [cache-key [::resolve id tempid]]
      (if (= :empty id)
        (resolve-empty-node node)
        (cache/lru-lookup
         cache
         cache-key
         (fn [_]
           (resolve-index-node this node)))))))
