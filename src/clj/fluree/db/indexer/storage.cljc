(ns fluree.db.indexer.storage
  (:require [fluree.db.serde.protocol :as serdeproto]
            [fluree.db.flake :as flake]
            [clojure.string :as str]
            [clojure.set :refer [map-invert]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.index :as index]
            [fluree.db.json-ld.iri :as iri]
            [clojure.core.async :refer [go] :as async]
            [fluree.db.util.async #?(:clj :refer :cljs :refer-macros) [<? go-try]]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.json-ld.vocab :as vocab]
            [fluree.db.connection :as connection]))

#?(:clj (set! *warn-on-reflection* true))

(defn serde
  "Returns serializer from connection."
  [conn]
  (:serializer conn))

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
  (select-keys child [:id :leaf :first :rhs :size]))

(defn write-leaf
  "Serializes and writes the index leaf node `leaf` to storage."
  [{:keys [conn ledger] :as _db} idx-type leaf]
  (let [ser (serdeproto/-serialize-leaf (serde conn) leaf)]
    (connection/-index-file-write conn ledger idx-type ser)))

(defn write-branch-data
  "Serializes final data for branch and writes it to provided key.
  Returns two-tuple of response output and raw bytes written."
  [{:keys [conn ledger] :as _db} idx-type data]
  (let [ser (serdeproto/-serialize-branch (serde conn) data)]
    (connection/-index-file-write conn ledger idx-type ser)))

(defn write-branch
  "Writes the child attributes index branch node `branch` to storage."
  [db idx-type {:keys [children] :as _branch}]
  (let [child-vals (->> children
                        (map val)
                        (mapv child-data))
        data       {:children child-vals}]
    (write-branch-data db idx-type data)))

(defn write-garbage
  "Writes garbage record out for latest index."
  [db garbage]
  (let [{:keys [conn ledger ledger-alias t]} db

        data {:ledger-alias ledger-alias
              :t            t
              :garbage      garbage}
        ser  (serdeproto/-serialize-garbage (serde conn) data)]
    (connection/-index-file-write conn ledger :garbage ser)))

(defn write-db-root
  [db]
  (let [{:keys [conn ledger commit t stats spot psot post opst tspo
                schema namespace-codes]}
        db

        ledger-alias (:id commit)
        data         {:ledger-alias    ledger-alias
                      :t               t
                      :v               1 ;; version of db root file
                      :schema          (vocab/serialize-schema schema)
                      :stats           (select-keys stats [:flakes :size])
                      :spot            (child-data spot)
                      :psot            (child-data psot)
                      :post            (child-data post)
                      :opst            (child-data opst)
                      :tspo            (child-data tspo)
                      :timestamp       (util/current-time-millis)
                      :prevIndex       (or (:indexed stats) 0)
                      :namespace-codes  namespace-codes}
        ser          (serdeproto/-serialize-db-root (serde conn) data)]
    (connection/-index-file-write conn ledger :root ser)))


(defn read-branch
  [{:keys [serializer] :as conn} key]
  (go-try
    (when-let [data (<? (connection/-index-file-read conn key))]
      (serdeproto/-deserialize-branch serializer data))))

(defn read-leaf
  [{:keys [serializer] :as conn} key]
  (go-try
    (when-let [data (<? (connection/-index-file-read conn key))]
      (serdeproto/-deserialize-leaf serializer data))))

(defn reify-index-root
  "Turns each index root node into an unresolved node."
  [_conn {:keys [ledger-alias comparators t]} index index-data]
  (let [cmp (or (get comparators index)
                (throw (ex-info (str "Internal error reifying db index root: "
                                     (pr-str index))
                                {:status 500
                                 :error  :db/unexpected-error})))]
    (cond-> index-data
      (:rhs index-data)   (update :rhs flake/parts->Flake)
      (:first index-data) (update :first flake/parts->Flake)
      true                (assoc :comparator cmp
                                 :ledger-alias ledger-alias
                                 :t t
                                 :leftmost? true))))

(defn reify-db-root-v0
  "Reify db root for version 0 of the file.

  This legacy version requires many queries to hydrate
  the schema and will be slow for large dbs"
  [conn blank-db root-data]
  (go-try
   (let [{:keys [t stats preds namespace-codes]}
         root-data
         namespaces (map-invert namespace-codes)
         db         (assoc blank-db
                      :t t
                      :namespaces namespaces
                      :namespace-codes namespace-codes
                      :stats (assoc stats :indexed t))
         indexed-db (reduce
                     (fn [db* idx]
                       (let [idx-root (reify-index-root conn db* idx (get root-data idx))]
                         (assoc db* idx idx-root)))
                     db index/types)
         preds*     (mapv (fn [p]
                            (if (iri/serialized-sid? p)
                              (iri/deserialize-sid p)
                              (mapv iri/deserialize-sid p)))
                          preds)
         schema     (<? (vocab/load-schema indexed-db preds*))]
     (assoc indexed-db :schema schema))))

(defn reify-db-root-v1
  [blank-db root-data]
  (let [{:keys [t stats schema namespace-codes]}
        root-data
        namespaces (map-invert namespace-codes)
        db         (assoc blank-db
                     :t t
                     :namespaces namespaces
                     :namespace-codes namespace-codes
                     :stats (assoc stats :indexed t))
        indexed-db (reduce
                    (fn [db* idx]
                      (let [idx-root (reify-index-root nil db* idx (get root-data idx))]
                        (assoc db* idx idx-root)))
                    db index/types)
        schema     (vocab/deserialize-schema schema namespace-codes)]
    (assoc indexed-db :schema schema)))

(defn reify-db-root
  "Constructs db from blank-db, and ensure index roots have proper config as unresolved nodes.

  Returns async chan"
  [conn blank-db {:keys [v] :as root-data}]
  (cond
    (= 1 v)
    (go (reify-db-root-v1 blank-db root-data))

    (nil? v) ;; version 0
    (reify-db-root-v0 conn blank-db root-data)

    :else
    (do
      (log/warn "Index db-root files not recognized. File contents: " root-data)
      (throw (ex-info (str "Invalid db-root index file - version or file not recognized. "
                           "Attempting to reify index for db: " blank-db)
                      {:status 500
                       :error  :db/invalid-index})))))


(defn read-garbage
  "Returns garbage file data for a given index t."
  [conn ledger-alias t]
  (go-try
    (let [key  (ledger-garbage-key ledger-alias t)
          data (<? (connection/-index-file-read conn key))]
      (when data
        (serdeproto/-deserialize-garbage (serde conn) data)))))


(defn read-db-root
  "Returns all data for a db index root of a given t."
  ([conn idx-address]
   (go-try
     (let [data (<? (connection/-index-file-read conn idx-address))]
       (when data
         (serdeproto/-deserialize-db-root (serde conn) data))))))


(defn reify-db
  "Reifies db at specified index point. If unable to read db-root at index,
  throws."
  ([conn blank-db idx-address]
   (go-try
     (let [db-root (<? (read-db-root conn idx-address))]
       (if-not db-root
         (throw (ex-info (str "Database " (:address blank-db)
                              " could not be loaded at index point: "
                              idx-address ".")
                         {:status 400
                          :error  :db/unavailable}))
         (<? (reify-db-root conn blank-db db-root)))))))

(defn fetch-child-attributes
  [conn {:keys [id comparator leftmost?] :as branch}]
  (go-try
    (if-let [{:keys [children]} (<? (read-branch conn id))]
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
  [conn {:keys [id comparator]}]
  (go-try
    (if-let [{:keys [flakes] :as leaf} (<? (read-leaf conn id))]
      (apply flake/sorted-set-by comparator flakes)
      (throw (ex-info (str "Unable to retrieve leaf node with id: "
                           id " from storage")
                      {:status 500, :error :db/storage-error})))))

(defn resolve-index-node
  ([conn node]
   (resolve-index-node conn node nil))
  ([conn {:keys [comparator leaf] :as node} error-fn]
   (assert comparator "Cannot resolve index node; configuration does not have a comparator.")
   (let [return-ch (async/chan)]
     (go
       (try*
         (let [[k data] (if leaf
                          [:flakes (<? (fetch-leaf-flakes conn node))]
                          [:children (<? (fetch-child-attributes conn node))])]
           (async/put! return-ch
                       (assoc node k data)))
         (catch* e
                 (log/error e "Error resolving index node")
                 (when error-fn
                   (try*
                     (error-fn)
                     (catch* e (log/error e "Error executing error-fn in resolve-index-node!"))))
                 (async/put! return-ch e)
                 (async/close! return-ch))))
     return-ch)))

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
