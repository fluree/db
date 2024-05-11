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
  [{:keys [alias conn] :as _db} idx-type leaf]
  (let [ser (serdeproto/-serialize-leaf (serde conn) leaf)]
    (connection/-index-file-write conn alias idx-type ser)))

(defn write-branch-data
  "Serializes final data for branch and writes it to provided key.
  Returns two-tuple of response output and raw bytes written."
  [{:keys [alias conn] :as _db} idx-type data]
  (let [ser (serdeproto/-serialize-branch (serde conn) data)]
    (connection/-index-file-write conn alias idx-type ser)))

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
  (let [{:keys [alias conn t]} db

        data {:ledger-alias alias
              :t            t
              :garbage      garbage}
        ser  (serdeproto/-serialize-garbage (serde conn) data)]
    (connection/-index-file-write conn alias :garbage ser)))

(defn write-db-root
  [db]
  (let [{:keys [alias conn schema t stats spot psot post opst tspo
                namespace-codes]}
        db

        preds (vocab/serialize-schema-predicates schema)
        data  {:ledger-alias    alias
               :t               t
               :preds           preds
               :stats           (select-keys stats [:flakes :size])
               :spot            (child-data spot)
               :psot            (child-data psot)
               :post            (child-data post)
               :opst            (child-data opst)
               :tspo            (child-data tspo)
               :timestamp       (util/current-time-millis)
               :namespace-codes namespace-codes}
        ser   (serdeproto/-serialize-db-root (serde conn) data)]
    (connection/-index-file-write conn alias :root ser)))


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
     (if-let [data (<? (connection/-index-file-read conn idx-address))]
       (let [{:keys [t] :as root-data}
             (serdeproto/-deserialize-db-root (serde conn) data)]
         (-> root-data
             reify-index-roots
             reify-namespaces
             (update :stats assoc :indexed t)
             (update :preds deserialize-preds)))
       (throw (ex-info (str "Could not load index point at address: "
                            idx-address ".")
                       {:status 400
                        :error  :db/unavailable}))))))

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
