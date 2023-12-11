(ns fluree.db.conn.s3
  (:require [cognitect.aws.client.api :as aws]
            [fluree.db.method.s3.core :as s3]
            [fluree.db.nameservice.s3 :as ns-s3]
            [clojure.core.async :as async :refer [go <!]]
            [fluree.crypto :as crypto]
            [fluree.db.conn.cache :as conn-cache]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.conn.core :as conn-core]
            [fluree.db.full-text :as full-text]
            [fluree.db.index :as index]
            [fluree.db.indexer.default :as idx-default]
            [fluree.db.ledger.proto :as ledger-proto]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.db.storage :as storage]
            [fluree.db.util.context :as ctx-util]
            [fluree.db.util.core :as util]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld])
  (:import (java.io Writer)))

(set! *warn-on-reflection* true)


(defn write-data
  [{:keys [s3-client s3-bucket s3-prefix] :as _conn} ledger data-type data]
  (go
    (let [alias    (ledger-proto/-alias ledger)
          branch   (-> ledger ledger-proto/-branch :name name)
          json     (if (string? data)
                     data
                     (json-ld/normalize-data data))
          bytes    (.getBytes ^String json)
          hash     (crypto/sha2-256 bytes :hex)
          type-dir (name data-type)
          path     (str alias
                        (when branch (str "/" branch))
                        (str "/" type-dir "/")
                        hash ".json")
          result   (<! (s3/write-s3-data s3-client s3-bucket s3-prefix path bytes))]
      (if (instance? Throwable result)
        result
        {:name    hash
         :hash    hash
         :json    json
         :size    (count json)
         :address (s3/s3-address s3-bucket s3-prefix path)}))))

(defn read-commit
  [{:keys [s3-client s3-bucket s3-prefix] :as _conn} address]
  (go (json/parse (<! (s3/read-address s3-client s3-bucket s3-prefix address)) false)))

(defn write-commit
  [conn ledger commit-data]
  (write-data conn ledger :commit commit-data))

(defn read-context
  [{:keys [s3-client s3-bucket s3-prefix] :as _conn} address]
  (go (json/parse (<! (s3/read-address s3-client s3-bucket s3-prefix address)) false)))

(defn write-context
  [conn ledger context-data]
  (write-data conn ledger :context context-data))

(defn write-index
  [conn ledger index-type index-data]
  (write-data conn ledger (str "index/" (name index-type)) index-data))

(defn read-index
  [{:keys [s3-client s3-bucket s3-prefix] :as _conn} index-address]
  (go (-> (s3/read-address s3-client s3-bucket s3-prefix index-address) <! (json/parse true))))


(defrecord S3Connection [id s3-client s3-bucket s3-prefix memory state
                         ledger-defaults parallelism msg-in-ch msg-out-ch
                         lru-cache-atom nameservices]
  conn-proto/iStorage
  (-c-read [conn commit-key] (read-commit conn commit-key))
  (-c-write [conn ledger commit-data] (write-commit conn ledger commit-data))
  (-ctx-read [conn context-key] (read-context conn context-key))
  (-ctx-write [conn ledger context-data] (write-context conn ledger context-data))
  (-index-file-write [conn ledger index-type index-data]
    (write-index conn ledger index-type index-data))
  (-index-file-read [conn index-address]
    (read-index conn index-address))

  conn-proto/iConnection
  (-close [_] (swap! state assoc :closed? true))
  (-closed? [_] (boolean (:closed? @state)))
  (-method [_] :s3)
  (-parallelism [_] parallelism)
  (-id [_] id)
  (-new-indexer [_ opts]
    (let [indexer-fn (:indexer ledger-defaults)]
      (indexer-fn opts)))
  (-did [_] (:did ledger-defaults))
  (-msg-in [_ _] (throw (ex-info "Unsupported S3Connection op: msg-in" {})))
  (-msg-out [_ _] (throw (ex-info "Unsupported S3Connection op: msg-out" {})))
  (-nameservices [_] nameservices)
  (-state [_] @state)
  (-state [_ ledger] (get @state ledger))

  index/Resolver
  (resolve [conn {:keys [id leaf tempid] :as node}]
    (let [cache-key [::resolve id tempid]]
      (if (= :empty id)
        (storage/resolve-empty-node node)
        (conn-cache/lru-lookup lru-cache-atom cache-key
                               (fn [_]
                                 (storage/resolve-index-node
                                   conn node
                                   (fn [] (conn-cache/lru-evict lru-cache-atom
                                                                cache-key))))))))

  full-text/IndexConnection
  (open-storage [_conn _network _dbid _lang]
    (throw (ex-info "S3 connection does not support full text operations."
                    {:status 400, :error :db/unsupported-operation}))))


(defmethod print-method S3Connection [^S3Connection conn, ^Writer w]
  (.write w (str "#S3Connection "))
  (binding [*out* w]
    (pr (conn-core/printer-map conn))))

(defn ledger-defaults
  [{:keys [context context-type did indexer]}]
  {:context      (ctx-util/stringify-context context)
   :context-type context-type
   :did          did
   :indexer      (cond
                   (fn? indexer)
                   indexer

                   (or (map? indexer) (nil? indexer))
                   (fn [opts]
                     (idx-default/create (merge indexer opts)))

                   :else
                   (throw (ex-info (str "Expected an indexer constructor fn or default indexer options map. Provided: "
                                        indexer)
                                   {:status 400, :error :db/invalid-s3-connection})))})

(defn default-S3-nameservice
  "Returns S3 nameservice or will throw if storage-path generates an exception."
  [s3-client s3-bucket s3-prefix]
  (ns-s3/initialize s3-client s3-bucket s3-prefix))

(defn connect
  "Create a new S3 connection."
  [{:keys [defaults parallelism s3-endpoint s3-bucket s3-prefix lru-cache-atom
           memory serializer nameservices]
    :or   {serializer (json-serde)} :as _opts}]
  (go
    (let [aws-opts       (cond-> {:api :s3}
                                 s3-endpoint (assoc :endpoint-override s3-endpoint))
          client         (aws/client aws-opts)
          conn-id        (str (random-uuid))
          state          (conn-core/blank-state)
          nameservices*  (util/sequential
                           (or nameservices (default-S3-nameservice client s3-bucket s3-prefix)))
          cache-size     (conn-cache/memory->cache-size memory)
          lru-cache-atom (or lru-cache-atom
                             (atom (conn-cache/create-lru-cache cache-size)))]
      (map->S3Connection {:id              conn-id
                          :s3-client       client
                          :s3-bucket       s3-bucket
                          :s3-prefix       s3-prefix
                          :memory          memory
                          :state           state
                          :ledger-defaults (ledger-defaults defaults)
                          :serializer      serializer
                          :parallelism     parallelism
                          :msg-in-ch       (async/chan)
                          :msg-out-ch      (async/chan)
                          :nameservices    nameservices*
                          :lru-cache-atom  lru-cache-atom}))))
