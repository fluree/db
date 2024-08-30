(ns fluree.db.connection.s3
  (:require [clojure.string :as str]
            [fluree.db.nameservice.storage-backed :as storage-ns]
            [clojure.core.async :as async :refer [go]]
            [fluree.db.connection.cache :as conn-cache]
            [fluree.db.connection :as connection]
            [fluree.db.flake.index :as index]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.db.flake.index.storage :as index-storage]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util]
            [fluree.db.storage :as storage]
            [fluree.db.storage.s3 :as s3-storage])
  (:import (java.io Writer)))

(set! *warn-on-reflection* true)

(defrecord S3Connection [id state ledger-defaults parallelism lru-cache-atom nameservices store]
  connection/iStorage
  (-c-read [_ commit-address]
    (storage/read-json store commit-address))
  (-c-write [_ ledger-alias commit-data]
    (let [path (str/join "/" [ledger-alias "commit"])]
      (storage/content-write-json store path commit-data)))
  (-txn-read [_ txn-address]
    (storage/read-json store txn-address))
  (-txn-write [_ ledger-alias txn-data]
    (let [path (str/join "/" [ledger-alias "txn"])]
      (storage/content-write-json store path txn-data)))
  (-index-file-write [_ ledger-alias index-type index-data]
    (let [index-name (name index-type)
          path       (str/join "/" [ledger-alias "index" index-name])]
      (storage/content-write-json store path index-data)))
  (-index-file-read [_ index-address]
    (storage/read-json store index-address true))

  connection/iConnection
  (-did [_] (:did ledger-defaults))
  (-nameservices [_] nameservices)

  index/Resolver
  (resolve [conn node]
    (index-storage/index-resolver conn lru-cache-atom node)))


(defmethod print-method S3Connection [^S3Connection conn, ^Writer w]
  (.write w (str "#S3Connection "))
  (binding [*out* w]
    (pr (connection/printer-map conn))))

(defn connect
  "Create a new S3 connection."
  [{:keys [defaults parallelism s3-endpoint s3-bucket s3-prefix lru-cache-atom
           cache-max-mb serializer nameservices]
    :or   {serializer (json-serde)} :as _opts}]
  (go
    (let [conn-id        (str (random-uuid))
          state          (connection/blank-state)
          s3-store       (s3-storage/open s3-bucket s3-prefix s3-endpoint)
          nameservices*  (-> nameservices
                             (or (storage-ns/start "fluree:s3://" s3-store true))
                             util/sequential)
          cache-size     (conn-cache/memory->cache-size cache-max-mb)
          lru-cache-atom (or lru-cache-atom
                             (atom (conn-cache/create-lru-cache cache-size)))]
      (map->S3Connection {:id              conn-id
                          :store           s3-store
                          :state           state
                          :ledger-defaults defaults
                          :serializer      serializer
                          :parallelism     parallelism
                          :msg-in-ch       (async/chan)
                          :msg-out-ch      (async/chan)
                          :nameservices    nameservices*
                          :lru-cache-atom  lru-cache-atom}))))
