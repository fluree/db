(ns fluree.db.conn.file
  (:require [clojure.core.async :as async :refer [go]]
            [clojure.string :as str]
            [fluree.db.util.core :as util]
            [fluree.db.index :as index]
            [fluree.db.connection :as connection]
            [fluree.db.conn.cache :as conn-cache]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.indexer.storage :as index-storage]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.db.nameservice.storage-backed :as storage-ns]
            [fluree.db.storage :as storage]
            [fluree.db.storage.file :as file-storage])
  #?(:clj (:import (java.io Writer))))

#?(:clj (set! *warn-on-reflection* true))

(defn close
  [id state]
  (log/info "Closing file connection" id)
  (swap! state assoc :closed? true))

(defrecord FileConnection [id state ledger-defaults parallelism msg-in-ch store
                           nameservices serializer msg-out-ch lru-cache-atom]

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
  (-index-file-delete [_ index-address]
    (storage/delete store index-address))

  connection/iConnection
  (-close [_] (close id state))
  (-closed? [_] (boolean (:closed? @state)))
  (-did [_] (:did ledger-defaults))
  (-msg-in [conn msg] (throw (ex-info "Unsupported FileConnection op: msg-in" {})))
  (-msg-out [conn msg] (throw (ex-info "Unsupported FileConnection op: msg-out" {})))
  (-nameservices [_] nameservices)
  (-state [_] @state)
  (-state [_ ledger] (get @state ledger))

  index/Resolver
  (resolve
    [conn node]
    (index-storage/index-resolver conn lru-cache-atom node)))

#?(:cljs
   (extend-type FileConnection
     IPrintWithWriter
     (-pr-writer [conn w opts]
       (-write w "#FileConnection ")
       (-write w (pr (connection/printer-map conn))))))

#?(:clj
   (defmethod print-method FileConnection [^FileConnection conn, ^Writer w]
     (.write w (str "#FileConnection "))
     (binding [*out* w]
       (pr (connection/printer-map conn)))))

(defn default-lru-cache
  [cache-max-mb]
  (let [cache-size (conn-cache/memory->cache-size cache-max-mb)]
    (atom (conn-cache/create-lru-cache cache-size))))

(defn connect
  "Create a new file system connection."
  [{:keys [defaults parallelism storage-path store
           lru-cache-atom cache-max-mb serializer nameservices]
    :or   {serializer (json-serde)} :as _opts}]
  (log/debug "Initialized file connection with options: " _opts)
  (go
    (let [conn-id         (str (random-uuid))
          state           (connection/blank-state)
          lru-cache-atom* (or lru-cache-atom
                              (default-lru-cache cache-max-mb))
          store*          (or store
                              (file-storage/open storage-path))
          nameservices*   (-> nameservices
                              (or (storage-ns/start "fluree:file://" store* true))
                              util/sequential)]
      ;; TODO - need to set up monitor loops for async chans
      (map->FileConnection {:id              conn-id
                            :store           store*
                            :ledger-defaults defaults
                            :serializer      serializer
                            :parallelism     parallelism
                            :msg-in-ch       (async/chan)
                            :msg-out-ch      (async/chan)
                            :nameservices    nameservices*
                            :state           state
                            :lru-cache-atom  lru-cache-atom*}))))
