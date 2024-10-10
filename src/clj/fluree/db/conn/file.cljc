(ns fluree.db.conn.file
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.db.util.async :refer [<? go-try]]
            [clojure.string :as str]
            [fluree.db.util.core :as util]
            [fluree.json-ld :as json-ld]
            [fluree.db.flake.index :as index]
            [fluree.db.connection :as connection]
            [fluree.db.conn.cache :as conn-cache]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.flake.index.storage :as index-storage]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.db.util.bytes :as bytes]
            [fluree.db.util.json :as json]
            [fluree.db.nameservice.filesystem :as ns-filesystem]
            [fluree.db.storage :as storage]
            [fluree.db.storage.file :as file-storage])
  #?(:clj (:import (java.io Writer))))

#?(:clj (set! *warn-on-reflection* true))

(defn- write-data
  [{:keys [store] :as _conn} ledger-alias data-type data]
  (go-try
    (let [json     (if (string? data)
                     data
                     (json-ld/normalize-data data))
          bytes    (bytes/string->UTF8 json)
          type-dir (name data-type)
          path     (str/join "/" [ledger-alias type-dir])

          {:keys [path hash address]} (<? (storage/write store path bytes))]
      {:name    path
       :hash    hash
       :json    json
       :size    (count json)
       :address address})))

(defn read-data
  [conn address keywordize?]
  (go-try
   (some-> (<? (storage/read (:store conn) address))
           (json/parse keywordize?))))

(defn delete-data
  "Will throw if not deleted."
  [conn address]
  (storage/delete (:store conn) address))

(defn close
  [id state]
  (log/info "Closing file connection" id)
  (swap! state assoc :closed? true))

(defrecord FileConnection [id state ledger-defaults parallelism msg-in-ch store
                           nameservices serializer msg-out-ch lru-cache-atom]

  connection/iStorage
  (-c-read [conn commit-key]
    (read-data conn commit-key false))
  (-c-write [conn ledger-alias commit-data]
    (write-data conn ledger-alias :commit commit-data))
  (-txn-read [conn txn-key]
    (read-data conn txn-key false))
  (-txn-write [conn ledger-alias txn-data]
    (write-data conn ledger-alias :txn txn-data))
  (-index-file-write [conn ledger-alias index-type index-data]
    (write-data conn ledger-alias (str "index/" (name index-type)) index-data))
  (-index-file-read [conn index-address] (read-data conn index-address true))
  (-index-file-delete [conn index-address] (delete-data conn index-address))

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

(defn default-file-nameservice
  "Returns file nameservice or will throw if storage-path generates an exception."
  [path]
  (ns-filesystem/initialize path))

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
          nameservices*   (util/sequential
                           (or nameservices (default-file-nameservice storage-path)))
          lru-cache-atom* (or lru-cache-atom
                              (default-lru-cache cache-max-mb))
          store*          (or store
                              (file-storage/open storage-path))]
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
