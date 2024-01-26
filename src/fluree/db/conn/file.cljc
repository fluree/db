(ns fluree.db.conn.file
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.db.util.async :refer [<? go-try]]
            [clojure.string :as str]
            [fluree.crypto :as crypto]
            [fluree.db.util.context :as ctx-util]
            [fluree.db.util.core :as util]
            [fluree.json-ld :as json-ld]
            [fluree.db.index :as index]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.conn.cache :as conn-cache]
            [fluree.db.conn.core :as conn-core]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.indexer.storage :as storage]
            [fluree.db.indexer.default :as idx-default]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.db.util.filesystem :as fs]
            [fluree.db.util.bytes :as bytes]
            #?(:clj [fluree.db.full-text :as full-text])
            [fluree.db.util.json :as json]
            [fluree.db.nameservice.filesystem :as ns-filesystem]
            [fluree.db.ledger.proto :as ledger-proto]
            [fluree.db.storage :as store]
            [fluree.db.storage.util :as store-util])
  #?(:clj (:import (java.io Writer))))

#?(:clj (set! *warn-on-reflection* true))

(defn- write-data
  [{:keys [store] :as _conn} ledger data-type data]
  (go-try
    (let [alias      (ledger-proto/-alias ledger)
          branch     (name (:name (ledger-proto/-branch ledger)))
          json       (if (string? data)
                       data
                       (json-ld/normalize-data data))
          bytes      (bytes/string->UTF8 json)
          hash       (crypto/sha2-256 bytes :hex)
          type-dir   (name data-type)
          path       (str alias
                          (when branch (str "/" branch))
                          (str "/" type-dir "/")
                          hash ".json")
          {:keys [k hash v address]} (<? (store/write store path bytes))]
      {:name    hash
       :hash    hash
       :json    json
       :size    (count json)
       :address address})))

(defn write-commit
  [conn ledger commit-data]
  (write-data conn ledger :commit commit-data))

(defn write-context
  [conn ledger context-data]
  (write-data conn ledger :context context-data))

(defn write-index-item
  [conn ledger index-type index-data]
  (write-data conn ledger (str "index/" (name index-type)) index-data))

(defn read-commit
  [conn address]
  (go-try
    (-> (<? (store/read (:store conn) address))
        (json/parse false))))

(defn read-context
  [conn context-key]
  (go-try
    (-> (<? (store/read (:store conn) context-key))
        (json/parse true))))

(defn read-index-item
  [conn index-address]
  (go-try
    (-> (<? (store/read (:store conn) index-address))
        (json/parse true))))

(defn close
  [id state]
  (log/info "Closing file connection" id)
  (swap! state assoc :closed? true))

(defrecord FileConnection [id state ledger-defaults parallelism msg-in-ch store
                           nameservices serializer msg-out-ch lru-cache-atom]

  conn-proto/iStorage
  (-c-read [conn commit-key] (read-commit conn commit-key))
  (-c-write [conn ledger commit-data] (write-commit conn ledger commit-data))
  (-ctx-read [conn context-key] (read-context conn context-key))
  (-ctx-write [conn ledger context-data] (write-context conn ledger context-data))
  (-index-file-write [conn ledger index-type index-data]
    (write-index-item conn ledger index-type index-data))
  (-index-file-read [conn index-address]
    (read-index-item conn index-address))

  conn-proto/iConnection
  (-close [_] (close id state))
  (-closed? [_] (boolean (:closed? @state)))
  (-method [_] :file)
  (-parallelism [_] parallelism)
  (-id [_] id)
  (-new-indexer [_ opts]
    (let [indexer-fn (:indexer ledger-defaults)]
      (indexer-fn opts)))
  ;; default new ledger indexer
  (-did [_] (:did ledger-defaults))
  (-msg-in [conn msg] (throw (ex-info "Unsupported FileConnection op: msg-in" {})))
  (-msg-out [conn msg] (throw (ex-info "Unsupported FileConnection op: msg-out" {})))
  (-nameservices [_] nameservices)
  (-state [_] @state)
  (-state [_ ledger] (get @state ledger))

  index/Resolver
  (resolve
    [conn {:keys [id leaf tempid] :as node}]
    (let [cache-key [::resolve id tempid]]
      (if (= :empty id)
        (storage/resolve-empty-node node)
        (conn-cache/lru-lookup
          lru-cache-atom
          cache-key
          (fn [_]
            (storage/resolve-index-node conn node
                                        (fn [] (conn-cache/lru-evict lru-cache-atom cache-key))))))))

  #?@(:clj
      [full-text/IndexConnection
       (open-storage [conn network dbid lang]
         (throw (ex-info "File connection does not support full text operations."
                         {:status 500 :error :db/unexpected-error})))]))

#?(:cljs
   (extend-type FileConnection
     IPrintWithWriter
     (-pr-writer [conn w opts]
       (-write w "#FileConnection ")
       (-write w (pr (conn-core/printer-map conn))))))

#?(:clj
   (defmethod print-method FileConnection [^FileConnection conn, ^Writer w]
     (.write w (str "#FileConnection "))
     (binding [*out* w]
       (pr (conn-core/printer-map conn)))))

(defn trim-last-slash
  [s]
  (if (str/ends-with? s "/")
    (subs s 0 (dec (count s)))
    s))

(defn ledger-defaults
  [{:keys [did indexer]}]
  {:did     did
   :indexer (cond
              (fn? indexer)
              indexer

              (or (map? indexer) (nil? indexer))
              (fn [opts]
                (idx-default/create (merge indexer opts)))

              :else
              (throw (ex-info (str "Expected an indexer constructor fn or "
                                   "default indexer options map. Provided: " indexer)
                              {:status 400 :error :db/invalid-file-connection})))})

(defn default-file-nameservice
  "Returns file nameservice or will throw if storage-path generates an exception."
  [store]
  (ns-filesystem/initialize store))

(defn connect
  "Create a new file system connection."
  [{:keys [defaults parallelism store lru-cache-atom memory serializer nameservices]
    :or {serializer (json-serde)} :as _opts}]
  (go
    (let [conn-id        (str (random-uuid))
          state          (conn-core/blank-state)
          nameservices*  (util/sequential
                           (or nameservices (default-file-nameservice store)))
          cache-size     (conn-cache/memory->cache-size memory)
          lru-cache-atom (or lru-cache-atom (atom (conn-cache/create-lru-cache cache-size)))]
      ;; TODO - need to set up monitor loops for async chans
      (map->FileConnection {:id              conn-id
                            :store           store
                            :ledger-defaults (ledger-defaults defaults)
                            :serializer      serializer
                            :parallelism     parallelism
                            :msg-in-ch       (async/chan)
                            :msg-out-ch      (async/chan)
                            :nameservices    nameservices*
                            :state           state
                            :lru-cache-atom  lru-cache-atom}))))
