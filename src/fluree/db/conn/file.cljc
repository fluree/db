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
            [fluree.db.conn.state-machine :as state-machine]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.storage :as storage]
            [fluree.db.indexer.default :as idx-default]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.db.util.filesystem :as fs]
            [fluree.db.util.bytes :as bytes]
            #?(:clj [fluree.db.full-text :as full-text])
            [fluree.db.util.json :as json]
            [fluree.db.nameservice.filesystem :as ns-filesystem]
            [fluree.db.ledger.proto :as ledger-proto]))

#?(:clj (set! *warn-on-reflection* true))

(defn file-address
  "Turn a path or a protocol-relative URL into a fluree file address."
  [path]
  (if (str/starts-with? path "//")
    (str "fluree:file:" path)
    (str "fluree:file://" path)))


(defn address-path
  [address]
  (let [[_ _ path] (str/split address #":")]
    path))

(defn address-full-path
  [{:keys [storage-path] :as _conn} address]
  (str (fs/local-path storage-path) "/" (address-path address)))

(defn address-path-exists?
  [conn address]
  (let [full-path (address-full-path conn address)]
    (fs/exists? full-path)))

(defn read-address
  [conn address]
  (->> address (address-full-path conn) fs/read-file))

(defn read-commit
  [conn address]
  (json/parse (read-address conn address) false))

(defn- write-data
  [{:keys [storage-path] :as _conn} ledger data-type data]
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
        write-path (str (fs/local-path storage-path) "/" path)]
    (log/debug (str "Writing " (name data-type) " at " write-path))
    (fs/write-file write-path bytes)
    {:name    hash
     :hash    hash
     :json    json
     :size    (count json)
     :address (file-address path)}))

(defn write-commit
  [conn ledger commit-data]
  (write-data conn ledger :commit commit-data))

(defn write-context
  [conn ledger context-data]
  (write-data conn ledger :context context-data))

(defn write-index-item
  [conn ledger index-type index-data]
  (write-data conn ledger (str "index/" (name index-type)) index-data))

(defn push
  "Just write to a different directory?"
  [{:keys [storage-path] :as _conn} publish-address {commit-address :address}]
  (let [local-path  (fs/local-path storage-path)
        commit-path (address-path commit-address)
        head-path   (address-path publish-address)
        write-path  (str local-path "/" head-path)

        work        (fn [complete]
                      (log/debug (str "Updating head at " write-path " to " commit-path "."))
                      (fs/write-file write-path (bytes/string->UTF8 commit-path))
                      (complete (file-address head-path)))]
    #?(:clj  (let [p (promise)]
               (future (work (partial deliver p)))
               p)
       :cljs (js/Promise. (fn [resolve reject] (work resolve))))))

(defn read-context
  [conn context-key]
  (json/parse (read-address conn context-key) true))

(defrecord FileConnection [id memory state ledger-defaults parallelism msg-in-ch
                           nameservices serializer msg-out-ch lru-cache-atom]

  conn-proto/iStorage
  (-c-read [conn commit-key] (go (read-commit conn commit-key)))
  (-c-write [conn ledger commit-data] (go (write-commit conn ledger
                                                        commit-data)))
  (-ctx-read [conn context-key] (go (read-context conn context-key)))
  (-ctx-write [conn ledger context-data] (go (write-context conn ledger
                                                            context-data)))
  (-index-file-write [conn ledger index-type index-data]
    #?(:clj (async/thread (write-index-item conn ledger index-type index-data))
       :cljs (async/go (write-index-item conn ledger index-type index-data))))
  (-index-file-read [conn index-address]
    #?(:clj (async/thread (json/parse (read-address conn index-address) true))
       :cljs (async/go (json/parse (read-address conn index-address) true))))

  conn-proto/iConnection
  (-close [_]
    (log/info "Closing file connection" id)
    (swap! state assoc :closed? true))
  (-closed? [_] (boolean (:closed? @state)))
  (-method [_] :file)
  (-parallelism [_] parallelism)
  (-id [_] id)
  (-default-context [_] (:context ledger-defaults))
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

(defn trim-last-slash
  [s]
  (if (str/ends-with? s "/")
    (subs s 0 (dec (count s)))
    s))

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
                   (throw (ex-info (str "Expected an indexer constructor fn or "
                                        "default indexer options map. Provided: " indexer)
                                   {:status 400 :error :db/invalid-file-connection})))})

(defn default-file-nameservice
  "Returns file nameservice or will throw if storage-path generates an exception."
  [storage-path]
  (ns-filesystem/initialize storage-path))

(defn connect
  "Create a new file system connection."
  [{:keys [defaults parallelism storage-path lru-cache-atom memory serializer nameservices]
    :or   {serializer (json-serde)} :as _opts}]
  (go
    (let [storage-path   (trim-last-slash storage-path)
          conn-id        (str (random-uuid))
          state          (state-machine/blank-state)
          nameservices*  (util/sequential
                           (or nameservices (default-file-nameservice storage-path)))
          cache-size     (conn-cache/memory->cache-size memory)
          lru-cache-atom (or lru-cache-atom (atom (conn-cache/create-lru-cache cache-size)))]
      ;; TODO - need to set up monitor loops for async chans
      (map->FileConnection {:id              conn-id
                            :storage-path    storage-path
                            :ledger-defaults (ledger-defaults defaults)
                            :serializer      serializer
                            :parallelism     parallelism
                            :msg-in-ch       (async/chan)
                            :msg-out-ch      (async/chan)
                            :nameservices    nameservices*
                            :state           state
                            :lru-cache-atom  lru-cache-atom}))))
