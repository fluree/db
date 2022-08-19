(ns fluree.db.conn.ipfs
  (:refer-clojure :exclude [read])
  (:require [fluree.db.storage.core :as storage]
            [fluree.db.index :as index]
            [fluree.db.util.core :as util :refer [exception?]]
            #?(:clj [fluree.db.full-text :as full-text])
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.method.ipfs.core :as ipfs]
            [fluree.db.util.async :refer [<? go-try channel?]]
            [clojure.core.async :as async :refer [go <!]]
            [fluree.db.conn.state-machine :as state-machine]
            [#?(:cljs cljs.cache :clj clojure.core.cache) :as cache]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.db.method.ipfs.keys :as ipfs-keys]
            [fluree.db.indexer.default :as default-indexer]))

#?(:clj (set! *warn-on-reflection* true))

;; IPFS Connection object

(defn get-address
  "Returns IPNS address for a given key."
  [{:keys [ipfs-endpoint ledger-defaults] :as _conn} ledger-alias opts]
  (go-try
    (let [base-address (if-let [key (-> opts :ipns :key)]
                         (<? (ipfs-keys/address ipfs-endpoint key))
                         (-> ledger-defaults :ipns :address))]
      (str "fluree:ipns://" base-address "/" ledger-alias))))

(defn lookup-address
  "Given IPNS address, performs lookup and returns latest db address."
  [_conn ledger-name]
  (go-try
    ;; for now, no need to resolve as IPFS auto-resolves as part of its process
    ledger-name))


(defrecord IPFSConnection [id transactor? memory state
                           ledger-defaults async-cache
                           local-read local-write
                           read write
                           storage-read storage-write serializer
                           parallelism close-fn
                           msg-in-ch msg-out-ch
                           ipfs-endpoint]

  conn-proto/iStorage
  (-c-read [_ commit-key] (read commit-key))
  (-c-write [_ commit-data] (write commit-data))

  conn-proto/iNameService
  (-push [this address ledger-data] (ipfs/push! this address ledger-data))
  (-pull [this ledger] :TODO)
  (-subscribe [this ledger] :TODO)
  (-lookup [this ledger] (lookup-address this ledger))
  (-address [this ledger-alias opts] (get-address this ledger-alias opts))

  conn-proto/iConnection
  (-close [_]
    (when (fn? close-fn)
      (close-fn))
    (swap! state assoc :closed? true))
  (-closed? [_] (boolean (:closed? @state)))
  (-method [_] :ipfs)
  (-parallelism [_] parallelism)
  (-transactor? [_] transactor?)
  (-id [_] id)
  (-read-only? [_] (not (fn? write))) ; if no commit fn, then read-only
  (-context [_] (:context ledger-defaults))
  (-new-indexer [_ opts]                                    ;; default new ledger indexer
    (let [indexer-fn (:indexer ledger-defaults)]
      (indexer-fn opts)))
  (-did [_] (:did ledger-defaults))
  ;; (-msg-in [_ msg] (go-try
  ;;                    ;; TODO - push into state machine
  ;;                    (log/warn "-msg-in: " msg)
  ;;                    :TODO))
  ;; (-msg-out [_ msg] (go-try
  ;;                     ;; TODO - register/submit event
  ;;                     (log/warn "-msg-out: " msg)
  ;;                     :TODO))
  (-state [_] @state)
  (-state [_ ledger] (get @state ledger))

  storage/Store
  (read [_ k]
    (log/warn "idx-read: " k)
    (storage-read k))
  (write [_ k data]
    (storage-write k data))
  (exists? [_ k]
    (log/warn "idx-exists?: " k)
    (storage-read k))
  (rename [_ old-key new-key]
    (throw (ex-info (str "IPFS does not support renaming of files: " old-key new-key)
                    {:status 500 :error :db/unexpected-error})))

  index/Resolver
  (resolve
    [_ node]
    ;; all root index nodes will be empty

    (storage/resolve-empty-leaf node))

  #?@(:clj
      [full-text/IndexConnection
       (open-storage [conn network dbid lang]
         (throw (ex-info "Memory connection does not support full text operations."
                         {:status 500 :error :db/unexpected-error})))]))


;; TODO - the following few functions are duplicated from fluree.db.connection
;; TODO - should move to a common space

(defn- lookup-cache
  [cache-atom k value-fn]
  (if (nil? value-fn)
    (swap! cache-atom cache/evict k)
    (when-let [v (get @cache-atom k)]
      (do (swap! cache-atom cache/hit k)
          v))))

(defn- default-object-cache-fn
  "Default synchronous object cache to use for ledger."
  [cache-atom]
  (fn [k value-fn]
    (if-let [v (lookup-cache cache-atom k value-fn)]
      v
      (let [v (value-fn k)]
        (swap! cache-atom cache/miss k v)
        v))))

(defn- default-async-cache-fn
  "Default asynchronous object cache to use for ledger."
  [cache-atom]
  (fn [k value-fn]
    (let [out (async/chan)]
      (if-let [v (lookup-cache cache-atom k value-fn)]
        (async/put! out v)
        (go
          (let [v (<! (value-fn k))]
            (when-not (exception? v)
              (swap! cache-atom cache/miss k v))
            (async/put! out v))))
      out)))

(defn- default-object-cache-factory
  "Generates a default object cache."
  [cache-size]
  (cache/lru-cache-factory {} :threshold cache-size))

(defn ledger-defaults
  "Normalizes ledger defaults settings"
  [ipfs-endpoint {:keys [ipns context did indexer] :as defaults}]
  (go-try
    (let [ipns-default-key     (or (:key ipns) "self")
          ipns-default-address (<? (ipfs-keys/address ipfs-endpoint ipns-default-key))
          new-indexer-fn       (cond
                                 (fn? indexer)
                                 indexer

                                 (or (map? indexer) (nil? indexer))
                                 (fn [opts]
                                   (default-indexer/create (merge indexer opts)))

                                 :else
                                 (throw (ex-info (str "Expected an indexer constructor fn or "
                                                      "default indexer options map. Provided: " indexer)
                                                 {:status 400 :error :db/invalid-connection})))]
      (when-not ipns-default-address
        (throw (ex-info (str "IPNS publishing appears to have an issue. No corresponding ipns address found for key: "
                             ipns-default-key)
                        {:status 400 :error :db/ipfs-keys})))
      {:ipns    {:key     ipns-default-key
                 :address ipns-default-address}
       :context context
       :did     did
       :indexer new-indexer-fn})))


(defn connect
  "Creates a new IPFS connection."
  [{:keys [server local-read local-write parallelism async-cache memory defaults serializer]
    :or   {server     "http://127.0.0.1:5001/"
           serializer (json-serde)}}]
  (go-try
    (let [ipfs-endpoint      (or server "http://127.0.0.1:5001/") ;; TODO - validate endpoint looks like a good URL and ends in a '/' or add it
          ledger-defaults    (<? (ledger-defaults ipfs-endpoint defaults))
          memory             (or memory 1000000)            ;; default 1MB memory
          conn-id            (str (random-uuid))
          read               (ipfs/default-read-fn ipfs-endpoint)
          write              (ipfs/default-commit-fn ipfs-endpoint)
          state              (state-machine/blank-state)
          memory-object-size (quot memory 100000)           ;; avg 100kb per cache object
          _                  (when (< memory-object-size 10)
                               (throw (ex-info (str "Must allocate at least 1MB of memory for Fluree. You've allocated: " memory " bytes.") {:status 400 :error :db/invalid-configuration})))

          default-cache-atom (atom (default-object-cache-factory memory-object-size))
          async-cache-fn     (or async-cache
                                 (default-async-cache-fn default-cache-atom))
          close-fn           (fn [& _] (log/info (str "IPFS Connection " conn-id " closed")))
          storage-write      (fn [k data]
                               (write data))]
      ;; TODO - need to set up monitor loops for async chans
      (map->IPFSConnection {:id              conn-id
                            :ipfs-endpoint   ipfs-endpoint
                            :ledger-defaults ledger-defaults
                            :transactor?     false
                            :local-read      local-read
                            :local-write     local-write
                            :read            read
                            :write           write
                            :storage-read    (ipfs/default-read-fn ipfs-endpoint)
                            :storage-write   storage-write
                            :serializer      serializer
                            :parallelism     parallelism
                            :msg-in-ch       (async/chan)
                            :msg-out-ch      (async/chan)
                            :close           close-fn
                            :memory          true
                            :state           state
                            :async-cache     async-cache-fn}))))
