(ns fluree.db.conn.memory
  (:require [fluree.db.storage.core :as storage]
            [fluree.db.index :as index]
            [fluree.db.util.core :as util :refer [try* catch* exception?]]
            #?(:clj [fluree.db.full-text :as full-text])
            [fluree.db.util.log :as log]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.util.async :refer [<? go-try channel?]]
            #?(:clj  [clojure.core.async :as async :refer [go <!]]
               :cljs [cljs.core.async :as async :refer [go <!]])
            [fluree.db.conn.state-machine :as state-machine]
            [#?(:cljs cljs.cache :clj clojure.core.cache) :as cache]
            [fluree.json-ld :as json-ld]
            [fluree.db.indexer.default :as idx-default]
            [fluree.crypto :as crypto]))

#?(:clj (set! *warn-on-reflection* true))

;; Memory Connection object

(defn- addr-path
  "Returns the path portion of a Fluree memory address."
  [address]
  (if-let [[_ path] (re-find #"^fluree:memory://(.+)$" address)]
    path
    (throw (ex-info (str "Incorrectly formatted Fluree memory db address: " address)
                    {:status 500 :error :db/invalid-db}))))

(defn c-write!
  [data-atom commit-data]
  (go-try
    (let [json (json-ld/normalize-data commit-data)
          hash (crypto/sha2-256-normalize json)]
      (swap! data-atom assoc hash commit-data)
      {:name    hash
       :hash    hash
       :size    (count json)
       :address (str "fluree:memory://" hash)})))

(defn c-read
  [data-atom commit-key]
  (go-try
    (get @data-atom (addr-path commit-key))))


(defn push!
  [data-atom address ledger-data]
  (let [commit-address (:address ledger-data)]
    (swap! data-atom
           (fn [state]
             (let [commit-path  (addr-path commit-address)
                   commit       (get state commit-path)
                   address-path (addr-path address)]
               (when-not commit
                 (throw (ex-info (str "Unable to locate commit in memory, cannot push!: " commit-address)
                                 {:status 500 :error :db/invalid-db})))
               (log/debug "pushing:" address "referencing commit:" commit-address)
               (assoc state address-path commit)))))
  ledger-data)


(defrecord MemoryConnection [id transactor? memory state
                             ledger-defaults async-cache
                             local-read local-write
                             parallelism close-fn
                             msg-in-ch msg-out-ch
                             ipfs-endpoint data-atom]

  conn-proto/iStorage
  (-c-read [_ commit-key] (c-read data-atom commit-key))
  (-c-write [_ commit-data] (c-write! data-atom commit-data))

  conn-proto/iNameService
  (-push [this address ledger-data] (push! data-atom address ledger-data))
  (-pull [this ledger] :TODO)
  (-subscribe [this ledger] :TODO)
  (-lookup [this ledger] (async/go :TODO))
  (-address [_ ledger-alias _] (go (str "fluree:memory://" ledger-alias)))

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
  (-read-only? [_] false)
  (-context [_] (:context ledger-defaults))
  (-new-indexer [_ opts] (idx-default/create opts))         ;; default new ledger indexer
  (-did [_] (:did ledger-defaults))
  (-msg-in [_ msg] (go-try
                     ;; TODO - push into state machine
                     (log/warn "-msg-in: " msg)
                     :TODO))
  (-msg-out [_ msg] (go-try
                      ;; TODO - register/submit event
                      (log/warn "-msg-out: " msg)
                      :TODO))
  (-state [_] @state)
  (-state [_ ledger] (get @state ledger))

  storage/Store
  (read [_ k]
    (throw (ex-info (str "Memory connection does not support storage reads. Requested key: " k)
                    {:status 500 :error :db/unexpected-error})))
  (write [_ k data]
    (throw (ex-info (str "Memory connection does not support storage writes. Requested key: " k)
                    {:status 500 :error :db/unexpected-error})))
  (exists? [_ k]
    (throw (ex-info (str "Memory connection does not support storage exists?. Requested key: " k)
                    {:status 500 :error :db/unexpected-error})))
  (rename [_ old-key new-key]
    (throw (ex-info (str "Memory connection does not support storage rename. Old/new key: " old-key new-key)
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
  [{:keys [context did] :as defaults}]
  (go-try
    {:context context
     :did     did}))


(defn connect
  "Creates a new memory connection."
  [{:keys [local-read local-write parallelism async-cache memory defaults]}]
  (go-try
    (let [ledger-defaults    (<? (ledger-defaults defaults))
          memory             (or memory 1000000)            ;; default 1MB memory
          conn-id            (str (random-uuid))
          data-atom          (atom {})
          state              (state-machine/blank-state)
          memory-object-size (quot memory 100000)           ;; avg 100kb per cache object
          _                  (when (< memory-object-size 10)
                               (throw (ex-info (str "Must allocate at least 1MB of memory for Fluree. You've allocated: " memory " bytes.") {:status 400 :error :db/invalid-configuration})))

          default-cache-atom (atom (default-object-cache-factory memory-object-size))
          async-cache-fn     (or async-cache
                                 (default-async-cache-fn default-cache-atom))
          close-fn           (fn [& _] (log/info (str "IPFS Connection " conn-id " closed")))]
      (map->MemoryConnection {:id              conn-id
                              :ledger-defaults ledger-defaults
                              :data-atom       data-atom
                              :transactor?     false
                              :local-read      local-read
                              :local-write     local-write
                              :parallelism     parallelism
                              :msg-in-ch       (async/chan)
                              :msg-out-ch      (async/chan)
                              :close           close-fn
                              :memory          true
                              :state           state
                              :async-cache     async-cache-fn}))))
