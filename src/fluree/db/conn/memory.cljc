(ns fluree.db.conn.memory
  (:require [clojure.core.async :as async]
            [fluree.db.storage.core :as storage]
            [fluree.db.index :as index]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.util.core :as util
             #?@(:clj [:refer [try* catch*]])
             #?@(:cljs [:refer-macros [try* catch*]])]
            #?(:clj [fluree.db.full-text :as full-text])
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.util.async :refer [<? go-try channel?]]
            [fluree.db.platform :as platform]
            [fluree.db.conn.cache :as conn-cache]
            [fluree.db.conn.state-machine :as state-machine]
            [fluree.db.indexer.default :as idx-default]
            [fluree.json-ld :as json-ld]
            [fluree.crypto :as crypto]))

#?(:clj (set! *warn-on-reflection* true))

;; Memory Connection object

(defn memory-address
  "Turn a path into a fluree memory address."
  [path]
  (str "fluree:memory://" path))

(defn- address-path
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
      #?(:cljs (when platform/BROWSER
                 (.setItem js/localStorage hash json)))
      (swap! data-atom assoc hash commit-data)
      {:name    hash
       :hash    hash
       :size    (count json)
       :address (memory-address hash)})))

(defn read-address
  [data-atom address]
  #?(:clj (get @data-atom (address-path address))
     :cljs (or (get @data-atom (address-path address))
               (and platform/BROWSER (.getItem js/localStorage (address-path address))))))

(defn read-commit
  [data-atom address]
  (let [commit (read-address data-atom address)]
    #?(:cljs (if (and platform/BROWSER (string? commit))
               (js->clj (.parse js/JSON commit))
               commit)
       :clj commit)))

(defn push!
  [data-atom publish-address ledger-data]
  (let [commit-address (:address ledger-data)
        commit-path (address-path commit-address)
        address-path (address-path publish-address)]
    (swap! data-atom
           (fn [state]
             (let [commit (get state commit-path)]
               (when-not commit
                 (throw (ex-info (str "Unable to locate commit in memory, cannot push!: " commit-address)
                                 {:status 500 :error :db/invalid-db})))
               (log/debug "pushing:" publish-address "referencing commit:" commit-address)
               (assoc state address-path commit))))
    #?(:cljs (and platform/BROWSER (.setItem js/localStorage address-path commit-path))))
  ledger-data)


(defrecord MemoryConnection [id transactor? memory state
                             ledger-defaults async-cache
                             local-read local-write
                             parallelism close-fn
                             msg-in-ch msg-out-ch
                             ipfs-endpoint data-atom]

  conn-proto/iStorage
  (-c-read [_ commit-key] (async/go (read-commit data-atom commit-key)))
  (-c-write [_ commit-data] (c-write! data-atom commit-data))

  conn-proto/iNameService
  (-pull [this ledger] :TODO)
  (-subscribe [this ledger] :TODO)
  (-push [this address ledger-data] (async/go (push! data-atom address ledger-data)))
  (-lookup [this head-commit-address]
    (async/go #?(:clj (throw (ex-info (str "Cannot lookup ledger address with memory connection: " head-commit-address)
                                      {:status 500 :error :db/invalid-ledger}))
                 :cljs
                 (if platform/BROWSER
                   (if-let [head-commit (read-address data-atom head-commit-address)]
                     (memory-address head-commit)
                     (throw (ex-info (str "Unable to lookup ledger address from localStorage: "
                                          head-commit-address)
                                     {:status 500 :error :db/missing-head})))
                   (throw (ex-info (str "Cannot lookup ledger address with memory connection: "
                                        head-commit-address)
                                   {:status 500 :error :db/invalid-ledger}))))))
  (-address [_ ledger-alias {:keys [branch] :as _opts}]
    (async/go (memory-address (str ledger-alias "/" (name branch) "/" "HEAD"))))

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
    #?(:clj (throw (ex-info (str "Memory connection does not support storage reads. Requested key: " k)
                            {:status 500 :error :db/unexpected-error}))
       :cljs (if platform/BROWSER

               (throw (ex-info (str "Memory connection does not support storage reads. Requested key: " k)
                               {:status 500 :error :db/unexpected-error})))))
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

(defn ledger-defaults
  "Normalizes ledger defaults settings"
  [{:keys [context did] :as defaults}]
  (async/go
    {:context context
     :did     did}))

(defn connect
  "Creates a new memory connection."
  [{:keys [local-read local-write parallelism async-cache memory defaults]}]
  (go-try
    (let [ledger-defaults    (<? (ledger-defaults defaults))
          conn-id            (str (random-uuid))
          data-atom          (atom {})
          state              (state-machine/blank-state)
          async-cache-fn     (or async-cache
                                 (conn-cache/default-async-cache-fn memory))
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
