(ns fluree.db.conn.memory
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.db.storage.core :as storage]
            [fluree.db.index :as index]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log :include-macros true]
            #?(:clj [fluree.db.full-text :as full-text])
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.util.async :refer [<? go-try channel?]]
            [fluree.db.platform :as platform]
            [fluree.db.conn.cache :as conn-cache]
            [fluree.db.conn.state-machine :as state-machine]
            [fluree.db.indexer.default :as idx-default]
            [fluree.json-ld :as json-ld]
            [clojure.string :as str]
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

(defn- write-data!
  [data-atom data]
  (let [json (json-ld/normalize-data data)
        hash (crypto/sha2-256-normalize json)
        path hash]
    #?(:cljs (when platform/BROWSER
               (.setItem js/localStorage hash json)))
    (swap! data-atom assoc hash data)
    {:name    hash
     :hash    hash
     :size    (count json)
     :address (memory-address path)}))

(defn write-commit!
  [data-atom commit-data]
  (write-data! data-atom commit-data))

(defn- read-address
  [data-atom address]
  (let [addr-path (address-path address)]
    #?(:clj  (get @data-atom addr-path)
       :cljs (or (get @data-atom addr-path)
                 (and platform/BROWSER (.getItem js/localStorage addr-path))))))

(defn- read-data
  [data-atom address]
  (let [data (read-address data-atom address)]
    #?(:cljs (if (and platform/BROWSER (string? data))
               (js->clj (.parse js/JSON data))
               data)
       :clj  data)))

(defn read-commit
  [data-atom address]
  (read-data data-atom address))

(defn write-context!
  [data-atom context-data]
  (write-data! data-atom context-data))

(defn read-context
  [data-atom context-key]
  (read-data data-atom context-key))

(defn push!
  [data-atom publish-address {commit-address :address :as ledger-data}]
  (let [commit-path (address-path commit-address)
        head-path   (address-path publish-address)]
    (swap! data-atom
           (fn [state]
             (let [commit (get state commit-path)]
               (when-not commit
                 (throw (ex-info (str "Unable to locate commit in memory, cannot push!: " commit-address)
                                 {:status 500 :error :db/invalid-db})))
               (log/debug "pushing:" publish-address "referencing commit:" commit-address)
               (let [commit (assoc commit "address" commit-address)]
                 (assoc state head-path commit)))))
    #?(:cljs (and platform/BROWSER (.setItem js/localStorage address-path commit-path)))
    ledger-data))


(defrecord MemoryConnection [id memory state ledger-defaults lru-cache-atom
                             parallelism msg-in-ch msg-out-ch data-atom]

  conn-proto/iStorage
  (-c-read [_ commit-key] (go (read-commit data-atom commit-key)))
  (-c-write [_ _ledger commit-data] (go (write-commit! data-atom commit-data)))
  (-ctx-write [_ _ledger context-data] (go (write-context! data-atom context-data)))
  (-ctx-read [_ context-key] (go (read-context data-atom context-key)))

  conn-proto/iNameService
  (-pull [this ledger] :TODO)
  (-subscribe [this ledger] :TODO)
  (-push [this address ledger-data] (go (push! data-atom address ledger-data)))
  (-alias [this address]
    (-> (address-path address)
        (str/split #"/")
        (->> (drop 2)
             (str/join "/"))))
  (-lookup [this head-commit-address]
    (go #?(:clj
           (if-let [head-commit (read-address data-atom head-commit-address)]
             (-> head-commit (get "address"))
             (throw (ex-info (str "Unable to lookup ledger address from conn: "
                                  head-commit-address)
                             {:status 500 :error :db/missing-head})))

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
  (-address [_ ledger-alias {:keys [branch] :or {branch :main} :as _opts}]
    (go (memory-address (str ledger-alias "/" (name branch) "/head"))))
  (-exists? [_ ledger-address]
    (go (boolean (read-address data-atom ledger-address))))

  conn-proto/iConnection
  (-close [_]
    (log/info "Closing memory connection" id)
    (swap! state assoc :closed? true))
  (-closed? [_] (boolean (:closed? @state)))
  (-method [_] :memory)
  (-parallelism [_] parallelism)
  (-id [_] id)
  (-context [_] (:context ledger-defaults))
  (-new-indexer [_ opts] (idx-default/create opts)) ;; default new ledger indexer
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
  [{:keys [context-type context did] :as _defaults}]
  (async/go
    {:context (util/normalize-context context-type context)
     :did     did}))

(defn connect
  "Creates a new memory connection."
  [{:keys [parallelism lru-cache-atom memory defaults]}]
  (go-try
    (let [ledger-defaults (<? (ledger-defaults defaults))
          conn-id         (str (random-uuid))
          data-atom       (atom {})
          state           (state-machine/blank-state)

          cache-size      (conn-cache/memory->cache-size memory)
          lru-cache-atom  (or lru-cache-atom (atom (conn-cache/create-lru-cache
                                                     cache-size)))]
      (map->MemoryConnection {:id              conn-id
                              :ledger-defaults ledger-defaults
                              :data-atom       data-atom
                              :parallelism     parallelism
                              :msg-in-ch       (async/chan)
                              :msg-out-ch      (async/chan)
                              :memory          true
                              :state           state
                              :lru-cache-atom  lru-cache-atom}))))
