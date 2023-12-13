(ns fluree.db.conn.memory
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.db.storage :as storage]
            [fluree.db.index :as index]
            [fluree.db.util.context :as ctx-util]
            [fluree.db.nameservice.memory :as ns-memory]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log :include-macros true]
            #?(:clj [fluree.db.full-text :as full-text])
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.platform :as platform]
            [fluree.db.conn.cache :as conn-cache]
            [fluree.db.conn.core :as conn-core]
            [fluree.db.indexer.default :as idx-default]
            [fluree.json-ld :as json-ld]
            [fluree.crypto :as crypto])
  #?(:clj (:import (java.io Writer))))

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
     :json    json
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

(defn close
  [id state]
  (log/info "Closing memory connection" id)
  (swap! state assoc :closed? true))

(defrecord MemoryConnection [id memory state ledger-defaults lru-cache-atom
                             parallelism msg-in-ch msg-out-ch nameservices data-atom]

  conn-proto/iStorage
  (-c-read [_ commit-key] (go (read-commit data-atom commit-key)))
  (-c-write [_ _ledger commit-data] (go (write-commit! data-atom commit-data)))
  (-ctx-write [_ _ledger context-data] (go (write-context! data-atom context-data)))
  (-ctx-read [_ context-key] (go (read-context data-atom context-key)))

  conn-proto/iConnection
  (-close [_] (close id state))
  (-closed? [_] (boolean (:closed? @state)))
  (-method [_] :memory)
  (-parallelism [_] parallelism)
  (-id [_] id)
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
  (-nameservices [_] nameservices)
  (-state [_] @state)
  (-state [_ ledger] (get @state ledger))

  index/Resolver
  (resolve
    [_ node]
    ;; all root index nodes will be empty

    (storage/resolve-empty-node node))

  #?@(:clj
      [full-text/IndexConnection
       (open-storage [conn network dbid lang]
         (throw (ex-info "Memory connection does not support full text operations."
                         {:status 500 :error :db/unexpected-error})))]))

#?(:cljs
   (extend-type MemoryConnection
     IPrintWithWriter
     (-pr-writer [conn w opts]
       (-write w "#MemoryConnection ")
       (-write w (pr (conn-core/printer-map conn))))))

#?(:clj
   (defmethod print-method MemoryConnection [^MemoryConnection conn, ^Writer w]
     (.write w (str "#MemoryConnection "))
     (binding [*out* w]
       (pr (conn-core/printer-map conn)))))

(defn ledger-defaults
  "Normalizes ledger defaults settings"
  [{:keys [did] :as _defaults}]
  (async/go
    {:did did}))

(defn default-memory-nameservice
  "Returns memory nameservice"
  [data-atom]
  (ns-memory/initialize data-atom))

(defn connect
  "Creates a new memory connection."
  [{:keys [parallelism lru-cache-atom memory defaults nameservices]}]
  (go-try
    (let [ledger-defaults (<? (ledger-defaults defaults))
          conn-id         (str (random-uuid))
          data-atom       (atom {})
          state           (conn-core/blank-state)
          nameservices*   (util/sequential
                            (or nameservices
                                (default-memory-nameservice data-atom)))
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
                              :nameservices    nameservices*
                              :lru-cache-atom  lru-cache-atom}))))
