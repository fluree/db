(ns fluree.db.conn.memory
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.db.indexer.storage :as index-storage]
            [fluree.db.index :as index]
            [fluree.db.nameservice.memory :as ns-memory]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.connection :as connection]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.conn.cache :as conn-cache]
            [fluree.json-ld :as json-ld]
            [fluree.db.storage :as storage]
            [fluree.db.storage.memory :as memory-storage]
            #?(:cljs [fluree.db.platform :as platform]))
  #?(:clj (:import (java.io Writer))))

#?(:clj (set! *warn-on-reflection* true))

;; Memory Connection object

(defn- write-data!
  [store type data]
  (go-try
    (let [{:keys [address path hash size]}
          (<? (storage/write store type data))]
      {:name    path
       :hash    hash
       :size    size
       :address address})))

(defn- read-data
  [store address]
  (go-try
    (let [data (<? (storage/read store address))]
      #?(:cljs (if (and platform/BROWSER (string? data))
                 (js->clj (.parse js/JSON data))
                 data)
         :clj  data))))

(defn close
  [id state]
  (log/info "Closing memory connection" id)
  (swap! state assoc :closed? true))

(defrecord MemoryConnection [id memory state ledger-defaults lru-cache-atom store
                             parallelism msg-in-ch msg-out-ch nameservices data-atom]

  connection/iStorage
  (-c-read [_ commit-key] (read-data store commit-key))
  (-c-write [_ _ledger-alias commit-data] (write-data! store :commit commit-data))
  (-txn-read [_ txn-key] (read-data store txn-key))
  (-txn-write [_ _ledger-alias txn-data] (write-data! store :transaction txn-data))

  connection/iConnection
  (-close [_] (close id state))
  (-closed? [_] (boolean (:closed? @state)))
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

    (index-storage/resolve-empty-node node)))

#?(:cljs
   (extend-type MemoryConnection
     IPrintWithWriter
     (-pr-writer [conn w opts]
       (-write w "#MemoryConnection ")
       (-write w (pr (connection/printer-map conn))))))

#?(:clj
   (defmethod print-method MemoryConnection [^MemoryConnection conn, ^Writer w]
     (.write w (str "#MemoryConnection "))
     (binding [*out* w]
       (pr (connection/printer-map conn)))))

(defn ledger-defaults
  "Normalizes ledger defaults settings"
  [{:keys [did] :as _defaults}]
  (go
    {:did did}))

(defn default-memory-nameservice
  "Returns memory nameservice"
  [store]
  (ns-memory/initialize store))

(defn connect
  "Creates a new memory connection."
  [{:keys [parallelism lru-cache-atom memory defaults nameservices]}]
  (go-try
    (let [ledger-defaults (<? (ledger-defaults defaults))
          conn-id         (str (random-uuid))
          state           (connection/blank-state)
          mem-store       (memory-storage/create)
          nameservices*   (util/sequential
                            (or nameservices
                                ;; TODO: We should not reach inside the storage
                                ;; implementation to reuse the contents atom
                                ;; because that breaks the abstraction and
                                ;; implicitly couples components that should be
                                ;; independent. We should change the memory
                                ;; nameservice to either use a storage
                                ;; implementation explicitly, or to use an atom
                                ;; independent of the data held in commit and
                                ;; index storage.
                                (default-memory-nameservice (:contents mem-store))))
          cache-size      (conn-cache/memory->cache-size memory)
          lru-cache-atom  (or lru-cache-atom (atom (conn-cache/create-lru-cache
                                                     cache-size)))]
      (map->MemoryConnection {:id              conn-id
                              :ledger-defaults ledger-defaults
                              :store           mem-store
                              :parallelism     parallelism
                              :msg-in-ch       (async/chan)
                              :msg-out-ch      (async/chan)
                              :memory          true
                              :state           state
                              :nameservices    nameservices*
                              :lru-cache-atom  lru-cache-atom}))))
