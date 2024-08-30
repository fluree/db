(ns fluree.db.connection.memory
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.db.flake.index.storage :as index-storage]
            [fluree.db.flake.index :as index]
            [fluree.db.nameservice.storage-backed :as storage-ns]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.connection :as connection]
            [fluree.db.util.async :refer [ go-try]]
            [fluree.db.cache :as cache]
            [fluree.db.storage :as storage]
            [fluree.db.storage.memory :as memory-storage]
            #?(:cljs [fluree.db.platform :as platform]))
  #?(:clj (:import (java.io Writer))))

#?(:clj (set! *warn-on-reflection* true))

(defn close
  [id state]
  (log/info "Closing memory connection" id)
  (swap! state assoc :closed? true))

(defrecord MemoryConnection [id memory state ledger-defaults lru-cache-atom store
                             parallelism msg-in-ch msg-out-ch nameservices data-atom]

  connection/iStorage
  (-c-read [_ commit-key]
    (storage/read-json store commit-key))
  (-c-write [_ _ledger-alias commit-data]
    (storage/content-write-json store "commit" commit-data))
  (-txn-read [_ txn-key]
    (storage/read-json store txn-key))
  (-txn-write [_ _ledger-alias txn-data]
    (storage/content-write-json store "transaction" txn-data))

  connection/iConnection
  (-did [_] (:did ledger-defaults))
  (-nameservices [_] nameservices)

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

(defn connect
  "Creates a new memory connection."
  [{:keys [parallelism lru-cache-atom cache-max-mb defaults nameservices]}]
  (go-try
    (let [conn-id         (str (random-uuid))
          state           (connection/blank-state)
          mem-store       (memory-storage/create)
          nameservices*   (util/sequential
                            (or nameservices
                                (storage-ns/start "fluree:memory://" mem-store true)))
          cache-size      (cache/memory->cache-size cache-max-mb)
          lru-cache-atom  (or lru-cache-atom (atom (cache/create-lru-cache
                                                     cache-size)))]
      (map->MemoryConnection {:id              conn-id
                              :ledger-defaults defaults
                              :store           mem-store
                              :parallelism     parallelism
                              :msg-in-ch       (async/chan)
                              :msg-out-ch      (async/chan)
                              :memory          true
                              :state           state
                              :nameservices    nameservices*
                              :lru-cache-atom  lru-cache-atom}))))
