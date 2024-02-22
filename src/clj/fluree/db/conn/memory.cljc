(ns fluree.db.conn.memory
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.db.indexer.storage :as storage]
            [fluree.db.index :as index]
            [fluree.db.nameservice.memory :as ns-memory]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.conn.cache :as conn-cache]
            [fluree.db.conn.core :as conn-core]
            [fluree.db.indexer.default :as idx-default]
            [fluree.json-ld :as json-ld]
            [fluree.crypto :as crypto]
            [fluree.db.storage :as store]
            #?(:cljs [fluree.db.platform :as platform]))
  #?(:clj (:import (java.io Writer))))

#?(:clj (set! *warn-on-reflection* true))

;; Memory Connection object

(defn- write-data!
  [store data]
  (go-try
    (let [json (json-ld/normalize-data data)
          hash (crypto/sha2-256 json)
          {path :k
           address :address
           size :size}
          (<? (store/write store hash data))]
      {:name    path
       :hash    hash
       :json    json
       :size    (count json)
       :address address})))

(defn- read-data
  [store address]
  (go-try
    (let [data (<? (store/read store address))]
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

  conn-proto/iStorage
  (-c-read [_ commit-key] (read-data store commit-key))
  (-c-write [_ _ledger commit-data] (write-data! store commit-data))

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

    (storage/resolve-empty-node node)))

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
  (go
    {:did did}))

(defn default-memory-nameservice
  "Returns memory nameservice"
  [store]
  (ns-memory/initialize store))

(defn connect
  "Creates a new memory connection."
  [{:keys [parallelism lru-cache-atom memory defaults nameservices store]}]
  (go-try
    (let [ledger-defaults (<? (ledger-defaults defaults))
          conn-id         (str (random-uuid))
          state           (conn-core/blank-state)
          nameservices*   (util/sequential
                            (or nameservices
                                (default-memory-nameservice (:storage-atom store))))
          cache-size      (conn-cache/memory->cache-size memory)
          lru-cache-atom  (or lru-cache-atom (atom (conn-cache/create-lru-cache
                                                     cache-size)))]
      (map->MemoryConnection {:id              conn-id
                              :ledger-defaults ledger-defaults
                              :store           store
                              :parallelism     parallelism
                              :msg-in-ch       (async/chan)
                              :msg-out-ch      (async/chan)
                              :memory          true
                              :state           state
                              :nameservices    nameservices*
                              :lru-cache-atom  lru-cache-atom}))))
