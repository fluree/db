(ns fluree.db.conn.remote
  (:require [clojure.core.async :as async :refer [go chan]]
            [fluree.db.storage :as storage]
            [fluree.db.index :as index]
            [fluree.db.util.context :as ctx-util]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.db.conn.cache :as conn-cache]
            [fluree.db.conn.core :as conn-core]
            [fluree.db.method.remote.core :as remote]
            [fluree.db.nameservice.remote :as ns-remote]
            [fluree.db.indexer.default :as idx-default]
            [clojure.string :as str])
  #?(:clj (:import (java.io Writer))))



(defrecord RemoteConnection [id server-state state lru-cache-atom serializer
                             nameservices ledger-defaults parallelism]

  conn-proto/iStorage
  (-c-read [_ commit-key] (remote/remote-read state server-state commit-key false))
  (-ctx-read [_ context-key] (remote/remote-read state server-state context-key false))
  (-index-file-read [_ index-address] (remote/remote-read state server-state index-address true))

  conn-proto/iConnection
  (-close [_]
    (log/info "Closing remote connection" id)
    (swap! state assoc :closed? true))
  (-closed? [_] (boolean (:closed? @state)))
  (-method [_] :remote)
  (-parallelism [_] parallelism)
  (-id [_] id)
  (-default-context [_] (:context ledger-defaults))
  (-default-context [_ context-type] (let [ctx (:context ledger-defaults)]
                                       (if (= :keyword context-type)
                                         (ctx-util/keywordize-context ctx)
                                         ctx)))
  (-context-type [_] (:context-type ledger-defaults))
  (-did [_] (:did ledger-defaults))
  (-msg-in [_ msg] (go-try
                     (log/warn "-msg-in: " msg)
                     :TODO))
  (-msg-out [_ msg] (go-try
                      ;; TODO - register/submit event
                      (log/warn "-msg-out: " msg)
                      :TODO))
  (-nameservices [_] nameservices)
  (-new-indexer [_ opts] (idx-default/create opts))
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
                                        (fn [] (conn-cache/lru-evict lru-cache-atom cache-key)))))))))

#?(:cljs
   (extend-type RemoteConnection
     IPrintWithWriter
     (-pr-writer [conn w opts]
       (-write w "#RemoteConnection ")
       (-write w (pr (conn-core/printer-map conn))))))

#?(:clj
   (defmethod print-method RemoteConnection [^RemoteConnection conn, ^Writer w]
     (.write w (str "#RemoteConnection "))
     (binding [*out* w]
       (pr (conn-core/printer-map conn)))))

(defn ledger-defaults
  "Normalizes ledger defaults settings"
  [{:keys [context did context-type] :as _defaults}]
  (go
    {:context      (ctx-util/stringify-context context)
     :context-type context-type
     :did          did}))

(defn default-remote-nameservice
  "Returns remote nameservice or will throw if generates an exception."
  [server-state state-atom]
  (ns-remote/initialize server-state state-atom))


(defn connect
  "Creates a new memory connection."
  [{:keys [parallelism lru-cache-atom memory defaults servers serializer nameservices]
    :or   {serializer (json-serde)}}]
  (go-try
    (let [ledger-defaults (<? (ledger-defaults defaults))
          servers*        (str/split servers #",")
          server-state    (atom {:servers      servers*
                                 :connected-to nil
                                 :stats        {:connected-at nil}})
          conn-id         (str (random-uuid))
          state           (conn-core/blank-state)
          nameservices*   (util/sequential
                            (or nameservices
                                ;; if default ns, and returns exception, throw - connection fails
                                ;; (likely due to unreachable server with websocket request)
                                (<? (default-remote-nameservice server-state state))))
          cache-size      (conn-cache/memory->cache-size memory)
          lru-cache-atom  (or lru-cache-atom (atom (conn-cache/create-lru-cache
                                                     cache-size)))]
      (map->RemoteConnection {:id              conn-id
                              :server-state    server-state
                              :state           state
                              :lru-cache-atom  lru-cache-atom
                              :serializer      serializer
                              :ledger-defaults ledger-defaults
                              :parallelism     parallelism
                              :nameservices    nameservices*}))))
