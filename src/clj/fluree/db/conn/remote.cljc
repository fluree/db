(ns fluree.db.conn.remote
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.db.flake.index.storage :as index-storage]
            [fluree.db.flake.index :as index]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.connection :as connection]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.db.conn.cache :as conn-cache]
            [fluree.db.method.remote.core :as remote]
            [fluree.db.nameservice.remote :as ns-remote]
            [clojure.string :as str])
  #?(:clj (:import (java.io Writer))))

(defn close
   [id state]
  (log/info "Closing remote connection" id)
  (swap! state assoc :closed? true))

(defrecord RemoteConnection [id server-state state lru-cache-atom serializer
                             nameservices ledger-defaults parallelism]

  connection/iStorage
  (-c-read [_ commit-key] (remote/remote-read state server-state commit-key false))
  (-txn-read [_ txn-key] (remote/remote-read state server-state txn-key false))
  (-index-file-read [_ index-address] (remote/remote-read state server-state index-address true))

  connection/iConnection
  (-close [_] (close id state))
  (-closed? [_] (boolean (:closed? @state)))
  (-did [_] (:did ledger-defaults))
  (-msg-in [_ msg] (go-try
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
    [conn node]
    (index-storage/index-resolver conn lru-cache-atom node)))

#?(:cljs
   (extend-type RemoteConnection
     IPrintWithWriter
     (-pr-writer [conn w opts]
       (-write w "#RemoteConnection ")
       (-write w (pr (connection/printer-map conn))))))

#?(:clj
   (defmethod print-method RemoteConnection [^RemoteConnection conn, ^Writer w]
     (.write w (str "#RemoteConnection "))
     (binding [*out* w]
       (pr (connection/printer-map conn)))))

(defn default-remote-nameservice
  "Returns remote nameservice or will throw if generates an exception."
  [server-state state-atom]
  (ns-remote/initialize server-state state-atom))


(defn connect
  "Creates a new memory connection."
  [{:keys [parallelism lru-cache-atom cache-max-mb defaults servers serializer nameservices]
    :or   {serializer (json-serde)}}]
  (go-try
    (let [servers*        (str/split servers #",")
          server-state    (atom {:servers      servers*
                                 :connected-to nil
                                 :stats        {:connected-at nil}})
          conn-id         (str (random-uuid))
          state           (connection/blank-state)
          nameservices*   (util/sequential
                            (or nameservices
                                ;; if default ns, and returns exception, throw - connection fails
                                ;; (likely due to unreachable server with websocket request)
                                (<? (default-remote-nameservice server-state state))))
          cache-size      (conn-cache/memory->cache-size cache-max-mb)
          lru-cache-atom  (or lru-cache-atom (atom (conn-cache/create-lru-cache
                                                     cache-size)))]
      (map->RemoteConnection {:id              conn-id
                              :server-state    server-state
                              :state           state
                              :lru-cache-atom  lru-cache-atom
                              :serializer      serializer
                              :ledger-defaults defaults
                              :parallelism     parallelism
                              :nameservices    nameservices*}))))
