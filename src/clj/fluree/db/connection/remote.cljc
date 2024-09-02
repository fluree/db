(ns fluree.db.connection.remote
  (:require [fluree.db.util.core :as util]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.connection :as connection]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.db.cache :as cache]
            [fluree.db.method.remote :as remote]
            [fluree.db.nameservice.remote :as ns-remote]
            [clojure.string :as str])
  #?(:clj (:import (java.io Writer))))

(defrecord RemoteConnection [id server-state state lru-cache-atom serializer
                             nameservices ledger-defaults parallelism]
  connection/iStorage
  (-c-read [_ commit-key]
    (remote/remote-read server-state commit-key false))

  connection/iConnection
  (-did [_] (:did ledger-defaults))
  (-nameservices [_] nameservices))

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
          cache-size      (cache/memory->cache-size cache-max-mb)
          lru-cache-atom  (or lru-cache-atom (atom (cache/create-lru-cache
                                                     cache-size)))]
      (map->RemoteConnection {:id              conn-id
                              :server-state    server-state
                              :state           state
                              :lru-cache-atom  lru-cache-atom
                              :serializer      serializer
                              :ledger-defaults defaults
                              :parallelism     parallelism
                              :nameservices    nameservices*}))))
