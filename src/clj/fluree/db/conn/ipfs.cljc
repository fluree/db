(ns fluree.db.conn.ipfs
  (:require [fluree.db.storage.ipfs :as ipfs-storage]
            [clojure.string :as str]
            [fluree.db.indexer.storage :as index-storage]
            [fluree.db.flake.index :as index]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.connection :as connection]
            [fluree.db.util.async :refer [<? go-try]]
            [clojure.core.async :as async :refer [chan]]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.db.nameservice.ipns :as ns-ipns]
            [fluree.db.nameservice.filesystem :as ns-filesystem]
            [fluree.db.conn.cache :as conn-cache]
            [fluree.db.storage :as storage])
  #?(:clj (:import (java.io Writer))))

#?(:clj (set! *warn-on-reflection* true))

(def default-ipfs-server "http://127.0.0.1:5001/")

(defn close
  [id state]
  (log/info "Closing IPFS Connection" id)
  (swap! state assoc :closed? true))

;; IPFS Connection object

(defrecord IPFSConnection [id state ledger-defaults lru-cache-atom serializer
                           parallelism msg-in-ch msg-out-ch nameservices
                           ipfs-endpoint store]

  connection/iStorage
  (-c-read [_ commit-key]
    (storage/read store commit-key))
  (-c-write [_ _ commit-data]
    (storage/write store "commit" commit-data))
  (-txn-read [_ txn-key]
    (storage/read store txn-key))
  (-txn-write [_ _ txn-data]
    (storage/write store "txn" txn-data))

  connection/iConnection
  (-close [_] (close id state))
  (-closed? [_] (boolean (:closed? @state)))
  (-did [_] (:did ledger-defaults))
  ;; (-msg-in [_ msg] (go-try
  ;;                    ;; TODO - push into state machine
  ;;                    (log/warn "-msg-in: " msg)
  ;;                    :TODO))
  ;; (-msg-out [_ msg] (go-try
  ;;                     ;; TODO - register/submit event
  ;;                     (log/warn "-msg-out: " msg)
  ;;                     :TODO))
  (-nameservices [_] nameservices)
  (-state [_] @state)
  (-state [_ ledger] (get @state ledger))

  index/Resolver
  (resolve
    [conn node]
    (index-storage/index-resolver conn lru-cache-atom node)))

#?(:cljs
   (extend-type IPFSConnection
     IPrintWithWriter
     (-pr-writer [conn w opts]
       (-write w "#IPFSConnection ")
       (-write w (pr (connection/printer-map conn))))))

#?(:clj
   (defmethod print-method IPFSConnection [^IPFSConnection conn, ^Writer w]
     (.write w (str "#IPFSConnection "))
     (binding [*out* w]
       (pr (connection/printer-map conn)))))

(defn validate-http-url
  "Tests that URL starts with http:// or https://, returns exception or
  original url if passes. Appends a '/' at the end if not already present."
  [url]
  (if (and (string? url)
           (re-matches #"^https?://(.*)" url))
    (if (str/ends-with? url "/")
      url
      (str url "/"))
    (throw (ex-info (str "Invalid IPFS endpoint: " url)
                    {:status 400 :error :db/invalid-ipfs-endpoint}))))

(defn default-ipns-nameservice
  [{:keys [profile server]} default-ipfs-endpoint]
  (let [server* (if server
                  (validate-http-url server)
                  default-ipfs-endpoint)]
    (ns-ipns/initialize server* profile)))

(defn default-file-nameservice
  [{:keys [path base-address sync?]}]
  (ns-filesystem/initialize path {:base-address base-address
                                  :sync? sync?}))

(defn connect
  "Creates a new IPFS connection.
  Options include:
  - ipns-nameservice - default: {:profile 'self', :server <connection 'server'>, :sync? false}
           Map of IPNS nameservice options. If you don't want to6 use IPNS, set
           value to 'nil'. Note IPNS is always asynchronous and will update when
           it can, and is not guaranteed to persist over time. It should be a
           secondary nameservice whenever used for a ledger receiving frequent
           updates or timeliness is important.
  - file-nameservice - default: {:path 'data/ns', :base-address 'fluree:file://', :sync? true}
           Map of file nameservice options. If you don't want to use file
           nameservice, set value to 'nil'. File nameservice is synchronous if
           enabled and commits will not succeed until the ns file is written to
           disk. It should be used as the primary nameservice whenever used for
           a ledger receiving frequent updates or timeliness is important."
  [{:keys [server parallelism lru-cache-atom cache-max-mb defaults serializer
           ;; only include if you want to configure custom nameservice(s)
           nameservices
           ;; if not providing preconfigured 'nameservices', ipns + file nameservices used
           ipns-nameservice file-nameservice]
    :or   {server           default-ipfs-server
           serializer       (json-serde)
           ipns-nameservice {:profile "self"
                             :server  nil ;; :server will default to 'server' param
                             :sync?   false}
           file-nameservice {:path         "data/ns"
                             :base-address "fluree:file://"
                             :sync?        true}}}]
  (go-try
    (let [ipfs-endpoint   (validate-http-url server)
          conn-id         (str (random-uuid))
          state           (connection/blank-state)
          nameservices*   (if nameservices ;; provided pre-configured nameservices to connection
                            (util/sequential nameservices)
                            (cond-> [] ;; utilize default nameservices with provided config options
                                    ipns-nameservice (conj (<? (default-ipns-nameservice ipns-nameservice ipfs-endpoint)))
                                    file-nameservice (conj (default-file-nameservice file-nameservice))))
          cache-size      (conn-cache/memory->cache-size cache-max-mb)
          lru-cache-atom  (or lru-cache-atom (atom (conn-cache/create-lru-cache cache-size)))
          ipfs-store      (ipfs-storage/open ipfs-endpoint)]
      (when (empty? nameservices*)
        (throw (ex-info "At least one nameservice must be provided for IPFS connection."
                        {:status 400 :error :db/invalid-nameservices})))
      (when (> (count (filter :sync? nameservices*)) 1)
        (throw (ex-info (str "More than one synchronous nameservice configured for IPFS connection. "
                             "There must be only one.")
                        {:status 400 :error :db/invalid-nameservices})))
      (when (every? #(not (:sync? %)) nameservices*)
        (log/warn (str "No synchronous nameservice provided for IPFS connection. "
                       "This can results in unregistered commits in a failure scenario.")))
      ;; TODO - need to set up monitor loops for async chans
      (map->IPFSConnection {:id              conn-id
                            :store           ipfs-store
                            :ipfs-endpoint   ipfs-endpoint
                            :ledger-defaults defaults
                            :serializer      serializer
                            :parallelism     parallelism
                            :msg-in-ch       (chan)
                            :msg-out-ch      (chan)
                            :state           state
                            :lru-cache-atom  lru-cache-atom
                            :nameservices    nameservices*}))))
