(ns fluree.db.conn.ipfs
  (:require [fluree.db.storage.ipfs :as ipfs-storage]
            [fluree.db.indexer.storage :as storage]
            [fluree.db.index :as index]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.util.async :refer [<? go-try]]
            [clojure.core.async :as async :refer [chan]]
            [fluree.db.conn.core :as conn-core]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.db.indexer.default :as idx-default]
            [fluree.db.nameservice.ipns :as ns-ipns]
            [fluree.db.conn.cache :as conn-cache]
            [fluree.db.storage :as store])
  #?(:clj (:import (java.io Writer))))

#?(:clj (set! *warn-on-reflection* true))

(defn close
  [id state]
  (log/info "Closing IPFS Connection" id)
  (swap! state assoc :closed? true))

;; IPFS Connection object

(defrecord IPFSConnection [id state ledger-defaults lru-cache-atom
                           serializer parallelism msg-in-ch msg-out-ch
                           nameservices ipfs-endpoint store]

  conn-proto/iStorage
  (-c-read [_ commit-key] (store/read store commit-key))
  (-c-write [_ _ commit-data] (store/write store "commit" commit-data nil))

  conn-proto/iConnection
  (-close [_] (close id state))
  (-closed? [_] (boolean (:closed? @state)))
  (-method [_] :ipfs)
  (-parallelism [_] parallelism)
  (-id [_] id)
  (-new-indexer [_ opts] ;; default new ledger indexer
    (let [indexer-fn (:indexer ledger-defaults)]
      (indexer-fn opts)))
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
   (extend-type IPFSConnection
     IPrintWithWriter
     (-pr-writer [conn w opts]
       (-write w "#IPFSConnection ")
       (-write w (pr (conn-core/printer-map conn))))))

#?(:clj
   (defmethod print-method IPFSConnection [^IPFSConnection conn, ^Writer w]
     (.write w (str "#IPFSConnection "))
     (binding [*out* w]
       (pr (conn-core/printer-map conn)))))

(defn ledger-defaults
  [{:keys [did indexer]}]
  {:did     did
   :indexer (cond
              (fn? indexer)
              indexer

              (or (map? indexer) (nil? indexer))
              (fn [opts]
                (idx-default/create (merge indexer opts)))

              :else
              (throw (ex-info (str "Expected an indexer constructor fn or "
                                   "default indexer options map. Provided: " indexer)
                              {:status 400 :error :db/invalid-file-connection})))})

(defn default-ipns-nameservice
  [ipfs-endpoint ipns-profile]
  (ns-ipns/initialize ipfs-endpoint ipns-profile))


(defn connect
  "Creates a new IPFS connection."
  [{:keys [server parallelism lru-cache-atom memory ipns defaults serializer nameservices]
    :or   {server     "http://127.0.0.1:5001/"
           serializer (json-serde)
           ipns       "self"}}]
  (go-try
    (let [ipfs-endpoint   server ; TODO - validate endpoint looks like a good URL and ends in a '/' or add it
          ledger-defaults (ledger-defaults defaults)
          memory          (or memory 1000000) ; default 1MB memory
          conn-id         (str (random-uuid))
          state           (conn-core/blank-state)
          nameservices*   (util/sequential
                            (or nameservices (<? (default-ipns-nameservice ipfs-endpoint ipns))))
          cache-size      (conn-cache/memory->cache-size memory)
          lru-cache-atom  (or lru-cache-atom (atom (conn-cache/create-lru-cache cache-size)))
          ipfs-store      (ipfs-storage/open ipfs-endpoint)]
      ;; TODO - need to set up monitor loops for async chans
      (map->IPFSConnection {:id              conn-id
                            :store           ipfs-store
                            :ipfs-endpoint   ipfs-endpoint
                            :ledger-defaults ledger-defaults
                            :serializer      serializer
                            :parallelism     parallelism
                            :msg-in-ch       (chan)
                            :msg-out-ch      (chan)
                            :state           state
                            :lru-cache-atom  lru-cache-atom
                            :nameservices    nameservices*}))))
