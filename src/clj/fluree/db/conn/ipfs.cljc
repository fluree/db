(ns fluree.db.conn.ipfs
  (:require [fluree.db.storage :as storage]
            [fluree.db.index :as index]
            [fluree.db.util.context :as ctx-util]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.method.ipfs.core :as ipfs]
            [fluree.db.util.async :refer [<? go-try]]
            [clojure.core.async :as async :refer [go <! chan]]
            [fluree.db.conn.core :as conn-core]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.db.method.ipfs.keys :as ipfs-keys]
            [fluree.db.indexer.default :as idx-default]
            [fluree.db.conn.cache :as conn-cache])
  #?(:clj (:import (java.io Writer))))

#?(:clj (set! *warn-on-reflection* true))

(defn close
  [id state]
  (log/info "Closing IPFS Connection" id)
  (swap! state assoc :closed? true))

;; IPFS Connection object

(defrecord IPFSConnection [id state ledger-defaults lru-cache-atom
                           serializer parallelism msg-in-ch msg-out-ch
                           ipfs-endpoint]

  conn-proto/iStorage
  (-c-read [_ commit-key]
    (ipfs/read ipfs-endpoint commit-key))
  (-c-write [_ _ commit-data]
    (ipfs/write ipfs-endpoint commit-data))
  (-ctx-read [_ context-key]
    (ipfs/read ipfs-endpoint context-key))
  (-ctx-write [_ _ context-data]
    (ipfs/write ipfs-endpoint context-data))

  conn-proto/iConnection
  (-close [_] (close id state))
  (-closed? [_] (boolean (:closed? @state)))
  (-method [_] :ipfs)
  (-parallelism [_] parallelism)
  (-id [_] id)
  (-default-context [_] (:context ledger-defaults))
  (-default-context [_ context-type] (let [ctx (:context ledger-defaults)]
                                       (if (= :keyword context-type)
                                         (ctx-util/keywordize-context ctx)
                                         ctx)))
  (-context-type [_] (:context-type ledger-defaults))
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
  "Normalizes ledger defaults settings"
  [ipfs-endpoint {:keys [ipns context context-type did indexer] :as _defaults}]
  (go-try
    (let [ipns-default-key     (or (:key ipns) "self")
          ipns-default-address (<? (ipfs-keys/address ipfs-endpoint ipns-default-key))
          new-indexer-fn       (cond
                                 (fn? indexer)
                                 indexer

                                 (or (map? indexer) (nil? indexer))
                                 (fn [opts]
                                   (idx-default/create (merge indexer opts)))

                                 :else
                                 (throw (ex-info (str "Expected an indexer constructor fn or "
                                                      "default indexer options map. Provided: " indexer)
                                                 {:status 400 :error :db/invalid-ipfs-connection})))]
      (when-not ipns-default-address
        (throw (ex-info (str "IPNS publishing appears to have an issue. No corresponding ipns address found for key: "
                             ipns-default-key)
                        {:status 400 :error :db/ipfs-keys})))
      {:ipns         {:key     ipns-default-key
                      :address ipns-default-address}
       :context      (ctx-util/stringify-context context)
       :context-type context-type
       :did          did
       :indexer      new-indexer-fn})))


(defn connect
  "Creates a new IPFS connection."
  [{:keys [server parallelism lru-cache-atom memory defaults serializer]
    :or   {server     "http://127.0.0.1:5001/"
           serializer (json-serde)}}]
  (go-try
    (let [ipfs-endpoint   (or server "http://127.0.0.1:5001/") ;; TODO - validate endpoint looks like a good URL and ends in a '/' or add it
          ledger-defaults (<? (ledger-defaults ipfs-endpoint defaults))
          memory          (or memory 1000000) ;; default 1MB memory
          conn-id         (str (random-uuid))
          state           (conn-core/blank-state)

          cache-size      (conn-cache/memory->cache-size memory)
          lru-cache-atom  (or lru-cache-atom (atom (conn-cache/create-lru-cache cache-size)))]
      ;; TODO - need to set up monitor loops for async chans
      (map->IPFSConnection {:id              conn-id
                            :ipfs-endpoint   ipfs-endpoint
                            :ledger-defaults ledger-defaults
                            :serializer      serializer
                            :parallelism     parallelism
                            :msg-in-ch       (chan)
                            :msg-out-ch      (chan)
                            :state           state
                            :lru-cache-atom  lru-cache-atom}))))
