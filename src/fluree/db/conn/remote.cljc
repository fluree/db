(ns fluree.db.conn.remote
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.db.storage :as storage]
            [fluree.db.index :as index]
            [fluree.db.util.context :as ctx-util]
            [fluree.db.util.log :as log :include-macros true]
            #?(:clj [fluree.db.full-text :as full-text])
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.db.conn.cache :as conn-cache]
            [fluree.db.conn.state-machine :as state-machine]
            [clojure.string :as str]
            [fluree.db.util.xhttp :as xhttp]))

(defn pick-server
  "Currently does just a round-robin selection if multiple servers are given.
  TODO - add re-tries with a different server if fails to connect. Consider keeping stats to select optimal server."
  [servers]
  (rand-nth servers))

(defn remote-read
  "Returns a core async channel with value of remote resource."
  [state servers commit-key keywordize-keys?]
  (log/debug "[remote conn] remote read initiated for: " commit-key)
  (xhttp/post-json (str (pick-server servers) "/fluree/remoteResource")
                   {:resource commit-key}
                   {:keywordize-keys keywordize-keys?}))

;; NOTE, below function works in conjunction with message broadcasting (not in current PR)
#_(defn remote-read
  "Returns a core async channel with value of remote resource."
  [state servers commit-key keywordize-keys?]
  (log/debug "[remote conn] remote read initiated for: " commit-key)
  (if-let [cached (get-in @state [:resource commit-key])]
      (go cached)
      (xhttp/post-json (str (pick-server servers) "/fluree/remoteResource")
                       {:resource commit-key}
                       {:keywordize-keys keywordize-keys?})))

(defn remote-lookup
  [state servers ledger-address]
  (go-try
    (let [head-commit  (<? (remote-read state servers ledger-address false))
          head-address (get head-commit "address")]
      head-address)))

;; NOTE, below function works in conjunction with message broadcasting (not in current PR)
#_(defn remote-lookup
  [state servers ledger-address]
  (go-try
    (or (get-in @state [:lookup ledger-address])
          (let [head-commit  (<? (remote-read state servers ledger-address false))
                head-address (get head-commit "address")]
            (swap! state assoc-in [:lookup ledger-address] head-address)
            (swap! state assoc-in [:resource head-address] head-commit)
            head-address))))

(defn remote-ledger-exists?
  [state servers ledger-address]
  (go-try
    (boolean
      (<? (remote-lookup state servers ledger-address)))))


(defrecord RemoteConnection [id servers state lru-cache-atom serializer
                             ledger-defaults parallelism msg-in-ch msg-out-ch]

  conn-proto/iStorage
  (-c-read [_ commit-key] (remote-read state servers commit-key false))
  (-ctx-read [_ context-key] (remote-read state servers context-key false))
  (-index-file-read [_ index-address] (remote-read state servers index-address true))

  conn-proto/iNameService
  (-pull [this ledger] :TODO)
  (-subscribe [this ledger] :TODO)
  (-alias [this address]
    address)
  (-lookup [this ledger-alias]
    (remote-lookup state servers ledger-alias))
  (-address [_ ledger-alias {:keys [branch] :or {branch :main} :as _opts}]
    (go (str ledger-alias "/" (name branch) "/head")))
  (-exists? [_ ledger-address]
    (remote-ledger-exists? state servers ledger-address))

  conn-proto/iConnection
  (-close [_]
    (log/info "Closing memory connection" id)
    (swap! state assoc :closed? true))
  (-closed? [_] (boolean (:closed? @state)))
  (-method [_] :remote)
  (-parallelism [_] parallelism)
  (-id [_] id)
  (-default-context [_] (:context ledger-defaults))
  (-did [_] (:did ledger-defaults))
  (-msg-in [_ msg] (go-try
                     ;; TODO - push into state machine
                     (log/warn "-msg-in: " msg)
                     :TODO))
  (-msg-out [_ msg] (go-try
                      ;; TODO - register/submit event
                      (log/warn "-msg-out: " msg)
                      :TODO))
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
                                        (fn [] (conn-cache/lru-evict lru-cache-atom cache-key))))))))

  #?@(:clj
      [full-text/IndexConnection
       (open-storage [conn network dbid lang]
         (throw (ex-info "Memory connection does not support full text operations."
                         {:status 500 :error :db/unexpected-error})))]))

(defn ledger-defaults
  "Normalizes ledger defaults settings"
  [{:keys [context did context-type] :as _defaults}]
  (async/go
    {:context      (ctx-util/stringify-context context)
     :context-type context-type
     :did          did}))


(defn connect
  "Creates a new memory connection."
  [{:keys [parallelism lru-cache-atom memory defaults servers serializer]
    :or {serializer (json-serde)}
    :as opts}]
  (go-try
    (let [ledger-defaults (<? (ledger-defaults defaults))
          servers*        (str/split servers #",")
          conn-id         (str (random-uuid))
          state           (state-machine/blank-state)
          cache-size      (conn-cache/memory->cache-size memory)
          lru-cache-atom  (or lru-cache-atom (atom (conn-cache/create-lru-cache
                                                     cache-size)))]
      (map->RemoteConnection {:id              conn-id
                              :servers         servers*
                              :state           state
                              :lru-cache-atom  lru-cache-atom
                              :serializer      serializer
                              :ledger-defaults ledger-defaults
                              :parallelism     parallelism
                              :msg-in-ch       (async/chan)
                              :msg-out-ch      (async/chan)}))))