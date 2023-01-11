(ns fluree.db.conn.ipfs
  (:require [fluree.db.storage.core :as storage]
            [fluree.db.index :as index]
            [fluree.db.util.core :as util :refer [exception?]]
            #?(:clj [fluree.db.full-text :as full-text])
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.method.ipfs.core :as ipfs]
            [fluree.db.util.async :refer [<? go-try channel?]]
            [clojure.core.async :as async :refer [go <!]]
            [fluree.db.conn.state-machine :as state-machine]
            [#?(:cljs cljs.cache :clj clojure.core.cache) :as cache]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.db.method.ipfs.keys :as ipfs-keys]
            [fluree.db.method.ipfs.directory :as ipfs-dir]
            [fluree.db.indexer.default :as idx-default]
            [fluree.db.ledger.json-ld :as jld-ledger]
            [clojure.string :as str]))

#?(:clj (set! *warn-on-reflection* true))

;; IPFS Connection object

(defn get-address
  "Returns IPNS address for a given key."
  [{:keys [ipfs-endpoint ledger-defaults] :as _conn} ledger-alias opts]
  (go-try
    (log/debug "Getting address for ledger alias:" ledger-alias)
    (let [base-address (if-let [key (-> opts :ipns :key)]
                         (<? (ipfs-keys/address ipfs-endpoint key))
                         (-> ledger-defaults :ipns :address))]
      (str "fluree:ipns://" base-address "/" ledger-alias))))

(defn trim-slashes
  "Trims any leading or following slash '/' characters from string"
  [s]
  (when s
    (cond-> s
            (str/ends-with? s "/") (subs 0 (dec (count s)))
            (str/starts-with? s "/") (subs 1))))

(defn address-parts
  "Returns three-tuple of ipfs/ipns (protocol), address, and ledger alias(directory)
  If no match, returns nil.
  e.g. fluree:ipfs://QmZ9FQA7eHnnuTV5kjiaQKPf99NSPzk2pi1AMe6XkDa2P2
       ->> [QmZ9FQA7eHnnuTV5kjiaQKPf99NSPzk2pi1AMe6XkDa2P2 nil]
       fluree:ipns://bafybeibtk2qwvuvbawhcgrktkgbdfnor4qzxitk4ct5mfwmvbaao53awou/my/db
       ->> [bafybeibtk2qwvuvbawhcgrktkgbdfnor4qzxitk4ct5mfwmvbaao53awou my/db]"
  [address]
  (when-let [[_ proto address db] (re-find #"^fluree:([^:]+)://([^/]+)(/.+)?$" address)]
    [proto address (trim-slashes db)]))

(defn lookup-address
  "Given IPNS address, performs lookup and returns latest ledger address."
  [{:keys [ipfs-endpoint] :as _conn} ledger-name]
  (go-try
    (if-let [[proto address ledger] (address-parts ledger-name)]
      (let [ipfs-addr (if (= "ipns" proto)
                        (str "/ipns/" address)
                        address)]
        ;; address might be a directory, or could directly be a commit file - try to look up as directory first
        (let [ledgers (<? (ipfs-dir/list-all ipfs-endpoint ipfs-addr))]
          (or (get ledgers ledger)
              ledger-name)))
      ledger-name)))

(defn address-exists?
  [{:keys [ipfs-endpoint] :as _conn} ledger-address]
  (go-try
    (log/debug "Checking for existence of ledger" ledger-address)
    (boolean
      (when-let [[proto address ledger] (address-parts ledger-address)]
        (let [ipfs-addr (if (= "ipns" proto)
                          (str "/ipns/" address)
                          address)
              ledgers   (<? (ipfs-dir/list-all ipfs-endpoint ipfs-addr))]
          (contains? ledgers ledger))))))

(defrecord IPFSConnection [id memory state ledger-defaults async-cache
                           serializer parallelism msg-in-ch msg-out-ch
                           ipfs-endpoint]
  conn-proto/iLedger
  (-create [conn {:keys [ledger-alias opts]}] (jld-ledger/create
                                                conn ledger-alias opts))
  (-load [conn {:keys [ledger-alias]}]
    (go
      (let [address (<! (conn-proto/-address conn {:ledger-alias ledger-alias}))]
        (log/debug "Loading ledger from" address)
        (<! (jld-ledger/load conn address)))))
  (-load-from-address [conn {:keys [ledger-address]}]
    (jld-ledger/load conn ledger-address))

  conn-proto/iStorage
  (-c-read [_ commit-key]
    (ipfs/read ipfs-endpoint commit-key))

  (-c-write [_ commit-data]
    (ipfs/commit ipfs-endpoint commit-data))

  (-c-write [_ _ commit-data]
    (ipfs/commit ipfs-endpoint commit-data))

  conn-proto/iNameService
  (-push [_ address ledger-data]
    (ipfs/push! ipfs-endpoint address ledger-data))
  (-pull [this ledger] :TODO)
  (-subscribe [this ledger] :TODO)
  (-lookup [this {:keys [head-commit-address]}]
    (lookup-address this head-commit-address))
  (-alias [_ {:keys [ledger-address]}]
    (let [[_ _ alias] (address-parts ledger-address)] alias))
  (-address [this {:keys [ledger-alias opts]}]
    (get-address this ledger-alias opts))
  (-exists? [this {:keys [ledger-address]}]
    (address-exists? this ledger-address))

  conn-proto/iConnection
  (-close [_]
    (log/info "Closing IPFS Connection" id)
    (swap! state assoc :closed? true))
  (-closed? [_] (boolean (:closed? @state)))
  (-method [_] :ipfs)
  (-parallelism [_] parallelism)
  (-id [_] id)
  (-context [_] (:context ledger-defaults))
  (-new-indexer [_ opts]                                    ;; default new ledger indexer
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

  storage/Store
  (read [_ k]
    (ipfs/read ipfs-endpoint k true))
  (write [_ k data]
    (ipfs/commit ipfs-endpoint data))
  (exists? [conn k]
    (storage/read conn k))
  (rename [_ old-key new-key]
    (throw (ex-info (str "IPFS does not support renaming of files: " old-key new-key)
                    {:status 500 :error :db/unexpected-error})))

  index/Resolver
  (resolve
    [conn {:keys [id leaf tempid] :as node}]
    (if (= :empty id)
      (storage/resolve-empty-leaf node)
      (async-cache
          [::resolve id tempid]
          (fn [_]
            (storage/resolve-index-node conn node
                                        (fn []
                                          (async-cache [::resolve id tempid] nil)))))))

  #?@(:clj
      [full-text/IndexConnection
       (open-storage [conn network dbid lang]
         (throw (ex-info "IPFS connection does not support full text operations."
                         {:status 500 :error :db/unexpected-error})))]))


;; TODO - the following few functions are duplicated from fluree.db.connection
;; TODO - should move to a common space

(defn- lookup-cache
  [cache-atom k value-fn]
  (if (nil? value-fn)
    (swap! cache-atom cache/evict k)
    (when-let [v (get @cache-atom k)]
      (do (swap! cache-atom cache/hit k)
          v))))

(defn- default-object-cache-fn
  "Default synchronous object cache to use for ledger."
  [cache-atom]
  (fn [k value-fn]
    (if-let [v (lookup-cache cache-atom k value-fn)]
      v
      (let [v (value-fn k)]
        (swap! cache-atom cache/miss k v)
        v))))

(defn- default-async-cache-fn
  "Default asynchronous object cache to use for ledger."
  [cache-atom]
  (fn [k value-fn]
    (let [out (async/chan)]
      (if-let [v (lookup-cache cache-atom k value-fn)]
        (async/put! out v)
        (go
          (let [v (<! (value-fn k))]
            (when-not (exception? v)
              (swap! cache-atom cache/miss k v))
            (async/put! out v))))
      out)))

(defn- default-object-cache-factory
  "Generates a default object cache."
  [cache-size]
  (cache/lru-cache-factory {} :threshold cache-size))

(defn ledger-defaults
  "Normalizes ledger defaults settings"
  [ipfs-endpoint {:keys [ipns context did indexer] :as defaults}]
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
      {:ipns    {:key     ipns-default-key
                 :address ipns-default-address}
       :context context
       :did     did
       :indexer new-indexer-fn})))


(defn connect
  "Creates a new IPFS connection."
  [{:keys [server parallelism async-cache memory defaults serializer]
    :or   {server     "http://127.0.0.1:5001/"
           serializer (json-serde)}}]
  (go-try
    (let [ipfs-endpoint      (or server "http://127.0.0.1:5001/") ;; TODO - validate endpoint looks like a good URL and ends in a '/' or add it
          ledger-defaults    (<? (ledger-defaults ipfs-endpoint defaults))
          memory             (or memory 1000000)            ;; default 1MB memory
          conn-id            (str (random-uuid))
          state              (state-machine/blank-state)
          memory-object-size (quot memory 100000)           ;; avg 100kb per cache object
          _                  (when (< memory-object-size 10)
                               (throw (ex-info (str "Must allocate at least 1MB of memory for Fluree. You've allocated: " memory " bytes.") {:status 400 :error :db/invalid-configuration})))

          default-cache-atom (atom (default-object-cache-factory memory-object-size))
          async-cache-fn     (or async-cache
                                 (default-async-cache-fn default-cache-atom))]
      ;; TODO - need to set up monitor loops for async chans
      (map->IPFSConnection {:id              conn-id
                            :ipfs-endpoint   ipfs-endpoint
                            :ledger-defaults ledger-defaults
                            :serializer      serializer
                            :parallelism     parallelism
                            :msg-in-ch       (async/chan)
                            :msg-out-ch      (async/chan)
                            :memory          true
                            :state           state
                            :async-cache     async-cache-fn}))))
