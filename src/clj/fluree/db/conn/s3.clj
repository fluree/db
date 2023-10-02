(ns fluree.db.conn.s3
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [cognitect.aws.client.api :as aws]
            [clojure.core.async :as async :refer [go go-loop <! >!]]
            [fluree.crypto :as crypto]
            [fluree.db.conn.cache :as conn-cache]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.conn.state-machine :as state-machine]
            [fluree.db.full-text :as full-text]
            [fluree.db.index :as index]
            [fluree.db.indexer.default :as idx-default]
            [fluree.db.ledger.proto :as ledger-proto]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.db.storage :as storage]
            [fluree.db.util.context :as ctx-util]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld])
  (:import (java.io Closeable ByteArrayOutputStream)))

(set! *warn-on-reflection* true)

(defn s3-address
  [{:keys [s3-bucket s3-prefix]} path]
  (if (str/starts-with? path "//")
    (str "fluree:s3://" s3-bucket "/" s3-prefix "/" (-> path (str/split #"//")
                                                        last))
    (str "fluree:s3://" s3-bucket "/" s3-prefix "/" path)))

(defn address-path
  ([conn address] (address-path conn address true))
  ([{:keys [s3-bucket s3-prefix]} address strip-prefix?]
   (log/debug "address-path address:" address)
   (let [path (-> address (str/split #"://") last)]
     (if strip-prefix?
       (-> path (str/replace-first (str s3-bucket "/" s3-prefix "/") ""))
       (str "//" path)))))

(defn handle-s3-response
  [resp]
  (if (:cognitect.anomalies/category resp)
    (if (:cognitect.aws.client/throwable resp)
      resp
      (ex-info "S3 read failed"
               {:status 500, :error :db/unexpected-error, :aws/response resp}))
    (let [{in :Body} resp
          _        (log/debug "S3 response:" resp)
          body-str (when in
                     (with-open [out (ByteArrayOutputStream.)]
                       (io/copy in out)
                       (.close ^Closeable in)
                       (String. (.toByteArray out))))]
      (cond-> resp
              body-str (assoc :Body body-str)))))

(defn s3-list*
  ([conn path] (s3-list* conn path nil))
  ([{:keys [s3-client s3-bucket s3-prefix]} path continuation-token]
   (let [ch        (async/promise-chan (map handle-s3-response))
         base-req  {:op      :ListObjectsV2
                    :ch      ch
                    :request {:Bucket s3-bucket}}
         full-path (if (empty? s3-prefix)
                     path
                     (str s3-prefix "/" path))
         req       (cond-> base-req
                           (not= full-path "/") (assoc-in [:request :Prefix]
                                                          full-path)
                           continuation-token (assoc-in
                                               [:request :ContinuationToken]
                                               continuation-token))]
     (log/debug "s3-list* req:" req)
     (aws/invoke-async s3-client req)
     ch)))

(defn s3-list
  "Returns a core.async channel that will contain one or more result batches of
  1000 or fewer object names. You should continue to take from the channel until
  it closes (i.e. returns nil)."
  [conn path]
  (let [ch (async/chan 1)]
    (go-loop [results (<! (s3-list* conn path))]
      (>! ch results)
      (let [truncated?         (:IsTruncated results)
            continuation-token (:NextContinuationToken results)]
        (if truncated?
          (recur (<! (s3-list* conn path continuation-token)))
          (async/close! ch))))
    ch))

(defn s3-key-exists?
  [conn key]
  (go
    (let [list (<! (s3-list conn key))]
      (< 0 (:KeyCount list)))))

(defn read-s3-data
  [{:keys [s3-client s3-bucket s3-prefix]} path]
  (let [ch        (async/promise-chan (map handle-s3-response))
        full-path (str s3-prefix "/" path)
        req       {:op      :GetObject
                   :ch      ch
                   :request {:Bucket s3-bucket, :Key full-path}}]
    (aws/invoke-async s3-client req)
    ch))

(defn write-s3-data
  [{:keys [s3-client s3-bucket s3-prefix]} path ^bytes data]
  (let [ch        (async/promise-chan (map handle-s3-response))
        full-path (str s3-prefix "/" path)
        req       {:op      :PutObject
                   :ch      ch
                   :request {:Bucket s3-bucket, :Key full-path, :Body data}}]
    (aws/invoke-async s3-client req)
    ch))

(defn write-data
  [conn ledger data-type data]
  (go
    (let [alias    (ledger-proto/-alias ledger)
          branch   (-> ledger ledger-proto/-branch :name name)
          json     (if (string? data)
                     data
                     (json-ld/normalize-data data))
          bytes    (.getBytes ^String json)
          hash     (crypto/sha2-256 bytes :hex)
          type-dir (name data-type)
          path     (str alias
                        (when branch (str "/" branch))
                        (str "/" type-dir "/")
                        hash ".json")
          result   (<! (write-s3-data conn path bytes))]
      (if (instance? Throwable result)
        result
        {:name    hash
         :hash    hash
         :json    json
         :size    (count json)
         :address (s3-address conn path)}))))

(defn read-address
  [conn address]
  (->> address (address-path conn) (read-s3-data conn)))

(defn read-commit
  [conn address]
  (go (json/parse (<! (read-address conn address)) false)))

(defn write-commit
  [conn ledger commit-data]
  (write-data conn ledger :commit commit-data))

(defn read-context
  [conn address]
  (go (json/parse (<! (read-address conn address)) false)))

(defn write-context
  [conn ledger context-data]
  (write-data conn ledger :context context-data))

(defn write-index
  [conn ledger index-type index-data]
  (write-data conn ledger (str "index/" (name index-type)) index-data))

(defn read-index
  [conn index-address]
  (go (-> conn (read-address index-address) <! (json/parse true))))

(defn push
  [conn publish-address {commit-address :address}]
  (go
    (let [commit-path (address-path conn commit-address false)
          head-path   (address-path conn publish-address)]
      (->> (.getBytes ^String commit-path)
           (write-s3-data conn head-path)
           :address))))

(defrecord S3Connection [id s3-client s3-bucket s3-prefix memory state
                         ledger-defaults parallelism msg-in-ch msg-out-ch
                         lru-cache-atom]
  conn-proto/iStorage
  (-c-read [conn commit-key] (read-commit conn commit-key))
  (-c-write [conn ledger commit-data] (write-commit conn ledger commit-data))
  (-ctx-read [conn context-key] (read-context conn context-key))
  (-ctx-write [conn ledger context-data] (write-context conn ledger context-data))
  (-index-file-write [conn ledger index-type index-data]
    (write-index conn ledger index-type index-data))
  (-index-file-read [conn index-address]
    (read-index conn index-address))

  conn-proto/iNameService
  (-pull [_conn _ledger] (throw (ex-info "Unsupported S3Connection op: pull" {})))
  (-subscribe [_conn _ledger]
    (throw (ex-info "Unsupported S3Connection op: subscribe" {})))
  (-alias [conn ledger-address]
    (-> ledger-address (->> (address-path conn)) (str/split #"/")
        (->> (drop-last 2) (str/join #"/"))))
  (-push [conn head-path commit-data] (push conn head-path commit-data))
  (-lookup [conn head-address]
    (go (s3-address conn (<! (read-address conn head-address)))))
  (-address [conn ledger-alias {:keys [branch] :as _opts}]
    (let [branch (if branch (name branch) "main")]
      (go (s3-address conn (str ledger-alias "/" branch "/head")))))
  (-exists? [conn ledger-address] (s3-key-exists? conn ledger-address))

  conn-proto/iConnection
  (-close [_] (swap! state assoc :closed? true))
  (-closed? [_] (boolean (:closed? @state)))
  (-method [_] :s3)
  (-parallelism [_] parallelism)
  (-id [_] id)
  (-default-context [_] (:context ledger-defaults))
  (-default-context [_ context-type] (let [ctx (:context ledger-defaults)]
                                       (if (= :keyword context-type)
                                         (ctx-util/keywordize-context ctx)
                                         ctx)))
  (-context-type [_] (:context-type ledger-defaults))
  (-new-indexer [_ opts]
    (let [indexer-fn (:indexer ledger-defaults)]
      (indexer-fn opts)))
  (-did [_] (:did ledger-defaults))
  (-msg-in [_ _] (throw (ex-info "Unsupported S3Connection op: msg-in" {})))
  (-msg-out [_ _] (throw (ex-info "Unsupported S3Connection op: msg-out" {})))
  (-state [_] @state)
  (-state [_ ledger] (get @state ledger))

  index/Resolver
  (resolve [conn {:keys [id leaf tempid] :as node}]
    (let [cache-key [::resolve id tempid]]
      (if (= :empty id)
        (storage/resolve-empty-node node)
        (conn-cache/lru-lookup lru-cache-atom cache-key
                               (fn [_]
                                 (storage/resolve-index-node
                                  conn node
                                  (fn [] (conn-cache/lru-evict lru-cache-atom
                                                               cache-key))))))))

  full-text/IndexConnection
  (open-storage [_conn _network _dbid _lang]
    (throw (ex-info "S3 connection does not support full text operations."
                    {:status 400, :error :db/unsupported-operation}))))

(defn ledger-defaults
  [{:keys [context context-type did indexer]}]
  {:context      (ctx-util/stringify-context context)
   :context-type context-type
   :did          did
   :indexer      (cond
                   (fn? indexer)
                   indexer

                   (or (map? indexer) (nil? indexer))
                   (fn [opts]
                     (idx-default/create (merge indexer opts)))

                   :else
                   (throw (ex-info (str "Expected an indexer constructor fn or default indexer options map. Provided: "
                                        indexer)
                                   {:status 400, :error :db/invalid-s3-connection})))})

(defn connect
  "Create a new S3 connection."
  [{:keys [defaults parallelism s3-endpoint s3-bucket s3-prefix lru-cache-atom
           memory serializer]
    :or   {serializer (json-serde)} :as _opts}]
  (go
    (let [aws-opts       (cond-> {:api :s3}
                                 s3-endpoint (assoc :endpoint-override s3-endpoint))
          client         (aws/client aws-opts)
          conn-id        (str (random-uuid))
          state          (state-machine/blank-state)
          cache-size     (conn-cache/memory->cache-size memory)
          lru-cache-atom (or lru-cache-atom
                             (atom (conn-cache/create-lru-cache cache-size)))]
      (map->S3Connection {:id              conn-id
                          :s3-client       client
                          :s3-bucket       s3-bucket
                          :s3-prefix       s3-prefix
                          :memory          memory
                          :state           state
                          :ledger-defaults (ledger-defaults defaults)
                          :serializer      serializer
                          :parallelism     parallelism
                          :msg-in-ch       (async/chan)
                          :msg-out-ch      (async/chan)
                          :lru-cache-atom  lru-cache-atom}))))
