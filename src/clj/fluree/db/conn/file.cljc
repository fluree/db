(ns fluree.db.conn.file
  (:refer-clojure :exclude [exists?])
  (:require [clojure.core.async :as async :refer [go]]
            [clojure.string :as str]
            [fluree.crypto :as crypto]
            [fluree.db.util.context :as ctx-util]
            [fluree.json-ld :as json-ld]
            [fluree.db.index :as index]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.conn.cache :as conn-cache]
            [fluree.db.conn.state-machine :as state-machine]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.storage :as storage]
            [fluree.db.indexer.default :as idx-default]
            [fluree.db.serde.json :refer [json-serde]]
            #?@(:cljs [["fs" :as fs]
                       ["path" :as path]])
            #?(:clj [fluree.db.full-text :as full-text])
            #?(:clj [clojure.java.io :as io])
            [fluree.db.util.json :as json]
            [fluree.db.ledger.proto :as ledger-proto])
  #?(:clj
     (:import (java.io ByteArrayOutputStream FileNotFoundException File))))

#?(:clj (set! *warn-on-reflection* true))

(defn file-address
  "Turn a path or a protocol-relative URL into a fluree file address."
  [path]
  (if (str/starts-with? path "//")
    (str "fluree:file:" path)
    (str "fluree:file://" path)))

(defn local-path
  [{:keys [storage-path] :as _conn}]
  (let [abs-path? #?(:clj (.isAbsolute (io/file storage-path))
                     :cljs (path/isAbsolute storage-path))
        abs-root  (if abs-path?
                    ""
                    (str #?(:clj  (.getAbsolutePath (io/file ""))
                            :cljs (path/resolve ".")) "/"))
        path      (str abs-root storage-path "/")]
    #?(:clj  (-> path io/file .getCanonicalPath)
       :cljs (path/resolve path))))

(defn address-path
  [address]
  (let [[_ _ path] (str/split address #":")]
    path))

(defn address-full-path
  [conn address]
  (str (local-path conn) "/" (address-path address)))

(defn address-path-exists?
  [conn address]
  (let [full-path (address-full-path conn address)]
    #?(:clj  (->> full-path io/file .exists)
       :cljs (fs/existsSync full-path))))

(defn read-file
  "Read a string from disk at `path`. Returns nil if file does not exist."
  [path]
  #?(:clj
     (try
       (with-open [xin  (io/input-stream path)
                   xout (ByteArrayOutputStream.)]
         (io/copy xin xout)
         (String. (.toByteArray xout)))

       (catch FileNotFoundException _
         nil))
     :cljs
     (try
       (fs/readFileSync path "utf8")
       (catch :default e
         (when (not= "ENOENT" (.-code e))
           (throw (ex-info "Error reading file."
                           {"errno"   ^String (.-errno e)
                            "syscall" ^String (.-syscall e)
                            "code"    (.-code e)
                            "path"    (.-path e)})))))))

(defn read-address
  [conn address]
  (->> address (address-full-path conn) read-file))

(defn read-commit
  [conn address]
  (json/parse (read-address conn address) false))

(defn write-file
  "Write bytes to disk at the given file path."
  [path ^bytes val]
  #?(:clj
     (try
       (with-open [out (io/output-stream (io/file path))]
         (.write out val))
       (catch FileNotFoundException _
         (try
           (io/make-parents (io/file path))
           (with-open [out (io/output-stream (io/file path))]
             (.write out val))
           (catch Exception e
             (log/error (str "Unable to create storage directory: " path
                             " with error: " (.getMessage e) "."))
             (log/error (str "Fatal Error, shutting down!"))
             (System/exit 1))))
       (catch Exception e (throw e)))
     :cljs
     (try
       (fs/writeFileSync path val)
       (catch :default e
         (if (= (.-code e) "ENOENT")
           (try
             (fs/mkdirSync (path/dirname path) #js{:recursive true})
             (try
               (fs/writeFileSync path val)
               (catch :default e
                 (log/error (str "Unable to write file to path " path
                                 " with error: " ^String (.-message e) "."))
                 (log/error (str "Fatal Error, shutting down! "
                                 {"errno"   ^String (.-errno e)
                                  "syscall" ^String (.-syscall e)
                                  "code"    (.-code e)
                                  "path"    (.-path e)}))
                 (js/process.exit 1)))
             (catch :default e
               (log/error (str "Unable to create storage directory: " path
                               " with error: " ^String (.-message e) "."))
               (log/error (str "Fatal Error, shutting down!"))
               (js/process.exit 1)))
           (throw (ex-info "Error writing file."
                           {"errno"   ^String (.-errno e)
                            "syscall" ^String (.-syscall e)
                            "code"    (.-code e)
                            "path"    (.-path e)})))))))

(defn ->bytes
  [s]
  #?(:clj  (.getBytes ^String s)
     :cljs (js/Uint8Array. (js/Buffer.from s "utf8"))))

(defn- write-data
  [conn ledger data-type data]
  (let [alias      (ledger-proto/-alias ledger)
        branch     (name (:name (ledger-proto/-branch ledger)))
        json       (if (string? data)
                     data
                     (json-ld/normalize-data data))
        bytes      (->bytes json)
        hash       (crypto/sha2-256 bytes :hex)
        type-dir   (name data-type)
        path       (str alias
                        (when branch (str "/" branch))
                        (str "/" type-dir "/")
                        hash ".json")
        write-path (str (local-path conn) "/" path)]
    (log/debug (str "Writing " (name data-type) " at " write-path))
    (write-file write-path bytes)
    {:name    hash
     :hash    hash
     :json    json
     :size    (count json)
     :address (file-address path)}))

(defn write-commit
  [conn ledger commit-data]
  (write-data conn ledger :commit commit-data))

(defn write-context
  [conn ledger context-data]
  (write-data conn ledger :context context-data))

(defn write-index-item
  [conn ledger index-type index-data]
  (write-data conn ledger (str "index/" (name index-type)) index-data))

(defn push
  "Just write to a different directory?"
  [conn publish-address {commit-address :address}]
  (let [local-path  (local-path conn)
        commit-path (address-path commit-address)
        head-path   (address-path publish-address)
        write-path  (str local-path "/" head-path)

        work        (fn [complete]
                      (log/debug (str "Updating head at " write-path " to " commit-path "."))
                      (write-file write-path (->bytes commit-path))
                      (complete (file-address head-path)))]
    #?(:clj  (let [p (promise)]
               (future (work (partial deliver p)))
               p)
       :cljs (js/Promise. (fn [resolve reject] (work resolve))))))

(defn read-context
  [conn context-key]
  (json/parse (read-address conn context-key) true))

(defrecord FileConnection [id memory state ledger-defaults parallelism msg-in-ch
                           serializer msg-out-ch lru-cache-atom]

  conn-proto/iStorage
  (-c-read [conn commit-key] (go (read-commit conn commit-key)))
  (-c-write [conn ledger commit-data] (go (write-commit conn ledger
                                                        commit-data)))
  (-ctx-read [conn context-key] (go (read-context conn context-key)))
  (-ctx-write [conn ledger context-data] (go (write-context conn ledger
                                                            context-data)))
  (-index-file-write [conn ledger index-type index-data]
    #?(:clj (async/thread (write-index-item conn ledger index-type index-data))
       :cljs (async/go (write-index-item conn ledger index-type index-data))))
  (-index-file-read [conn index-address]
    #?(:clj (async/thread (json/parse (read-address conn index-address) true))
       :cljs (async/go (json/parse (read-address conn index-address) true))))

  conn-proto/iNameService
  (-pull [conn ledger] (throw (ex-info "Unsupported FileConnection op: pull" {})))
  (-subscribe [conn ledger] (throw (ex-info "Unsupported FileConnection op: subscribe" {})))
  (-alias [conn ledger-address]
    ;; TODO: need to validate that the branch doesn't have a slash?
    (-> (address-path ledger-address)
        (str/split #"/")
        (->> (drop-last 2) ; branch-name, head
             (str/join #"/"))))
  (-push [conn head-path commit-data] (go (push conn head-path commit-data)))
  (-lookup [conn head-address] (go (file-address (read-address conn head-address))))
  (-address [conn ledger-alias {:keys [branch] :as _opts}]
    (let [branch (if branch (name branch) "main")]
      (go (file-address (str ledger-alias "/" branch "/head")))))
  (-exists? [conn ledger-address]
    (go (address-path-exists? conn ledger-address)))

  conn-proto/iConnection
  (-close [_]
    (log/info "Closing file connection" id)
    (swap! state assoc :closed? true))
  (-closed? [_] (boolean (:closed? @state)))
  (-method [_] :file)
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
  ;; default new ledger indexer
  (-did [_] (:did ledger-defaults))
  (-msg-in [conn msg] (throw (ex-info "Unsupported FileConnection op: msg-in" {})))
  (-msg-out [conn msg] (throw (ex-info "Unsupported FileConnection op: msg-out" {})))
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
         (throw (ex-info "File connection does not support full text operations."
                         {:status 500 :error :db/unexpected-error})))]))

(defn trim-last-slash
  [s]
  (if (str/ends-with? s "/")
    (subs s 0 (dec (count s)))
    s))

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
                   (throw (ex-info (str "Expected an indexer constructor fn or "
                                        "default indexer options map. Provided: " indexer)
                                   {:status 400 :error :db/invalid-file-connection})))})

(defn connect
  "Create a new file system connection."
  [{:keys [defaults parallelism storage-path lru-cache-atom memory serializer]
    :or   {serializer (json-serde)} :as _opts}]
  (go
    (let [storage-path   (trim-last-slash storage-path)
          conn-id        (str (random-uuid))
          state          (state-machine/blank-state)

          cache-size     (conn-cache/memory->cache-size memory)
          lru-cache-atom (or lru-cache-atom (atom (conn-cache/create-lru-cache cache-size)))]
      ;; TODO - need to set up monitor loops for async chans
      (map->FileConnection {:id              conn-id
                            :storage-path    storage-path
                            :ledger-defaults (ledger-defaults defaults)
                            :serializer      serializer
                            :parallelism     parallelism
                            :msg-in-ch       (async/chan)
                            :msg-out-ch      (async/chan)
                            :state           state
                            :lru-cache-atom  lru-cache-atom}))))
