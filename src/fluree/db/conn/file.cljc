(ns fluree.db.conn.file
  (:refer-clojure :exclude [exists?])
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [fluree.crypto :as crypto]
            [fluree.json-ld :as json-ld]
            [fluree.db.index :as index]
            [fluree.db.platform :as platform]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.conn.cache :as conn-cache]
            [fluree.db.conn.state-machine :as state-machine]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.async :refer [<? go-try channel?]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.storage.core :as storage]
            [fluree.db.indexer.default :as idx-default]
            #?@(:cljs [["fs" :as fs]
                       ["path" :as path]])
            #?(:clj [fluree.db.full-text :as full-text])
            #?(:clj [clojure.java.io :as io])
            [fluree.db.util.json :as json]
            [fluree.db.ledger.proto :as ledger-proto])
  #?(:clj
     (:import (java.io ByteArrayOutputStream FileNotFoundException File))))

(defn file-address
  "Turn a path into a fluree file address."
  [path]
  (str "fluree:file:" path))

(defn address-path
  [address]
  (let [[_ _ path]  (str/split address #":")]
    path))

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
         nil)
       (catch Exception e (throw e)))
     :cljs
     (try*
       (fs/readFileSync path "utf8")
       (catch* e
               (when (not= "ENOENT" (.-code e))
                 (throw (ex-info  "Error reading file." {"errno" ^String (.-errno e)
                                                         "syscall" ^String (.-syscall e)
                                                         "code" (.-code e)
                                                         "path" (.-path e)})))))))

(defn read-address
  [address]
  (read-file (address-path address)))

(defn read-commit
  [address]
  (json/parse (read-address address) false))

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
     (try*
       (fs/writeFileSync path val)
       (catch* e
               (if (= (.-code e) "ENOENT")
                 (try*
                   (fs/mkdirSync (path/dirname path) #js{:recursive true})
                   (try*
                     (fs/writeFileSync path val)
                     (catch* e
                             (log/error (str "Unable to write file to path " path
                                             " with error: " ^String (.-message e) "."))
                             (log/error (str "Fatal Error, shutting down! " {"errno" ^String (.-errno e)
                                                                             "syscall" ^String (.-syscall e)
                                                                             "code" (.-code e)
                                                                             "path" (.-path e)}))
                             (js/process.exit 1)))
                   (catch* e
                           (log/error (str "Unable to create storage directory: " path
                                           " with error: " ^String (.-message e) "."))
                           (log/error (str "Fatal Error, shutting down!"))
                           (js/process.exit 1)))
                 (throw (ex-info "Error writing file." {"errno" ^String (.-errno e)
                                                        "syscall" ^String (.-syscall e)
                                                        "code" (.-code e)
                                                        "path" (.-path e)})))))))

(defn ->bytes
  [s]
  #?(:clj (.getBytes ^String s)
     :cljs (js/Uint8Array. (js/Buffer.from s "utf8"))))

(defn commit
  ([conn data] (commit conn nil data))
  ([conn db data]
   (let [base-path   (:storage-path conn)
         ledger      (:ledger db)
         alias       (ledger-proto/-alias ledger)
         branch      (name (:name (ledger-proto/-branch ledger)))

         json        (json-ld/normalize-data data)
         bytes       (->bytes json)
         hash        (crypto/sha2-256 bytes :hex)

         commit-path #?(:clj (str (-> (io/file "") .getAbsolutePath) "/" base-path
                                  (when alias (str "/" alias))
                                  (when branch (str "/" branch))
                                  "/commits/" hash ".json")
                        :cljs (path/resolve "." base-path (or alias "") (or branch "") "commits" hash ".json"))]
       (log/debug (str "Writing commit " hash " at " commit-path))
       (write-file commit-path bytes)
       {:name    hash
        :hash    hash
        :size    (count json)
        :address (file-address commit-path)})))

(defn push
  "Just write to a different directory?"
  [publish-address ledger-data]
  #?(:clj
     (let [p (promise)]
       (future
         (let [{:keys [address]} ledger-data
               path-to-commit    (address-path address)
               path              (address-path publish-address)]
           (log/debug (str "Updating HEAD at " path " to " path-to-commit "."))
           (write-file path (.getBytes ^String path-to-commit))
           (deliver p (file-address path))))
       p)
     :cljs
     (let [{:keys [address]} ledger-data
           path-to-commit    (address-path address)
           path              (address-path publish-address)]
       (js/Promise. (fn [resolve reject]
                      (log/debug (str "Updating HEAD at " path " to " path-to-commit "."))
                      (write-file path (js/Buffer.from path-to-commit))
                      (resolve (file-address path)))))))

(defrecord FileConnection [id transactor? memory state
                           ledger-defaults
                           push commit
                           parallelism close-fn
                           msg-in-ch msg-out-ch
                           async-cache]

  conn-proto/iStorage
  (-c-read [_ commit-key] (async/go (read-commit commit-key)))
  (-c-write [conn commit-data] (async/go (commit conn commit-data)))
  (-c-write [conn db commit-data] (async/go (commit conn db commit-data)))

  conn-proto/iNameService
  (-pull [this ledger] (throw (ex-info "Unsupported FileConnection op: pull" {})))
  (-subscribe [this ledger] (throw (ex-info "Unsupported FileConnection op: subscribe" {})))
  (-alias [conn ledger-address]
    ;; TODO: need to validate that the branch doesn't have a slash?
    (let [{:keys [storage-path]} conn
          root-path #?(:cljs (path/resolve "." storage-path)
                       :clj (str (-> (io/file "") .getAbsolutePath) "/" storage-path))]
      (-> (address-path ledger-address)
          (str/replace-first (re-pattern root-path) "")
          (str/split  #"/")
          (->> (drop-last 2)            ; branch-name, HEAD
               (str/join #"/")))))
  (-push [this head-path commit-data] (async/go (push head-path commit-data)))
  (-lookup [this head-commit-address] (async/go (file-address (read-address head-commit-address))))
  (-address [conn ledger-alias {:keys [branch] :as _opts}]
    (async/go (file-address
                #?(:cljs (path/resolve "." (:storage-path conn) ledger-alias (name branch) "HEAD")
                   :clj (str (-> (io/file "") .getAbsolutePath)
                             "/" (:storage-path conn) "/" ledger-alias
                             "/" (name branch) "/HEAD")))))

  conn-proto/iConnection
  (-close [_] #_(when (fn? close-fn) (close-fn) (swap! state assoc :closed? true)))
  (-closed? [_] (boolean (:closed? @state)))
  (-method [_] :file)
  (-parallelism [_] parallelism)
  (-transactor? [_] transactor?)
  (-id [_] id)
  (-read-only? [_] (not (fn? commit)))
  (-context [_] (:context ledger-defaults))
  (-new-indexer [_ opts]
    (let [indexer-fn (:indexer ledger-defaults)]
      (indexer-fn opts)))
  ;; default new ledger indexer
  (-did [_] (:did ledger-defaults))
  (-msg-in [conn msg] (throw (ex-info "Unsupported FileConnection msg-in: pull" {})))
  (-msg-out [conn msg] (throw (ex-info "Unsupported FileConnection msg-out: pull" {})))
  (-state [_] @state)
  (-state [_ ledger] (get @state ledger))

  storage/Store
  (read [s k] (throw (ex-info "Unsupported FileConnection Store: read" {})))
  (write [s k data] (throw (ex-info "Unsupported FileConnection Store: write" {})))
  (exists? [s k] (throw (ex-info "Unsupported FileConnection Store: exists?" {})))
  (rename [s old-key new-key] (throw (ex-info "Unsupported FileConnection Store: rename" {})))

  index/Resolver
  (resolve
    [conn node]
    ;; all root index nodes will be empty
    (storage/resolve-empty-leaf node))

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
  [{:keys [context did indexer]}]
  {:context context
   :did did
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

(defn connect
  "Create a new file system connection."
  [{:keys [defaults parallelism storage-path async-cache memory] :as opts}]
  (async/go
    (let [storage-path   (trim-last-slash storage-path)
          conn-id        (str (random-uuid))
          state          (state-machine/blank-state)
          close-fn       (fn [] (log/info (str "File Connection " conn-id " Closed")))
          async-cache-fn (or async-cache
                             (conn-cache/default-async-cache-fn memory))]
      ;; TODO - need to set up monitor loops for async chans
      (map->FileConnection {:id           conn-id
                            :storage-path storage-path
                            :ledger-defaults (ledger-defaults defaults)
                            :transactor?  false
                            :commit       commit
                            :push         push
                            :parallelism  parallelism
                            :msg-in-ch    (async/chan)
                            :msg-out-ch   (async/chan)
                            :close        close-fn
                            :state        state
                            :async-cache  async-cache-fn}))))

(comment
  (read-file (read-file "/home/dan/projects/db2/dev/data/clj/test/db1/main/HEAD" ))

  ,)
