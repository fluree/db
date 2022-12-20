(ns fluree.store.file
  (:refer-clojure :exclude [exists? list])
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [fluree.common.identity :as ident]
   [fluree.common.protocols :as service-proto]
   [fluree.common.util :as util]
   [fluree.db.index]
   [fluree.db.storage.core :as storage]
   [fluree.db.util.async :refer [<? go-try]]
   [fluree.db.util.json :as json]
   [fluree.db.util.log :as log]
   #?@(:cljs [["fs" :as fs]
              ["path" :as path]])
   [fluree.store.protocols :as store-proto])
  (:import
   (java.io ByteArrayOutputStream FileNotFoundException)))

(defn write-file
  "Write string to disk at the given file path."
  [path data serialize-to]
  #?(:clj
     (try
       (with-open [out (io/output-stream (io/file path))]
         (let [serialized (case serialize-to
                            :json (json/stringify data)
                            :edn (pr-str data))]
           (.write out (util/string->bytes serialized))))
       :written
       (catch FileNotFoundException _
         (try
           (io/make-parents (io/file path))
           (with-open [out (io/output-stream (io/file path))]
             (.write out val))
           (catch Exception e
             (log/error (str "Unable to create storage directory: " path
                             " with error: " (.getMessage e) "."))
             (throw e))))
       (catch Exception e (throw e)))
     :cljs
     (try
       (fs/writeFileSync path val)
       (catch :default e
         (if (= (.-code e) "ENOENT")
           (try
             (fs/mkdirSync (path/dirname path) #js{:recursive true})
             (try
               (let [serialized (case serialize-to
                                  :json (js/JSON.stringify data)
                                  :edn (pr-str data))])
               (fs/writeFileSync path serialized)
               :store/written
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
                               " with error: " ^String (.-message e) "."))))
           (throw (ex-info "Error writing file."
                           {"errno"   ^String (.-errno e)
                            "syscall" ^String (.-syscall e)
                            "code"    (.-code e)
                            "path"    (.-path e)})))))))

(defn read-file
  "Read string from disk at given file path."
  [path serialize-to]
  #?(:clj
     (try
       (with-open [xin (io/input-stream path)
                   xout (ByteArrayOutputStream.)]
         (io/copy xin xout)
         (let [serialized (String. (.toByteArray xout))]
           (case serialize-to
             :json (json/parse serialized false)
             :edn (edn/read-string serialized))))
       (catch FileNotFoundException _
         nil))
     :cljs
     (try*
       (fs/readFileSync path "utf8")
       (catch* e
               (when (not= "ENOENT" (.-code e))
                 (throw (ex-info "Error reading file."
                                 {"errno"   ^String (.-errno e)
                                  "syscall" ^String (.-syscall e)
                                  "code"    (.-code e)
                                  "path"    (.-path e)})))))))

(defn delete-file
  "Delete file from disk at given file path."
  [path]
  (try
    (io/delete-file (io/file path))
    :deleted
    (catch Exception e
      (log/error (str "Failed to delete file: " path " with error: " (.getMessage e) ".")))))

(defn address-file
  [type k]
  (ident/create-address type :file k))

(defn stop-file-store
  "Notify of Store stopping. No state to dispose of."
  [store]
  (log/info (str "Stopping FileStore " (service-proto/id store) " " (:storage-path store ) "."))
  :stopped)

(defrecord FileStore [id serialize-to storage-path async-cache]
  service-proto/Service
  (id [_] id)
  (stop [store] (stop-file-store store))

  store-proto/Store
  (address [_ type k] (address-file type k))
  (read [_ k] (go-try (read-file (str storage-path k) serialize-to)))
  (write [_ k data] (go-try (write-file (str storage-path k) data serialize-to)))
  (delete [_ k] (go-try (delete-file (str storage-path k))))

  ;; TODO: make a proper resolver
  fluree.db.index/Resolver
  (resolve
    [_ node]
    (storage/resolve-empty-leaf node)))

(defn create-file-store
  [{:keys [:store/id :file-store/storage-path :file-store/serialize-to] :as config}]
  (let [id (or id (random-uuid))]
    (log/info "Starting FileStore " id "." config)
    (map->FileStore {:id id
                     :serialize-to serialize-to
                     :storage-path (util/ensure-trailing-slash storage-path)})))
