(ns fluree.store.file
  (:refer-clojure :exclude [read])
  (:require [fluree.crypto :as crypto]
            [fluree.db.util.bytes :as bytes]
            [fluree.db.util.filesystem :as fs]
            [fluree.store.proto :as store-proto]
            [fluree.store.util :as store-util]))

(defn file-write
  [storage-path k v {:keys [content-address?] :as opts}]
  (when (not (store-util/hashable? v))
    (throw (ex-info "Must serialize v before writing to FileStore."
                    {:storage-path storage-path
                     :k            k
                     :v            v
                     :opts         opts})))
  (let [hash  (crypto/sha2-256 v)
        k*    (if content-address?
                (str k hash)
                k)
        path  (str (fs/local-path storage-path) "/" k)
        bytes (if (string? v)
                (bytes/string->UTF8 v)
                v)]
    (fs/write-file path bytes)
    {:k    k*
     :hash hash
     :size (count v)}))

(defn file-read
  [storage-path k]
  (let [path (str (fs/local-path storage-path) "/" k)]
    (fs/read-file path)))

(defn file-delete
  [storage-path k]
  (let [path (str (fs/local-path storage-path) "/" k)]
    (fs/delete-file path)))

(defn file-exists?
  [storage-path k]
  (let [path (str (fs/local-path storage-path) "/" k)]
    (fs/exists? path)))

(defrecord FileStore [storage-path]
  store-proto/Store
  (write [_ k v opts] (file-write storage-path k v opts))
  (read [_ k] (file-read storage-path k))
  (delete [_ k] (file-delete storage-path k))
  (exists? [_ k] (file-exists? storage-path k)))

(defn create-file-store
  [{:keys [:file-store/storage-path] :as config}]
  (map->FileStore {:config config
                   :storage-path storage-path}))
