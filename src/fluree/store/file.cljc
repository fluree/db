(ns fluree.store.file
  (:refer-clojure :exclude [read list])
  (:require [clojure.string :as str]
            [fluree.crypto :as crypto]
            [fluree.db.util.bytes :as bytes]
            [fluree.db.util.filesystem :as fs]
            [fluree.store.proto :as store-proto]
            [fluree.store.util :as store-util]))

(defn file-address
  [path]
  (if (str/starts-with? path "//")
    (str "fluree:file:" path)
    (str "fluree:file://" path)))

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
     :address (file-address k*)
     :hash hash
     :size (count bytes)}))

(defn file-list
  [storage-path prefix]
  (let [path (str (fs/local-path storage-path) "/" prefix)]
    (fs/list-files path)))

(defn file-read
  [storage-path address]
  (let [k    (:local (store-util/address-parts address))
        path (str (fs/local-path storage-path) "/" k)]
    (fs/read-file path)))

(defn file-delete
  [storage-path address]
  (let [k    (:local (store-util/address-parts address))
        path (str (fs/local-path storage-path) "/" k)]
    (fs/delete-file path)))

(defn file-exists?
  [storage-path address]
  (let [k    (:local (store-util/address-parts address))
        path (str (fs/local-path storage-path) "/" k)]
    (fs/exists? path)))

(defrecord FileStore [storage-path]
  store-proto/Store
  (write [_ k v opts] (file-write storage-path k v opts))
  (read [_ address] (file-read storage-path address))
  (list [_ prefix] (file-list storage-path prefix))
  (delete [_ address] (file-delete storage-path address))
  (exists? [_ address] (file-exists? storage-path address)))

(defn create-file-store
  [{:keys [:file-store/storage-path] :as config}]
  (map->FileStore {:config config
                   :storage-path storage-path}))