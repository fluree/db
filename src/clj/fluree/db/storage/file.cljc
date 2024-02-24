(ns fluree.db.storage.file
  (:require [clojure.string :as str]
            [fluree.crypto :as crypto]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.bytes :as bytes]
            [fluree.db.util.filesystem :as fs]
            [fluree.db.storage.proto :as store-proto]
            [fluree.db.storage.util :as store-util]))

(defn file-address
  [path]
  (if (str/starts-with? path "//")
    (str "fluree:file:" path)
    (str "fluree:file://" path)))

(defn file-write
  [storage-path path v {:keys [content-address?] :as opts}]
  (when (not (store-util/hashable? v))
    (throw (ex-info "Must serialize v before writing to FileStore."
                    {:storage-path storage-path
                     :path            path
                     :v            v
                     :opts         opts})))
  (go-try
    (let [hash  (crypto/sha2-256 v)
          path*    (if content-address?
                     (str path hash)
                     path)
          path  (str (fs/local-path storage-path) "/" path*)
          bytes (if (string? v)
                  (bytes/string->UTF8 v)
                  v)]
      (<? (fs/write-file path bytes))
      {:path    path*
       :address (file-address path*)
       :hash hash
       :size (count bytes)})))

(defn file-list
  [storage-path prefix]
  (let [path (str (fs/local-path storage-path) "/" prefix)]
    (fs/list-files path)))

(defn file-read
  [storage-path address]
  (let [path    (:local (store-util/address-parts address))
        path (str (fs/local-path storage-path) "/" path)]
    (fs/read-file path)))

(defn file-delete
  [storage-path address]
  (let [path    (:local (store-util/address-parts address))
        path (str (fs/local-path storage-path) "/" path)]
    (fs/delete-file path)))

(defn file-exists?
  [storage-path address]
  (let [path    (:local (store-util/address-parts address))
        path (str (fs/local-path storage-path) "/" path)]
    (fs/exists? path)))

(defrecord FileStore [storage-path]
  store-proto/Store
  (address [_ path] (file-address path))
  (write [_ path v opts] (file-write storage-path path v opts))
  (read [_ address] (file-read storage-path address))
  (list [_ prefix] (file-list storage-path prefix))
  (delete [_ address] (file-delete storage-path address))
  (exists? [_ address] (file-exists? storage-path address)))

(defn open
  [storage-path]
  (->FileStore storage-path))
