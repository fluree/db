(ns fluree.db.storage.file
  (:require [clojure.string :as str]
            [fluree.crypto :as crypto]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.bytes :as bytes]
            [fluree.db.util.filesystem :as fs]
            [fluree.db.storage.proto :as store-proto]
            [fluree.db.storage.util :as store-util]))

(defn full-path
  [root relative-path]
  (str (fs/local-path root) "/" relative-path))

(defn file-address
  [path]
  (if (str/starts-with? path "//")
    (str "fluree:file:" path)
    (str "fluree:file://" path)))

(defn file-write
  [root path v {:keys [content-address?] :as opts}]
  (when (not (store-util/hashable? v))
    (throw (ex-info "Must serialize v before writing to FileStore."
                    {:root root
                     :path path
                     :v    v
                     :opts opts})))
  (go-try
    (let [hash  (crypto/sha2-256 v)
          path* (if content-address?
                  (str path hash)
                  path)
          path  (str (fs/local-path root) "/" path*)
          bytes (if (string? v)
                  (bytes/string->UTF8 v)
                  v)]
      (<? (fs/write-file path bytes))
      {:path    path*
       :address (file-address path*)
       :hash    hash
       :size    (count bytes)})))

(defn file-list
  [root prefix]
  (fs/list-files (full-path root prefix)))

(defn file-read
  [root address]
  (let [relative-path (:local (store-util/address-parts address))
        path          (full-path root relative-path)]
    (fs/read-file path)))

(defn file-delete
  [root address]
  (let [relative-path (:local (store-util/address-parts address))
        path          (full-path root relative-path)]
    (fs/delete-file path)))

(defn file-exists?
  [root address]
  (let [relative-path (:local (store-util/address-parts address))
        path          (full-path root relative-path)]
    (fs/exists? path)))

(defrecord FileStore [root]
  store-proto/Store
  (address [_ path] (file-address path))
  (write [_ path v opts] (file-write root path v opts))
  (read [_ address] (file-read root address))
  (list [_ prefix] (file-list root prefix))
  (delete [_ address] (file-delete root address))
  (exists? [_ address] (file-exists? root address)))

(defn open
  [root-path]
  (->FileStore root-path))
