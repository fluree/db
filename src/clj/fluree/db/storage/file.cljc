(ns fluree.db.storage.file
  (:require [fluree.crypto :as crypto]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.bytes :as bytes]
            [fluree.db.util.filesystem :as fs]
            [fluree.db.storage :as storage]))

(def method-name "file")

(defn full-path
  [root relative-path]
  (str (fs/local-path root) "/" relative-path))

(defn storage-path
  [root address]
  (let [relative-path (:local (storage/parse-address address))]
    (full-path root relative-path)))

(defrecord FileStore [root]
  storage/Store
  (address [_ path]
    (storage/build-fluree-address method-name path))

  (write [store path v {:keys [content-address?] :as opts}]
    (go-try
      (when (not (storage/hashable? v))
        (throw (ex-info "Must serialize v before writing to FileStore."
                        {:root root
                         :path path
                         :v    v
                         :opts opts})))
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
         :address (storage/address store path*)
         :hash    hash
         :size    (count bytes)})))

  (read [_ address]
    (let [path (storage-path root address)]
      (fs/read-file path)))

  (list [_ prefix]
    (fs/list-files (full-path root prefix)))

  (delete [_ address]
    (let [path (storage-path root address)]
      (fs/delete-file path)))

  (exists? [_ address]
    (let [path (storage-path root address)]
      (fs/exists? path))))

(defn open
  [root-path]
  (->FileStore root-path))
