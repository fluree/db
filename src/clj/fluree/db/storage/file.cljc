(ns fluree.db.storage.file
  (:require [fluree.crypto :as crypto]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.bytes :as bytes]
            [fluree.db.util.filesystem :as fs]
            [fluree.db.storage :as storage]
            [clojure.string :as str]))

(def method-name "file")

(defn full-path
  [root relative-path]
  (str (fs/local-path root) "/" relative-path))

(defn storage-path
  [root address]
  (let [relative-path (:local (storage/parse-address address))]
    (full-path root relative-path)))

(defn file-address
  [path]
  (storage/build-fluree-address method-name path))

(defrecord FileStore [root]
  storage/Store
  (write [_ dir data]
    (go-try
      (when (not (storage/hashable? data))
        (throw (ex-info "Must serialize v before writing to FileStore."
                        {:root root
                         :path dir
                         :data data})))
      (let [hash     (crypto/sha2-256 data :hex)
            path     (str/join "/" [dir hash ".json"])
            absolute (full-path root path)
            bytes    (if (string? data)
                       (bytes/string->UTF8 data)
                       data)]
        (<? (fs/write-file absolute bytes))
        {:path    path
         :address (file-address path)
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
