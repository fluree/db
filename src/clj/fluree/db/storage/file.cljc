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
  (read [_ address]
    (let [path (storage-path root address)]
      (fs/read-file path)))

  (list [_ prefix]
    (fs/list-files (full-path root prefix)))

  (exists? [_ address]
    (let [path (storage-path root address)]
      (fs/exists? path)))

  storage/EraseableStore
  (delete [_ address]
    (let [path (storage-path root address)]
      (fs/delete-file path)))

  storage/ContentAddressedStore
  (-content-write [_ dir data]
    (go-try
      (when (not (storage/hashable? data))
        (throw (ex-info "Must serialize data before writing to FileStore."
                        {:root root
                         :path dir
                         :data data})))
      (let [hash     (crypto/sha2-256 data :hex)
            filename (str hash ".json")
            path     (str/join "/" [dir filename])
            absolute (full-path root path)
            bytes    (if (string? data)
                       (bytes/string->UTF8 data)
                       data)]
        (<? (fs/write-file absolute bytes))
        {:path    path
         :address (file-address path)
         :hash    hash
         :size    (count bytes)})))

  storage/ByteStore
  (write-bytes [_ path bytes]
    (-> root
        (full-path path)
        (fs/write-file bytes)))

  (read-bytes [_ path]
    (-> root
        (full-path path)
        fs/read-file)))

(defn open
  [root-path]
  (->FileStore root-path))
