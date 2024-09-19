(ns fluree.db.storage.file
  (:require [fluree.crypto :as crypto]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.bytes :as bytes]
            [fluree.db.util.json :as json]
            [fluree.db.util.filesystem :as fs]
            [fluree.db.storage :as storage]
            [clojure.string :as str]))

(def method-name "file")

(defn full-path
  [root relative-path]
  (str (fs/local-path root) "/" relative-path))

(defn storage-path
  [root address]
  (let [relative-path (storage/parse-local-path address)]
    (full-path root relative-path)))

(defn file-address
  [identifier path]
  (storage/build-fluree-address identifier method-name path))

(defrecord FileStore [identifier root]
  storage/Addressable
  (-location [_]
    (storage/build-location storage/fluree-namespace identifier method-name))

  storage/JsonArchive
  (-read-json [_ address keywordize?]
    (go-try
      (let [path (storage-path root address)]
        (when-let [data (<? (fs/read-file path))]
          (json/parse data keywordize?)))))

  storage/EraseableStore
  (delete [_ address]
    (let [path (storage-path root address)]
      (fs/delete-file path)))

  storage/ContentAddressedStore
  (-content-write-bytes [_ dir data]
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
         :address (file-address identifier path)
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
  ([root-path]
   (open nil root-path))
  ([identifier root-path]
   (->FileStore identifier root-path)))
