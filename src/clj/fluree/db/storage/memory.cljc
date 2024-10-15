(ns fluree.db.storage.memory
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.crypto :as crypto]
            [fluree.db.storage :as storage]
            [fluree.db.util.json :as json]))

(def method-name "memory")

(defn memory-address
  [path]
  (storage/build-fluree-address method-name path))

(defrecord MemoryStore [contents]
  storage/JsonArchive
  (-read-json [_ address keywordize?]
    (go
      (let [path (:local (storage/parse-address address))]
        (when-let [data (get @contents path)]
          (json/parse data keywordize?)))))

  storage/EraseableStore
  (delete [_ address]
    (go
      (let [path (:local (storage/parse-address address))]
        (swap! contents dissoc path))))

  storage/ContentAddressedStore
  (-content-write [_ _ v]
    (go
      (let [hashable (if (storage/hashable? v)
                       v
                       (pr-str v))
            hash     (crypto/sha2-256 hashable)]
        (swap! contents assoc hash v)
        {:path    hash
         :address (memory-address hash)
         :hash    hash
         :size    (count hashable)})))

  storage/ByteStore
  (write-bytes [_ path bytes]
    (go
      (swap! contents assoc path bytes)))

  (read-bytes [_ path]
    (go
      (get @contents path))))

(defn create
  []
  (let [contents (atom {})]
    (->MemoryStore contents)))
