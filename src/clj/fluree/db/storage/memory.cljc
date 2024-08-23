(ns fluree.db.storage.memory
  (:require [clojure.core.async :as async :refer [go]]
            [clojure.string :as str]
            [fluree.crypto :as crypto]
            [fluree.db.storage :as storage]))

(def method-name "memory")

(defn memory-address
  [path]
  (storage/build-fluree-address method-name path))

(defrecord MemoryStore [contents]
  storage/ContentAddressedStore
  (write [_ _ v]
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

  (list [_ prefix]
    (go
      (filter #(when (string? %) (str/starts-with? % prefix))
              (keys contents))))

  (read [_ address]
    (go
      (let [path (:local (storage/parse-address address))]
        (get @contents path))))

  (delete [_ address]
    (go
      (let [path (:local (storage/parse-address address))]
        (swap! contents dissoc path))))

  (exists? [_ address]
    (go
      (let [path (:local (storage/parse-address address))]
        (contains? @contents path))))

  storage/ByteStore
  (write-bytes [_ path bytes]
    (swap! contents assoc path bytes))

  (read-bytes [_ path]
    (get @contents path)))

(defn create
  []
  (let [contents (atom {})]
    (->MemoryStore contents)))
