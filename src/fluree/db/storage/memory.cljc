(ns fluree.db.storage.memory
  (:require [clojure.core.async :as async :refer [go]]
            [clojure.string :as str]
            [fluree.crypto :as crypto]
            [fluree.db.storage :as storage]
            [fluree.db.util.json :as json]))

(def method-name "memory")

(defn memory-address
  [identifier path]
  (storage/build-fluree-address identifier method-name path))

(defrecord MemoryStore [identifier contents]
  storage/Addressable
  (location [_]
    (storage/build-location storage/fluree-namespace identifier method-name))

  storage/Identifiable
  (identifiers [_]
    #{identifier})

  storage/JsonArchive
  (-read-json [_ address keywordize?]
    (go
      (let [path (storage/get-local-path address)]
        (when-let [data (get @contents path)]
          (json/parse data keywordize?)))))

  storage/EraseableStore
  (delete [_ address]
    (go
      (let [path (storage/get-local-path address)]
        (swap! contents dissoc path))))

  storage/ContentAddressedStore
  (-content-write-bytes [_ _ v]
    (go
      (let [hashable (if (storage/hashable? v)
                       v
                       (pr-str v))
            hash     (crypto/sha2-256 hashable :base32)]
        (swap! contents assoc hash v)
        {:path    hash
         :address (memory-address identifier hash)
         :hash    hash
         :size    (count hashable)})))

  storage/ByteStore
  (write-bytes [_ path bytes]
    (go
      (swap! contents assoc path bytes)))

  (read-bytes [_ path]
    (go
      (get @contents path)))

  storage/ListableStore
  (list-paths [_ prefix]
    (go
      ;; Filter keys in contents that start with the prefix and end with .json
      (->> @contents
           keys
           (filter #(and (str/starts-with? % prefix)
                         (str/ends-with? % ".json")))
           vec))))

(defn open
  ([]
   (open nil))
  ([identifier]
   (let [contents (atom {})]
     (->MemoryStore identifier contents))))
