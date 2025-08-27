(ns fluree.db.storage.localstorage
  (:refer-clojure :exclude [read list])
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.crypto :as crypto]
            [fluree.db.platform :as platform]
            [fluree.db.storage :as storage]
            [fluree.db.util.json :as json]))

(def method-name "localstorage")

(defn local-storage-address
  [identifier path]
  (storage/build-fluree-address identifier method-name path))

(defrecord LocalStorageStore [identifier]
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
        (when-let [data (.getItem js/localStorage path)]
          (json/parse data keywordize?)))))

  storage/EraseableStore
  (delete [_ address]
    (go
      (let [path (storage/get-local-path address)]
        (.removeItem js/localStorage path))))

  storage/ContentAddressedStore
  (-content-write-bytes [_ _ v]
    (go
      (let [hashable (if (storage/hashable? v)
                       v
                       (pr-str v))
            hash     (crypto/sha2-256 hashable :base32)
            address  (local-storage-address identifier hash)]
        (.setItem js/localStorage address v)
        {:address address
         :hash    hash
         :size    (count hashable)})))

  storage/ContentArchive
  (-content-read-bytes [_ address]
    (go
      (let [path (storage/get-local-path address)]
        (.getItem js/localStorage path))))

  (get-hash [_ address]
    (-> address storage/split-address last)))

(defn open
  ([]
   (open nil))
  ([identifier]
   (if-not platform/BROWSER
     (throw (ex-info "LocalStorageStore is only supported on the Browser platform."
                     {}))
     (->LocalStorageStore identifier))))
