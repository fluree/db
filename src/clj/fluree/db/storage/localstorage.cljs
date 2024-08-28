(ns fluree.db.storage.localstorage
  (:refer-clojure :exclude [read list])
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.crypto :as crypto]
            [fluree.db.util.json :as json]
            [fluree.db.platform :as platform]
            [fluree.db.storage :as storage]))

(def method-name "localstorage")

(defn local-storage-address
  [path]
  (storage/build-fluree-address method-name path))

(defrecord LocalStorageStore []
  storage/JsonArchive
  (-read-json [_ address keywordize?]
    (go
      (let [path (:local (storage/parse-address address))]
        (when-let [data (.getItem js/localStorage path)]
          (json/parse data keywordize?)))))

  storage/EraseableStore
  (delete [_ address]
    (go
      (let [path (:local (storage/parse-address address))]
        (.removeItem js/localStorage path))))

  storage/ContentAddressedStore
  (-content-write [_ k v]
    (go
      (let [hashable (if (storage/hashable? v)
                       v
                       (pr-str v))
            hash     (crypto/sha2-256 hashable)]
        (.setItem js/localStorage k v)
        {:path    k
         :address (local-storage-address k)
         :hash    hash
         :size    (count hashable)}))))

(defn open
  [config]
  (if-not platform/BROWSER
    (throw (ex-info "LocalStorageStore is only supported on the Browser platform."
                    {:config config}))
    (->LocalStorageStore)))
