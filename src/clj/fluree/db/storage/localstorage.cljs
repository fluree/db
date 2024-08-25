(ns fluree.db.storage.localstorage
  (:refer-clojure :exclude [read list])
  (:require [clojure.core.async :as async :refer [go]]
            [clojure.string :as str]
            [fluree.crypto :as crypto]
            [fluree.db.platform :as platform]
            [fluree.db.storage :as storage]))

(def method-name "localstorage")

(defn local-storage-address
  [path]
  (storage/build-fluree-address method-name path))

(defrecord LocalStorageStore []
  storage/Store
  (list [_ prefix]
    (go
      (->> (js/Object.keys js/localstorage)
           (filter #(str/starts-with? % prefix)))))

  (read [_ address]
    (go
      (let [path (:local (storage/parse-address address))]
        (.getItem js/localStorage path))))

  (exists? [store address]
    (go
      (let [path (:local (storage/parse-address address))]
        (boolean (storage/read store path)))))

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
