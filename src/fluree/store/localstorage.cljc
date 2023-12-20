(ns fluree.store.localstorage
  (:refer-clojure :exclude [read])
  (:require [fluree.crypto :as crypto]
            [fluree.db.platform :as platform]
            [fluree.store.proto :as store-proto]
            [fluree.store.util :as store-util]))

(defn localstorage-write
  [k v {:keys [content-address?]}]
  #?(:cljs
     (let [hash     (if (store-util/hashable? v)
                      (crypto/sha2-256 v)
                      (crypto/sha2-256 (json-ld/normalize-data v)))
           k*       (if content-address?
                      (str k hash)
                      k)]
       (.setItem js/localStorage k* v)
       {:k k*
        :hash hash
        :size (count v)})))

(defn localstorage-read
  [k]
  #?(:cljs
     (.getItem js/localStorage k)))

(defn localstorage-delete
  [k]
  #?(:cljs
     (.removeItem js/localStorage k)))

(defn localstorage-exists?
  [k]
  #?(:cljs
     (boolean (localstorage-read k))))

(defrecord LocalStorageStore []
  store-proto/Store
  (write [_ k v opts] (localstorage-write k v opts))
  (read [_ k] (localstorage-read k))
  (delete [_ k] (localstorage-delete k))
  (exists? [_ k] (localstorage-exists? k)))

(defn create-localstorage-store
  [config]
  (if-not platform/BROWSER
    (throw (ex-info "LocalStorageStore is only supported on the Browser platform."
                    {:config config}))
    (map->LocalStorageStore {:config config})))
