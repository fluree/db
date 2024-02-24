(ns fluree.db.storage.localstorage
  (:refer-clojure :exclude [read list])
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [fluree.crypto :as crypto]
            [fluree.db.platform :as platform]
            [fluree.db.storage.proto :as store-proto]
            [fluree.db.storage.util :as store-util]
            [fluree.json-ld :as json-ld]))

(defn localstorage-address
  [path]
  (str "fluree:localstorage:" path))

(defn localstorage-write
  [k v {:keys [content-address?]}]
  #?(:clj (throw (ex-info "LocalStorageStore is only supported on the Browser platform." {}))
     :cljs
     (let [hashable (if (store-util/hashable? v)
                      v
                      (json-ld/normalize-data v))
           hash     (crypto/sha2-256 hashable)
           k*       (if content-address?
                      (str k hash)
                      k)]
       (.setItem js/localStorage k* v)
       {:path    k*
        :address (localstorage-address k*)
        :hash    hash
        :size    (count hashable)})))

(defn localstorage-list
  [prefix]
  #?(:clj (throw (ex-info "LocalStorageStore is only supported on the Browser platform." {}))
     :cljs
     (->> (js/Object.keys js/localstorage)
          (filter #(str/starts-with? % prefix)))))

(defn localstorage-read
  [address]
  #?(:clj (throw (ex-info "LocalStorageStore is only supported on the Browser platform." {}))
     :cljs
     (let [k (:local (store-util/address-parts address))]
       (.getItem js/localStorage k))))

(defn localstorage-delete
  [address]
  #?(:clj (throw (ex-info "LocalStorageStore is only supported on the Browser platform." {}))
     :cljs
     (let [k (:local (store-util/address-parts address))]
       (.removeItem js/localStorage k))))

(defn localstorage-exists?
  [address]
  #?(:clj (throw (ex-info "LocalStorageStore is only supported on the Browser platform." {}))
     :cljs
     (let [k (:local (store-util/address-parts address))]
       (boolean (localstorage-read k)))))

(defrecord LocalStorageStore []
  store-proto/Store
  (address [_ k] (localstorage-address k))
  (write [_ k v opts] (async/go (localstorage-write k v opts)))
  (list [_ prefix] (async/go (localstorage-list prefix)))
  (read [_ address] (async/go (localstorage-read address)))
  (delete [_ address] (async/go (localstorage-delete address)))
  (exists? [_ address] (async/go (localstorage-exists? address))))

(defn open
  [config]
  (if-not platform/BROWSER
    (throw (ex-info "LocalStorageStore is only supported on the Browser platform."
                    {:config config}))
    (->LocalStorageStore)))
