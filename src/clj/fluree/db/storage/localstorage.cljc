(ns fluree.db.storage.localstorage
  (:refer-clojure :exclude [read list])
  (:require [clojure.core.async :as async :refer [go]]
            [clojure.string :as str]
            [fluree.crypto :as crypto]
            [fluree.db.platform :as platform]
            [fluree.db.storage :as storage]
            [fluree.json-ld :as json-ld]))

(def method-name "localstorage")

(defrecord LocalStorageStore []
  storage/Store
  (address [_ path]
    (storage/build-fluree-address method-name path))

  (write [store k v]
    (go
      #?(:clj (throw (ex-info "LocalStorageStore is only supported on the Browser platform." {}))
         :cljs
         (let [hashable (if (storage/hashable? v)
                          v
                          (json-ld/normalize-data v))
               hash     (crypto/sha2-256 hashable)]
           (.setItem js/localStorage k v)
           {:path    k
            :address (storage/address store k)
            :hash    hash
            :size    (count hashable)}))))

  (list [_ prefix]
    (go
      #?(:clj (throw (ex-info "LocalStorageStore is only supported on the Browser platform." {}))
         :cljs
         (->> (js/Object.keys js/localstorage)
              (filter #(str/starts-with? % prefix))))))

  (read [_ address]
    (go
      #?(:clj (throw (ex-info "LocalStorageStore is only supported on the Browser platform." {}))
         :cljs
         (let [path (:local (storage/parse-address address))]
           (.getItem js/localStorage path)))))

  (delete [_ address]
    (go
      #?(:clj (throw (ex-info "LocalStorageStore is only supported on the Browser platform." {}))
         :cljs
         (let [path (:local (storage/parse-address address))]
           (.removeItem js/localStorage path)))))

  (exists? [store address]
    (go
      #?(:clj (throw (ex-info "LocalStorageStore is only supported on the Browser platform." {}))
         :cljs
         (let [path (:local (storage/parse-address address))]
           (boolean (storage/read store path)))))))

(defn open
  [config]
  (if-not platform/BROWSER
    (throw (ex-info "LocalStorageStore is only supported on the Browser platform."
                    {:config config}))
    (->LocalStorageStore)))
