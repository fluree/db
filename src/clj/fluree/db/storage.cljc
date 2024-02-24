(ns fluree.db.storage
  (:refer-clojure :exclude [read list exists?])
  (:require [fluree.db.storage.proto :as store-proto]))


(defn address
  [store k]
  (store-proto/address store k))

(defn write
  ([store k v]
   (store-proto/write store k v nil))
  ([store k v opts]
   (store-proto/write store k v opts)))

(defn read
  [store address]
  (store-proto/read store address))

(defn list
  [store prefix]
  (store-proto/list store prefix))

(defn delete
  [store address]
  (store-proto/delete store address))

(defn exists?
  [store address]
  (store-proto/exists? store address))
