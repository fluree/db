(ns fluree.db.connection.system
  (:require [fluree.db.connection.cache :as cache]
            [fluree.db.storage.file :as file-store]
            [fluree.db.storage.memory :as memory-store]
            [fluree.db.storage.remote :as remote-store]
            [fluree.db.storage.s3 :as s3-store]
            [fluree.db.storage.ipfs :as ipfs-store]
            [fluree.db.nameservice.storage-backed :as storage-nameservice]
            [fluree.db.nameservice.ipns :as ipns-nameservice]
            #?(:cljs [fluree.db.storage.localstorage :as localstorage-store])
            [integrant.core :as ig]))

(derive :fluree.storage/file :fluree/content-storage)
(derive :fluree.storage/file :fluree/byte-storage)
(derive :fluree.storage/file :fluree/json-archive)

(derive :fluree.storage/memory :fluree/content-storage)
(derive :fluree.storage/memory :fluree/byte-storage)
(derive :fluree.storage/memory :fluree/json-archive)

(derive :fluree.storage/s3 :fluree/content-storage)
(derive :fluree.storage/s3 :fluree/byte-storage)
(derive :fluree.storage/s3 :fluree/json-archive)

(derive :fluree.storage/ipfs :fluree/content-storage)
(derive :fluree.storage/ipfs :fluree/json-archive)

#?(:cljs (derive :fluree.storage/localstorage :fluree/content-storage))
#?(:cljs (derive :fluree.storage/localstorage :fluree/json-archive))

(derive :fluree.storage/remote-resource :fluree/json-archive)

(derive :fluree.publication/remote-resource :fluree/publication)

(derive :fluree.nameservice/storage-backed :fluree/nameservice)

(derive :fluree.nameservice/ipns :fluree/nameservice)

(defmethod ig/init-key :fluree/cache
  [_ max-mb]
  (-> max-mb cache/memory->cache-size cache/create-lru-cache atom))

(defmethod ig/init-key :fluree.storage/file
  [_ storage-path]
  (file-store/open storage-path))

(defmethod ig/init-key :fluree.storage/memory
  [_ _]
  (memory-store/create))

(defmethod ig/init-key :fluree.storage/s3
  [_ {:keys [bucket prefix endpoint]}]
  (s3-store/open bucket prefix endpoint))

(defmethod ig/init-key :fluree.storage/ipfs
  [_ endpoint]
  (ipfs-store/open endpoint))

(defmethod ig/init-key :fluree.storage/remote-resource
  [_ servers]
  (remote-store/resource servers))

#?(:cljs (defmethod ig/init-key :fluree.storage/localstorage
           [_ _]
           (localstorage-store/open)))

(defmethod ig/init-key :fluree.nameservice/storage-backed
  [_ {:keys [address-prefix store]}]
  (storage-nameservice/start address-prefix store true))

(defmethod ig/init-key :fluree.nameservice/ipns
  [_ {:keys [server profile]}]
  (ipns-nameservice/initialize server profile))

(defn start
  [config]
  (ig/init config))
