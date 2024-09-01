(ns fluree.db.connection.system
  (:require [fluree.db.connection :as connection]
            [fluree.db.cache :as cache]
            [fluree.db.storage.file :as file-store]
            [fluree.db.storage.memory :as memory-store]
            [fluree.db.storage.remote :as remote-store]
            [fluree.db.storage.s3 :as s3-store]
            [fluree.db.storage.ipfs :as ipfs-store]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.db.nameservice.storage-backed :as storage-nameservice]
            [fluree.db.nameservice.ipns :as ipns-nameservice]
            [fluree.db.flake.index.storage :as index.storage]
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

(derive :fluree.serializer/json :fluree/serializer)

(defmethod ig/init-key :default
  [_ component]
  component)

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
  (storage-nameservice/start address-prefix store))

(defmethod ig/init-key :fluree.nameservice/ipns
  [_ {:keys [server profile]}]
  (ipns-nameservice/initialize server profile))

(defmethod ig/init-key :fluree.serializer/json
  [_ _]
  (json-serde))

(defmethod ig/init-key :fluree.connection/id
  [_ _]
  (str (random-uuid)))

(defmethod ig/init-key :fluree.connection/state
  [_ _]
  (connection/blank-state))

(defmethod ig/init-key :fluree.index/storage
  [_ {:keys [storage serializer cache]}]
  (index.storage/index-store storage serializer cache))

(defmethod ig/init-key :fluree/connection
  [_ config]
  (connection/connect config))

(defn base-config
  [parallelism cache-max-mb defaults]
  {:fluree.serializer/json            {}
   :fluree/cache                      cache-max-mb
   :fluree.nameservice/storage-backed {:store (ig/ref :fluree/byte-storage)}
   :fluree.index/storage              {:storage    (ig/ref :fluree/content-storage)
                                       :serializer (ig/ref :fluree/serializer)
                                       :cache      (ig/ref :fluree/cache)}
   :fluree.connection/id              {}
   :fluree.connection/state           {}
   :fluree/connection                 {:id          (ig/ref :fluree.connection/id)
                                       :state       (ig/ref :fluree.connection/state)
                                       :cache       (ig/ref :fluree/cache)
                                       :store       (ig/ref :fluree/content-storage)
                                       :index-store (ig/ref :fluree.index/storage)
                                       :serializer  (ig/ref :fluree/serializer)
                                       :primary-ns  (ig/ref :fluree/nameservice)
                                       :aux-nses    []
                                       :parallelism parallelism
                                       :defaults    defaults}})

(defn memory-config
  [parallelism cache-max-mb defaults]
  (-> (base-config parallelism cache-max-mb defaults)
      (assoc :fluree.storage/memory {})
      (assoc-in [:fluree.nameservice/storage-backed :address-prefix] "fluree:memory://")))

(defn file-config
  [storage-path parallelism cache-max-mb defaults]
  (-> (base-config parallelism cache-max-mb defaults)
      (assoc :fluree.storage/file storage-path)
      (assoc-in [:fluree.nameservice/storage-backed :address-prefix] "fluree:file://")))

(defn s3-config
  [endpoint bucket prefix parallelism cache-max-mb defaults]
  (-> (base-config parallelism cache-max-mb defaults)
      (assoc :fluree.storage/s3 {:bucket bucket, :prefix prefix, :endpoint endpoint})
      (assoc-in [:fluree.nameservice/storage-backed :address-prefix] "fluree:s3://")))

(defn ipfs-config
  [server file-storage-path parallelism cache-max-mb defaults]
  (-> (file-config file-storage-path parallelism cache-max-mb defaults)
      (assoc :fluree.storage/ipfs server
             :fluree.nameservice/ipns {:profile "self", :server server, :sync? false}
             :fluree/nameservices [(ig/ref :fluree.nameservice/storage-backed)
                                   (ig/ref :fluree.nameservice/ipns)])
      (update :fluree.index/storage assoc :storage (ig/ref :fluree.storage/ipfs))
      (update :fluree/connection assoc :store (ig/ref :fluree.storage/ipfs))
      (update :fluree/connection assoc :primary-ns (ig/ref :fluree.nameservice/storage-backed))
      (update :fluree/connection assoc :aux-nses [(ig/ref :fluree.nameservice/ipns)])))

(defn start
  [config]
  (-> config ig/init :fluree/connection))
