(ns fluree.db.connection.system
  (:require [fluree.db.connection :as connection]
            [fluree.db.connection.vocab :as conn-vocab]
            [fluree.db.cache :as cache]
            [fluree.db.storage :as storage]
            [fluree.db.remote-system :as remote]
            [fluree.db.storage.file :as file-storage]
            [fluree.db.storage.memory :as memory-storage]
            [fluree.db.storage.ipfs :as ipfs-storage]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.db.nameservice.storage :as storage-nameservice]
            [fluree.db.nameservice.ipns :as ipns-nameservice]
            [fluree.db.flake.index.storage :as index.storage]
            #?(:clj  [fluree.db.storage.s3 :as s3-storage]
               :cljs [fluree.db.storage.localstorage :as localstorage-store])
            [fluree.db.util.core :as util :refer [get-id get-first get-first-value get-values]]
            [integrant.core :as ig]))

(derive :fluree.storage/file :fluree/content-storage)
(derive :fluree.storage/file :fluree/byte-storage)
(derive :fluree.storage/file :fluree/json-archive)

(derive :fluree.storage/memory :fluree/content-storage)
(derive :fluree.storage/memory :fluree/byte-storage)
(derive :fluree.storage/memory :fluree/json-archive)

#?(:clj (derive :fluree.storage/s3 :fluree/content-storage))
#?(:clj (derive :fluree.storage/s3 :fluree/byte-storage))
#?(:clj (derive :fluree.storage/s3 :fluree/json-archive))

(derive :fluree.storage/ipfs :fluree/content-storage)
(derive :fluree.storage/ipfs :fluree/json-archive)

#?(:cljs (derive :fluree.storage/localstorage :fluree/content-storage))
#?(:cljs (derive :fluree.storage/localstorage :fluree/json-archive))

(derive :fluree/remote-system :fluree/json-archive)
(derive :fluree/remote-system :fluree/nameservice)
(derive :fluree/remote-system :fluree/publication)

(derive :fluree.publication/remote-resources :fluree/publication)

(derive :fluree.nameservice/storage :fluree/publisher)
(derive :fluree.nameservice/storage :fluree/nameservice)

(derive :fluree.nameservice/ipns :fluree/publisher)
(derive :fluree.nameservice/ipns :fluree/nameservice)

(derive :fluree.serializer/json :fluree/serializer)

(defmethod ig/init-key :default
  [_ component]
  component)

(defmethod ig/expand-key :fluree/connection
  [k config]
  (let [cache-max-mb   (get-first-value config conn-vocab/cache-max-mb)
        commit-storage (get-first config conn-vocab/commit-storage)
        index-storage  (get-first config conn-vocab/index-storage)
        remote-systems (get config conn-vocab/remote-systems)
        config*        (-> config
                           (assoc :cache (ig/ref :fluree/cache)
                                  :commit-catalog (ig/ref :fluree/commit-catalog)
                                  :index-catalog (ig/ref :fluree/index-catalog)
                                  :serializer (ig/ref :fluree/serializer))
                           (dissoc conn-vocab/cache-max-mb conn-vocab/commit-storage
                                   conn-vocab/index-storage))]
    {:fluree/cache          cache-max-mb
     :fluree/commit-catalog {:content-stores     [commit-storage]
                             :read-only-archives remote-systems}
     :fluree/index-catalog  {:content-stores     [index-storage]
                             :read-only-archives remote-systems
                             :cache              (ig/ref :fluree/cache)
                             :serializer         (ig/ref :fluree/serializer)}
     k                      config*}))

(defmethod ig/init-key :fluree/cache
  [_ max-mb]
  (-> max-mb cache/memory->cache-size cache/create-lru-cache atom))

(defmethod ig/init-key :fluree.storage/file
  [_ config]
  (let [identifier (get-first-value config conn-vocab/address-identifier)
        root-path  (get-first-value config conn-vocab/file-path)]
    (file-storage/open identifier root-path)))

(defmethod ig/init-key :fluree.storage/memory
  [_ config]
  (let [identifier (get-first-value config conn-vocab/address-identifier)]
    (memory-storage/open identifier)))

#?(:clj
   (defmethod ig/init-key :fluree.storage/s3
     [_ config]
     (let [identifier  (get-first-value config conn-vocab/address-identifier)
           s3-bucket   (get-first-value config conn-vocab/s3-bucket)
           s3-prefix   (get-first-value config conn-vocab/s3-prefix)
           s3-endpoint (get-first-value config conn-vocab/s3-endpoint)]
       (s3-storage/open identifier s3-bucket s3-prefix s3-endpoint))))

(defmethod ig/init-key :fluree.storage/ipfs
  [_ config]
  (let [identifier    (get-first-value config conn-vocab/address-identifier)
        ipfs-endpoint (get-first-value config conn-vocab/ipfs-endpoint)]
    (ipfs-storage/open identifier ipfs-endpoint)))

#?(:cljs (defmethod ig/init-key :fluree.storage/localstorage
           [_ _]
           (localstorage-store/open)))

(defmethod ig/init-key :fluree.nameservice/storage
  [_ {:keys [storage]}]
  (storage-nameservice/start storage))

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

(defmethod ig/init-key :fluree/catalog
  [_ {:keys [storage]}]
  (storage/catalog [storage]))

(defmethod ig/init-key :fluree.index/storage
  [_ {:keys [catalog serializer cache]}]
  (index.storage/index-catalog catalog serializer cache))

(defmethod ig/init-key :fluree/connection
  [_ config]
  (connection/connect config))

(defn base-config
  [parallelism cache-max-mb defaults]
  {:fluree.serializer/json            {}
   :fluree/cache                      cache-max-mb
   :fluree.nameservice/storage {:storage (ig/ref :fluree/byte-storage)}
   :fluree/catalog                    {:storage (ig/ref :fluree/content-storage)}
   :fluree.index/storage              {:catalog    (ig/ref :fluree/catalog)
                                       :serializer (ig/ref :fluree/serializer)
                                       :cache      (ig/ref :fluree/cache)}
   :fluree.connection/id              {}
   :fluree.connection/state           {}
   :fluree/connection                 {:id                   (ig/ref :fluree.connection/id)
                                       :state                (ig/ref :fluree.connection/state)
                                       :cache                (ig/ref :fluree/cache)
                                       :commit-catalog       (ig/ref :fluree/catalog)
                                       :index-catalog        (ig/ref :fluree.index/storage)
                                       :serializer           (ig/ref :fluree/serializer)
                                       :primary-publisher    (ig/ref :fluree/nameservice)
                                       :secondary-publishers []
                                       :parallelism          parallelism
                                       :defaults             defaults}})

(defn memory-config
  [parallelism cache-max-mb defaults]
  (-> (base-config parallelism cache-max-mb defaults)
      (assoc :fluree.storage/memory {})))

(defn file-config
  [storage-path parallelism cache-max-mb defaults]
  (-> (base-config parallelism cache-max-mb defaults)
      (assoc :fluree.storage/file storage-path)))

#?(:clj
   (defn s3-config
     [endpoint bucket prefix parallelism cache-max-mb defaults]
     (-> (base-config parallelism cache-max-mb defaults)
         (assoc :fluree.storage/s3 {:bucket bucket, :prefix prefix, :endpoint endpoint}))))

(defn ipfs-config
  [server file-storage-path parallelism cache-max-mb defaults]
  (-> (file-config file-storage-path parallelism cache-max-mb defaults)
      (assoc :fluree.storage/ipfs server
             :fluree.nameservice/ipns {:profile "self", :server server, :sync? false}
             :fluree/nameservices [(ig/ref :fluree.nameservice/storage)
                                   (ig/ref :fluree.nameservice/ipns)])
      (update :fluree.index/storage assoc :storage (ig/ref :fluree.storage/ipfs))
      (update :fluree/connection assoc :commit-store (ig/ref :fluree.storage/ipfs))
      (update :fluree/connection assoc :primary-ns (ig/ref :fluree.nameservice/storage))
      (update :fluree/connection assoc :aux-nses [(ig/ref :fluree.nameservice/ipns)])))

(defn start
  [config]
  (-> config ig/init :fluree/connection))
