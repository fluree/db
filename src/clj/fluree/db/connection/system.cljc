(ns fluree.db.connection.system
  (:require [fluree.db.connection :as connection]
            [fluree.db.connection.vocab :as conn-vocab]
            [fluree.db.cache :as cache]
            [fluree.db.storage :as storage]
            [fluree.db.remote-system :as remote-system]
            [fluree.db.storage.file :as file-storage]
            [fluree.db.storage.memory :as memory-storage]
            [fluree.db.storage.ipfs :as ipfs-storage]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.db.nameservice.storage :as storage-nameservice]
            [fluree.db.nameservice.ipns :as ipns-nameservice]
            [fluree.db.flake.index.storage :as index-storage]
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

#?(:cljs
   (defmethod ig/init-key :fluree.storage/localstorage
     [_ _]
     (localstorage-store/open)))

(defmethod ig/init-key :fluree/remote-system
  [_ config]
  (let [urls        (get-values config conn-vocab/server-urls)
        identifiers (get-values config conn-vocab/address-identifiers)]
    (remote-system/connect urls identifiers)))

(defmethod ig/init-key :fluree/commit-catalog
  [_ {:keys [content-stores read-only-archives]}]
  (storage/catalog content-stores read-only-archives))

(defmethod ig/init-key :fluree/index-catalog
  [_ {:keys [content-stores read-only-archives serializer cache]}]
  (let [catalog (storage/catalog content-stores read-only-archives)]
    (index-storage/index-catalog catalog serializer cache)))

(defmethod ig/init-key :fluree.nameservice/storage
  [_ config]
  (let [storage (get-first config conn-vocab/storage)]
    (storage-nameservice/start storage)))

(defmethod ig/init-key :fluree.nameservice/ipns
  [_ config]
  (let [endpoint (get-first-value config conn-vocab/ipfs-endpoint)
        ipns-key (get-first-value config conn-vocab/ipns-key)]
    (ipns-nameservice/initialize endpoint ipns-key)))

(defmethod ig/init-key :fluree.serializer/json
  [_ _]
  (json-serde))

(defmethod ig/init-key :fluree/connection
  [_ {:keys [cache commit-catalog index-catalog serializer] :as config}]
  (let [parallelism          (get-first-value config conn-vocab/parallelism)
        primary-publisher    (get-first config conn-vocab/primary-publisher)
        secondary-publishers (get config conn-vocab/secondary-publishers)
        remote-systems       (get config conn-vocab/remote-systems)
        ledger-defaults      (get-first config conn-vocab/ledger-defaults)
        index-options        (get-first ledger-defaults conn-vocab/index-options)
        reindex-min-bytes    (get-first index-options conn-vocab/reindex-min-bytes)
        reindex-max-bytes    (get-first index-options conn-vocab/reindex-max-bytes)
        max-old-indexes      (get-first index-options conn-vocab/max-old-indexes)
        ledger-defaults*     {:index-options {:reindex-min-bytes reindex-min-bytes
                                              :reindex-max-bytes reindex-max-bytes
                                              :max-old-indexes   max-old-indexes}}]
    (connection/connect {:parallelism          parallelism
                         :cache                cache
                         :commit-catalog       commit-catalog
                         :index-catalog        index-catalog
                         :primary-publisher    primary-publisher
                         :secondary-publishers secondary-publishers
                         :remote-systems       remote-systems
                         :serializer           serializer
                         :defaults             ledger-defaults*})))

(defn initialize
  [config]
  (-> config ig/init :fluree/connection))
