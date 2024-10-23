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

(derive :fluree.db.storage/file :fluree.db/content-storage)
(derive :fluree.db.storage/file :fluree.db/byte-storage)
(derive :fluree.db.storage/file :fluree.db/json-archive)

(derive :fluree.db.storage/memory :fluree.db/content-storage)
(derive :fluree.db.storage/memory :fluree.db/byte-storage)
(derive :fluree.db.storage/memory :fluree.db/json-archive)

#?(:clj (derive :fluree.db.storage/s3 :fluree.db/content-storage))
#?(:clj (derive :fluree.db.storage/s3 :fluree.db/byte-storage))
#?(:clj (derive :fluree.db.storage/s3 :fluree.db/json-archive))

(derive :fluree.db.storage/ipfs :fluree.db/content-storage)
(derive :fluree.db.storage/ipfs :fluree.db/json-archive)

#?(:cljs (derive :fluree.db.storage/localstorage :fluree.db/content-storage))
#?(:cljs (derive :fluree.db.storage/localstorage :fluree.db/json-archive))

(derive :fluree.db/remote-system :fluree.db/json-archive)
(derive :fluree.db/remote-system :fluree.db/nameservice)
(derive :fluree.db/remote-system :fluree.db/publication)

(derive :fluree.db.nameservice/storage :fluree.db/publisher)
(derive :fluree.db.nameservice/storage :fluree.db/nameservice)

(derive :fluree.db.nameservice/ipns :fluree.db/publisher)
(derive :fluree.db.nameservice/ipns :fluree.db/nameservice)

(derive :fluree.db.serializer/json :fluree.db/serializer)

(defmethod ig/init-key :default
  [_ component]
  component)

(defmethod ig/expand-key :fluree.db/connection
  [k config]
  (let [cache-max-mb   (get-first-value config conn-vocab/cache-max-mb)
        commit-storage (get-first config conn-vocab/commit-storage)
        index-storage  (get-first config conn-vocab/index-storage)
        remote-systems (get config conn-vocab/remote-systems)
        config*        (-> config
                           (assoc :cache (ig/ref :fluree.db/cache)
                                  :commit-catalog (ig/ref :fluree.db/commit-catalog)
                                  :index-catalog (ig/ref :fluree.db/index-catalog)
                                  :serializer (ig/ref :fluree.db/serializer))
                           (dissoc conn-vocab/cache-max-mb conn-vocab/commit-storage
                                   conn-vocab/index-storage))]
    {:fluree.db/cache          cache-max-mb
     :fluree.db/commit-catalog {:content-stores     [commit-storage]
                             :read-only-archives remote-systems}
     :fluree.db/index-catalog  {:content-stores     [index-storage]
                             :read-only-archives remote-systems
                             :cache              (ig/ref :fluree.db/cache)
                             :serializer         (ig/ref :fluree.db/serializer)}
     k                      config*}))

(defmethod ig/init-key :fluree.db/cache
  [_ max-mb]
  (-> max-mb cache/memory->cache-size cache/create-lru-cache atom))

(defmethod ig/init-key :fluree.db.storage/file
  [_ config]
  (let [identifier (get-first-value config conn-vocab/address-identifier)
        root-path  (get-first-value config conn-vocab/file-path)]
    (file-storage/open identifier root-path)))

(defmethod ig/init-key :fluree.db.storage/memory
  [_ config]
  (let [identifier (get-first-value config conn-vocab/address-identifier)]
    (memory-storage/open identifier)))

#?(:clj
   (defmethod ig/init-key :fluree.db.storage/s3
     [_ config]
     (let [identifier  (get-first-value config conn-vocab/address-identifier)
           s3-bucket   (get-first-value config conn-vocab/s3-bucket)
           s3-prefix   (get-first-value config conn-vocab/s3-prefix)
           s3-endpoint (get-first-value config conn-vocab/s3-endpoint)]
       (s3-storage/open identifier s3-bucket s3-prefix s3-endpoint))))

(defmethod ig/init-key :fluree.db.storage/ipfs
  [_ config]
  (let [identifier    (get-first-value config conn-vocab/address-identifier)
        ipfs-endpoint (get-first-value config conn-vocab/ipfs-endpoint)]
    (ipfs-storage/open identifier ipfs-endpoint)))

#?(:cljs
   (defmethod ig/init-key :fluree.db.storage/localstorage
     [_ _]
     (localstorage-store/open)))

(defmethod ig/init-key :fluree.db/remote-system
  [_ config]
  (let [urls        (get-values config conn-vocab/server-urls)
        identifiers (get-values config conn-vocab/address-identifiers)]
    (remote-system/connect urls identifiers)))

(defmethod ig/init-key :fluree.db/commit-catalog
  [_ {:keys [content-stores read-only-archives]}]
  (storage/catalog content-stores read-only-archives))

(defmethod ig/init-key :fluree.db/index-catalog
  [_ {:keys [content-stores read-only-archives serializer cache]}]
  (let [catalog (storage/catalog content-stores read-only-archives)]
    (index-storage/index-catalog catalog serializer cache)))

(defmethod ig/init-key :fluree.db.nameservice/storage
  [_ config]
  (let [storage (get-first config conn-vocab/storage)]
    (storage-nameservice/start storage)))

(defmethod ig/init-key :fluree.db.nameservice/ipns
  [_ config]
  (let [endpoint (get-first-value config conn-vocab/ipfs-endpoint)
        ipns-key (get-first-value config conn-vocab/ipns-key)]
    (ipns-nameservice/initialize endpoint ipns-key)))

(defmethod ig/init-key :fluree.db.serializer/json
  [_ _]
  (json-serde))

(defn parse-identity
  [ledger-defaults]
  (when-let [identity (get-first ledger-defaults conn-vocab/identity)]
    {:id      (get-id identity)
     :public  (get-first-value identity conn-vocab/public-key)
     :private (get-first-value identity conn-vocab/private-key)}))

(defn parse-index-options
  [ledger-defaults]
  (when-let [index-options (get-first ledger-defaults conn-vocab/index-options)]
    {:reindex-min-bytes (get-first-value index-options conn-vocab/reindex-min-bytes)
     :reindex-max-bytes (get-first-value index-options conn-vocab/reindex-max-bytes)
     :max-old-indexes   (get-first-value index-options conn-vocab/max-old-indexes)}))

(defn parse-ledger-defaults
  [config]
  (when-let [ledger-defaults (get-first config conn-vocab/ledger-defaults)]
    (let [identity      (parse-identity ledger-defaults)
          index-options (parse-index-options ledger-defaults)]
      (cond-> nil
        identity      (assoc :identity identity)
        index-options (assoc :index-options index-options)))))

(defmethod ig/init-key :fluree.db/connection
  [_ {:keys [cache commit-catalog index-catalog serializer] :as config}]
  (let [parallelism          (get-first-value config conn-vocab/parallelism)
        primary-publisher    (get-first config conn-vocab/primary-publisher)
        secondary-publishers (get config conn-vocab/secondary-publishers)
        remote-systems       (get config conn-vocab/remote-systems)
        ledger-defaults      (parse-ledger-defaults config)]
    (connection/connect {:parallelism          parallelism
                         :cache                cache
                         :commit-catalog       commit-catalog
                         :index-catalog        index-catalog
                         :primary-publisher    primary-publisher
                         :secondary-publishers secondary-publishers
                         :remote-systems       remote-systems
                         :serializer           serializer
                         :defaults             ledger-defaults})))

(defn initialize
  [config]
  (let [system-map (ig/init config)]
    (-> system-map
        :fluree.db/connection
        (assoc ::map system-map))))

(defn terminate
  [conn]
  (-> conn ::map ig/halt!))
