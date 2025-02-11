(ns fluree.db.connection.system
  (:require #?(:clj  [fluree.db.storage.s3 :as s3-storage]
               :cljs [fluree.db.storage.localstorage :as localstorage-store])
            [clojure.string :as str]
            [fluree.db.cache :as cache]
            [fluree.db.connection :as connection]
            [fluree.db.connection.config :as config]
            [fluree.db.connection.vocab :as conn-vocab]
            [fluree.db.flake.index.storage :as index-storage]
            [fluree.db.nameservice.ipns :as ipns-nameservice]
            [fluree.db.nameservice.storage :as storage-nameservice]
            [fluree.db.remote-system :as remote-system]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.db.storage :as storage]
            [fluree.db.storage.file :as file-storage]
            [fluree.db.storage.ipfs :as ipfs-storage]
            [fluree.db.storage.memory :as memory-storage]
            [fluree.db.util.core :as util :refer [get-id get-first get-first-value]]
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

(defn reference?
  [node]
  (and (map? node)
       (contains? node :id)
       (-> node (dissoc :idx :id) empty?)))

(defn convert-reference
  [node]
  (if (reference? node)
    (let [id (get-id node)]
      (ig/ref id))
    node))

(defn convert-node-references
  [node]
  (reduce-kv (fn [m k v]
               (let [v* (if (coll? v)
                          (mapv convert-reference v)
                          (convert-reference v))]
                 (assoc m k v*)))
             {} node))

(defn convert-references
  [cfg]
  (reduce-kv (fn [m id node]
               (assoc m id (convert-node-references node)))
             {} cfg))

(defmethod ig/expand-key :fluree.db/connection
  [k config]
  (let [cache-max-mb   (config/get-first-integer config conn-vocab/cache-max-mb)
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

(defmethod ig/init-key :default
  [_ component]
  component)

(defn get-java-prop
  [java-prop]
  #?(:clj (System/getProperty java-prop)
     :cljs (throw (ex-info "Java system properties are not supported on this platform"
                           {:status 400, :error :db/unsupported-config}))))

(defn get-env
  [env-var]
  #?(:clj (System/getenv env-var)
     :cljs (throw (ex-info "Environment variables are not supported on this platform"
                           {:status 400, :error :db/unsupported-config}))))

(defn missing-config-error
  [env-var java-prop]
  (let [env-var-msg   (and env-var (str "environment variable " env-var))
        java-prop-msg (and java-prop (str "Java system property " java-prop))
        combined-msg  (cond (and env-var-msg java-prop-msg)
                            (str env-var-msg " and " java-prop-msg)

                            env-var-msg   env-var-msg
                            java-prop-msg java-prop-msg)]
    (ex-info (str "Missing config value specified by " combined-msg)
             {:status 400, :error :db/missing-config-val})))

(defn get-priority-value
  [env-var java-prop default-val]
  (or (and java-prop
           (get-java-prop java-prop))
      (and env-var
           (get-env env-var))
      default-val
      (throw (missing-config-error env-var java-prop))))

(defmethod ig/init-key :fluree.db/config-value
  [_ config-value-node]
  (let [env-var     (get-first-value config-value-node conn-vocab/env-var)
        java-prop   (get-first-value config-value-node conn-vocab/java-prop)
        default-val (get-first-value config-value-node conn-vocab/default-val)]
    {:value (get-priority-value env-var java-prop default-val)}))

(defmethod ig/init-key :fluree.db/cache
  [_ max-mb]
  (-> max-mb cache/memory->cache-size cache/create-lru-cache atom))

(defmethod ig/init-key :fluree.db.storage/file
  [_ config]
  (let [identifier (config/get-first-string config conn-vocab/address-identifier)
        root-path  (config/get-first-string config conn-vocab/file-path)]
    (file-storage/open identifier root-path)))

(defmethod ig/init-key :fluree.db.storage/memory
  [_ config]
  (let [identifier (config/get-first-string config conn-vocab/address-identifier)]
    (memory-storage/open identifier)))

#?(:clj
   (defmethod ig/init-key :fluree.db.storage/s3
     [_ config]
     (let [identifier  (config/get-first-string config conn-vocab/address-identifier)
           s3-bucket   (config/get-first-string config conn-vocab/s3-bucket)
           s3-prefix   (config/get-first-string config conn-vocab/s3-prefix)
           s3-endpoint (config/get-first-string config conn-vocab/s3-endpoint)]
       (s3-storage/open identifier s3-bucket s3-prefix s3-endpoint))))

(defmethod ig/init-key :fluree.db.storage/ipfs
  [_ config]
  (let [identifier    (config/get-first-string config conn-vocab/address-identifier)
        ipfs-endpoint (config/get-first-string config conn-vocab/ipfs-endpoint)]
    (ipfs-storage/open identifier ipfs-endpoint)))

#?(:cljs
   (defmethod ig/init-key :fluree.db.storage/localstorage
     [_ _]
     (localstorage-store/open)))

(defmethod ig/init-key :fluree.db/remote-system
  [_ config]
  (let [urls        (config/get-strings config conn-vocab/server-urls)
        identifiers (config/get-strings config conn-vocab/address-identifiers)]
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
  (let [endpoint (config/get-first-string config conn-vocab/ipfs-endpoint)
        ipns-key (config/get-first-string config conn-vocab/ipns-key)]
    (ipns-nameservice/initialize endpoint ipns-key)))

(defmethod ig/init-key :fluree.db.serializer/json
  [_ _]
  (json-serde))

(defn parse-identity
  [defaults]
  (when-let [identity (get-first defaults conn-vocab/identity)]
    {:id      (get-id identity)
     :public  (config/get-first-string identity conn-vocab/public-key)
     :private (config/get-first-string identity conn-vocab/private-key)}))

(defn parse-index-options
  [defaults]
  (when-let [index-options (get-first defaults conn-vocab/index-options)]
    {:reindex-min-bytes (config/get-first-integer index-options conn-vocab/reindex-min-bytes)
     :reindex-max-bytes (config/get-first-integer index-options conn-vocab/reindex-max-bytes)
     :max-old-indexes   (config/get-first-integer index-options conn-vocab/max-old-indexes)}))

(defn parse-defaults
  [config]
  (when-let [defaults (get-first config conn-vocab/defaults)]
    (let [identity      (parse-identity defaults)
          index-options (parse-index-options defaults)]
      (cond-> nil
        identity      (assoc :identity identity)
        index-options (assoc :indexing index-options)))))

(defmethod ig/init-key :fluree.db/connection
  [_ {:keys [cache commit-catalog index-catalog serializer] :as config}]
  (let [parallelism          (config/get-first-integer config conn-vocab/parallelism)
        primary-publisher    (get-first config conn-vocab/primary-publisher)
        secondary-publishers (get config conn-vocab/secondary-publishers)
        remote-systems       (get config conn-vocab/remote-systems)
        defaults             (parse-defaults config)]
    (connection/connect {:parallelism          parallelism
                         :cache                cache
                         :commit-catalog       commit-catalog
                         :index-catalog        index-catalog
                         :primary-publisher    primary-publisher
                         :secondary-publishers secondary-publishers
                         :remote-systems       remote-systems
                         :serializer           serializer
                         :defaults             defaults})))

(defn prepare
  [parsed-config]
  (-> parsed-config convert-references ig/expand))

(defn parsed-initialize
  [parsed-config]
  (-> parsed-config prepare ig/init))

(defn initialize
  [config]
  (-> config config/parse parsed-initialize))

(defn terminate
  [sys]
  (ig/halt! sys))
