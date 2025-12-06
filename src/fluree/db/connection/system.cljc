(ns fluree.db.connection.system
  (:require #?(:clj  [fluree.db.storage.s3 :as s3-storage]
               :cljs [fluree.db.storage.localstorage :as localstorage-store])
            #?(:clj [fluree.db.migrations.nameservice :as ns-migration])
            #?(:clj [fluree.db.nameservice.dynamodb :as dynamodb-nameservice])
            #?(:clj [fluree.db.storage.file :as file-storage])
            #?(:clj [fluree.db.util.log :as log])
            [fluree.db.cache :as cache]
            [fluree.db.connection :as connection]
            [fluree.db.connection.config :as config]
            [fluree.db.connection.vocab :as conn-vocab]
            [fluree.db.constants :as const]
            [fluree.db.flake.index.storage :as index-storage]
            [fluree.db.nameservice.ipns :as ipns-nameservice]
            [fluree.db.nameservice.storage :as storage-nameservice]
            [fluree.db.remote-system :as remote-system]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.db.storage :as storage]
            [fluree.db.storage.ipfs :as ipfs-storage]
            [fluree.db.storage.memory :as memory-storage]
            [fluree.db.util :as util :refer [get-id get-first get-first-value]]
            [integrant.core :as ig]))

#?(:clj (derive :fluree.db.storage/file :fluree.db/content-storage))
#?(:clj (derive :fluree.db.storage/file :fluree.db/byte-storage))
#?(:clj (derive :fluree.db.storage/file :fluree.db/json-archive))

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

#?(:clj (derive :fluree.db.nameservice/dynamodb :fluree.db/publisher))
#?(:clj (derive :fluree.db.nameservice/dynamodb :fluree.db/nameservice))

(derive :fluree.db.serializer/json :fluree.db/serializer)

(defn reference?
  [node]
  (and (map? node)
       (contains? node const/iri-id)
       (-> node (dissoc const/iri-id) empty?)))

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

(defmethod ig/expand-key :fluree.db/abstract-connection
  [k config]
  (let [cache-max-mb   (or (config/get-first-integer config conn-vocab/cache-max-mb)
                           (cache/default-cache-max-mb))
        commit-storage (get config conn-vocab/commit-storage)
        index-storage  (get config conn-vocab/index-storage)
        remote-systems (get config conn-vocab/remote-systems)
        config*        (-> config
                           (assoc :cache (ig/ref :fluree.db/cache)
                                  :commit-catalog (ig/ref :fluree.db/commit-catalog)
                                  :index-catalog (ig/ref :fluree.db/index-catalog)
                                  :serializer (ig/ref :fluree.db/serializer))
                           (dissoc conn-vocab/cache-max-mb conn-vocab/commit-storage
                                   conn-vocab/index-storage))]
    {:fluree.db/cache          cache-max-mb
     :fluree.db/commit-catalog {:content-stores     commit-storage
                                :read-only-archives remote-systems}
     :fluree.db/index-catalog  {:content-stores     index-storage
                                :read-only-archives remote-systems
                                :cache              (ig/ref :fluree.db/cache)
                                :serializer         (ig/ref :fluree.db/serializer)}
     k                         config*}))

(defmethod ig/init-key :default
  [_ component]
  component)

#?(:clj
   (defn get-java-prop
     [java-prop]
     (System/getProperty java-prop))

   :cljs
   (defn get-java-prop
     [_]
     (throw (ex-info "Java system properties are not supported on this platform"
                     {:status 400, :error :db/unsupported-config}))))

#?(:clj
   (defn get-env
     [env-var]
     (System/getenv env-var))

   :cljs
   (defn get-env
     [_]
     (throw (ex-info "Environment variables are not supported on this platform"
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
    {const/iri-value (get-priority-value env-var java-prop default-val)}))

(defmethod ig/init-key :fluree.db/cache
  [_ max-mb]
  (-> max-mb cache/memory->cache-size cache/create-lru-cache atom))

#?(:clj
   (defmethod ig/init-key :fluree.db.storage/file
     [_ config]
     (let [identifier     (config/get-first-string config conn-vocab/address-identifier)
           root-path      (config/get-first-string config conn-vocab/file-path)
           aes256-key     (config/get-first-string config conn-vocab/aes256-key)
           file-store     (file-storage/open identifier root-path aes256-key)]
       ;; Run nameservice migration if needed - fire and forget for now
       (ns-migration/run-migration-if-needed file-store)
       file-store)))

(defmethod ig/init-key :fluree.db.storage/memory
  [_ config]
  (let [identifier (config/get-first-string config conn-vocab/address-identifier)]
    (memory-storage/open identifier)))

#?(:clj
   (defmethod ig/init-key :fluree.db.storage/s3
     [_ config]
     ;; LOG RAW CONFIG - This will show if s3Endpoint is present at all
     (log/error "S3-INIT-CONFIG-RAW [v2025-12-06T02:00]"
                {:config-keys (keys config)
                 :has-endpoint? (contains? config conn-vocab/s3-endpoint)
                 :endpoint-raw (get config conn-vocab/s3-endpoint)
                 :full-config config})
     (let [identifier  (config/get-first-string config conn-vocab/address-identifier)
           s3-bucket   (config/get-first-string config conn-vocab/s3-bucket)
           s3-prefix   (config/get-first-string config conn-vocab/s3-prefix)
           s3-endpoint (config/get-first-string config conn-vocab/s3-endpoint)
           read-timeout-ms (config/get-first-long config conn-vocab/s3-read-timeout-ms)
           write-timeout-ms (config/get-first-long config conn-vocab/s3-write-timeout-ms)
           list-timeout-ms (config/get-first-long config conn-vocab/s3-list-timeout-ms)
           max-retries (config/get-first-integer config conn-vocab/s3-max-retries)
           retry-base-delay-ms (config/get-first-long config conn-vocab/s3-retry-base-delay-ms)
           retry-max-delay-ms (config/get-first-long config conn-vocab/s3-retry-max-delay-ms)]
       ;; DIAGNOSTIC: This fires when loading from solo3 config
       (log/warn "S3-DIAGNOSTIC: Initializing S3 storage from configuration [v2025-12-06T02:00]"
                 {:identifier identifier
                  :bucket s3-bucket
                  :prefix s3-prefix
                  :endpoint s3-endpoint
                  :code-version "2025-12-06T02:00:00Z"})
       (s3-storage/open identifier s3-bucket s3-prefix s3-endpoint
                        {:read-timeout-ms read-timeout-ms
                         :write-timeout-ms write-timeout-ms
                         :list-timeout-ms list-timeout-ms
                         :max-retries max-retries
                         :retry-base-delay-ms retry-base-delay-ms
                         :retry-max-delay-ms retry-max-delay-ms}))))

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

#?(:clj
   (defmethod ig/init-key :fluree.db.nameservice/dynamodb
     [_ config]
     (let [table-name (config/get-first-string config conn-vocab/dynamodb-table)
           region     (config/get-first-string config conn-vocab/dynamodb-region)
           endpoint   (config/get-first-string config conn-vocab/dynamodb-endpoint)
           timeout-ms (config/get-first-long config conn-vocab/dynamodb-timeout-ms)]
       (dynamodb-nameservice/start (cond-> {:table-name table-name}
                                     region     (assoc :region region)
                                     endpoint   (assoc :endpoint endpoint)
                                     timeout-ms (assoc :timeout-ms timeout-ms))))))

(defmethod ig/init-key :fluree.db.serializer/json
  [_ _]
  (json-serde))

(defmethod ig/init-key :fluree.db/connection
  [_ config]
  (-> config config/parse-connection-map connection/connect))

(defn prepare
  [parsed-config]
  (-> parsed-config convert-references ig/expand))

(defn initialize
  [parsed-config]
  (-> parsed-config prepare ig/init))

(defn terminate
  [sys]
  (ig/halt! sys))
