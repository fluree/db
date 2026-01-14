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

;;; ---------------------------------------------------------------------------
;;; Iceberg Configuration Parsing
;;; ---------------------------------------------------------------------------

#?(:clj
   (defn- parse-iceberg-auth
     "Parse IcebergAuth node from resolved config."
     [auth-node]
     (when auth-node
       {:type (keyword (or (get-first-value auth-node conn-vocab/iceberg-auth-type) "bearer"))
        :bearer-token (config/get-first-string auth-node conn-vocab/iceberg-bearer-token)
        :api-key (config/get-first-string auth-node conn-vocab/iceberg-api-key)})))

#?(:clj
   (defn- parse-iceberg-catalog
     "Parse a single IcebergCatalog node from resolved config."
     [catalog-node]
     (when catalog-node
       (let [name (get-first-value catalog-node conn-vocab/iceberg-catalog-name)
             auth-node (get-first catalog-node conn-vocab/iceberg-auth)]
         (when name
           {:name name
            :type (keyword (or (get-first-value catalog-node conn-vocab/iceberg-catalog-type) "rest"))
            :uri (config/get-first-string catalog-node conn-vocab/iceberg-rest-uri)
            :allow-vended-credentials? (let [v (get-first-value catalog-node conn-vocab/iceberg-allow-vended-credentials)]
                                         (if (nil? v) true v))
            :default-headers (get-first-value catalog-node conn-vocab/iceberg-default-headers)
            :auth (parse-iceberg-auth auth-node)})))))

#?(:clj
   (defn- parse-iceberg-cache-settings
     "Parse IcebergCache node from resolved config."
     [cache-node]
     (when cache-node
       {:enabled? (let [v (get-first-value cache-node conn-vocab/iceberg-cache-enabled)]
                    (if (nil? v) true v))
        :cache-dir (config/get-first-string cache-node conn-vocab/iceberg-cache-dir)
        :mem-cache-mb (or (config/get-first-integer cache-node conn-vocab/iceberg-mem-cache-mb) 256)
        :disk-cache-mb (config/get-first-integer cache-node conn-vocab/iceberg-disk-cache-mb)
        :block-size-mb (or (config/get-first-integer cache-node conn-vocab/iceberg-block-size-mb) 4)
        :ttl-seconds (or (config/get-first-integer cache-node conn-vocab/iceberg-cache-ttl-seconds) 300)})))

#?(:clj
   (defn- create-iceberg-cache-instance
     "Create a Caffeine cache instance from cache settings.
      This cache is created ONCE at publisher init and shared across all VGs.
      Returns nil if cache is disabled.
      Uses requiring-resolve to avoid loading Iceberg classes at namespace load time."
     [cache-settings]
     (when (:enabled? cache-settings true)
       ;; Dynamic load to avoid ClassNotFoundException when Iceberg deps not present
       (if-let [create-cache-fn (requiring-resolve 'fluree.db.tabular.seekable-stream/create-cache)]
         (create-cache-fn
          {:max-bytes (* (:mem-cache-mb cache-settings 256) 1024 1024)
           :ttl-minutes (quot (:ttl-seconds cache-settings 300) 60)})
         (do
           (log/warn "Iceberg cache requested but seekable-stream not available (missing Iceberg deps?)")
           nil)))))

#?(:clj
   (defn parse-iceberg-config
     "Parse Iceberg-related config from a publisher/nameservice config node.
      Returns nil if no Iceberg config present.

      Call this during ig/init-key where config values are already resolved."
     [ns-config-node]
     (let [catalog-nodes (get ns-config-node conn-vocab/iceberg-catalogs)
           cache-node (get-first ns-config-node conn-vocab/iceberg-cache)]
       (when (or (seq catalog-nodes) cache-node
                 (contains? ns-config-node conn-vocab/virtual-graph-allow-publish)
                 (contains? ns-config-node conn-vocab/iceberg-allow-dynamic-virtual-graphs)
                 (contains? ns-config-node conn-vocab/iceberg-allow-dynamic-catalogs))
         {:catalogs (->> catalog-nodes
                         (map parse-iceberg-catalog)
                         (filter :name)
                         (into {} (map (juxt :name identity))))
          :cache (parse-iceberg-cache-settings cache-node)
          ;; Global gate for all VG publishing (applies to ALL VG types)
          :allow-vg-publish? (let [v (get-first-value ns-config-node conn-vocab/virtual-graph-allow-publish)]
                               (if (nil? v) true v))
          ;; Iceberg-specific flags
          :allow-dynamic-vgs? (let [v (get-first-value ns-config-node conn-vocab/iceberg-allow-dynamic-virtual-graphs)]
                                (if (nil? v) true v))
          :allow-dynamic-catalogs? (let [v (get-first-value ns-config-node conn-vocab/iceberg-allow-dynamic-catalogs)]
                                     (if (nil? v) true v))
          :persist-dynamic-secrets? (let [v (get-first-value ns-config-node conn-vocab/iceberg-persist-dynamic-catalog-secrets)]
                                      (if (nil? v) false v))
          :allowed-catalog-names (let [v (util/get-values ns-config-node conn-vocab/iceberg-allowed-catalog-names)]
                                   (when (seq v) v))}))))

#?(:clj
   (defn- attach-iceberg-config
     "Attach Iceberg config and shared cache instance to a publisher.
      The cache is created ONCE here and shared across all VGs under this publisher.
      Returns publisher unchanged if no iceberg config."
     [publisher iceberg-config]
     (if iceberg-config
       (let [;; Create cache instance at publisher init time (shared across all VGs)
             cache-instance (create-iceberg-cache-instance (:cache iceberg-config))]
         (with-meta publisher {::iceberg-config iceberg-config
                               ::iceberg-cache-instance cache-instance}))
       publisher)))

#?(:clj
   (defn get-iceberg-config
     "Retrieve Iceberg config from a publisher/nameservice instance."
     [publisher]
     (-> publisher meta ::iceberg-config)))

#?(:clj
   (defn get-iceberg-cache
     "Retrieve the shared Iceberg cache instance from a publisher.
      This cache is created once at publisher init and shared across all VGs."
     [publisher]
     (-> publisher meta ::iceberg-cache-instance)))

(defmethod ig/init-key :fluree.db/config-value
  [_ config-value-node]
  (let [env-var     (get-first-value config-value-node conn-vocab/env-var)
        java-prop   (get-first-value config-value-node conn-vocab/java-prop)
        default-val (get-first-value config-value-node conn-vocab/default-val)]
    {const/iri-value (get-priority-value env-var java-prop default-val)}))

(defmethod ig/init-key :fluree.db/cache
  [_ max-mb]
  #?(:clj
     (-> max-mb cache/memory->cache-size cache/create-lru-cache)
     :cljs
     (-> max-mb cache/memory->cache-size cache/create-lru-cache atom)))

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
  (let [storage (get-first config conn-vocab/storage)
        ns (storage-nameservice/start storage)]
    #?(:clj (let [iceberg-cfg (parse-iceberg-config config)]
              (attach-iceberg-config ns iceberg-cfg))
       :cljs ns)))

(defmethod ig/init-key :fluree.db.nameservice/ipns
  [_ config]
  (let [endpoint (config/get-first-string config conn-vocab/ipfs-endpoint)
        ipns-key (config/get-first-string config conn-vocab/ipns-key)
        ns (ipns-nameservice/initialize endpoint ipns-key)]
    #?(:clj (let [iceberg-cfg (parse-iceberg-config config)]
              (attach-iceberg-config ns iceberg-cfg))
       :cljs ns)))

#?(:clj
   (defmethod ig/init-key :fluree.db.nameservice/dynamodb
     [_ config]
     (let [table-name (config/get-first-string config conn-vocab/dynamodb-table)
           region     (config/get-first-string config conn-vocab/dynamodb-region)
           endpoint   (config/get-first-string config conn-vocab/dynamodb-endpoint)
           timeout-ms (config/get-first-long config conn-vocab/dynamodb-timeout-ms)
           ns         (dynamodb-nameservice/start (cond-> {:table-name table-name}
                                                    region     (assoc :region region)
                                                    endpoint   (assoc :endpoint endpoint)
                                                    timeout-ms (assoc :timeout-ms timeout-ms)))
           iceberg-cfg (parse-iceberg-config config)]
       (attach-iceberg-config ns iceberg-cfg))))

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
