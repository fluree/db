(ns fluree.db.virtual-graph.iceberg.factory
  "Factory functions for creating IcebergDatabase virtual graphs.

   Handles configuration parsing, catalog resolution, and VG construction."
  (:require [clojure.string :as str]
            [fluree.db.storage.s3 :as s3]
            [fluree.db.storage.vended-s3 :as vended-s3]
            [fluree.db.tabular.iceberg :as iceberg]
            [fluree.db.tabular.iceberg.rest :as iceberg-rest]
            [fluree.db.tabular.protocol :as tabular]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.util.log :as log]
            [fluree.db.virtual-graph.iceberg.join :as join]
            [fluree.db.virtual-graph.iceberg.query :as query]
            [fluree.db.virtual-graph.iceberg.r2rml :as r2rml])
  (:import [java.time Instant]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; Time Travel Parsing
;;; ---------------------------------------------------------------------------

(defn parse-time-travel
  "Convert time-travel value from parse-ledger-alias to Iceberg format.

   Used at query-time to parse time-travel from FROM clause aliases.

   Input (from parse-ledger-alias :t value):
   - nil -> nil (latest snapshot)
   - Long -> {:snapshot-id Long} (t: syntax)
   - String -> {:as-of-time Instant} (iso: syntax)
   - {:sha ...} -> not supported for Iceberg, throws

   Output:
   - nil
   - {:snapshot-id Long}
   - {:as-of-time Instant}

   Example:
     (parse-time-travel 12345)
     ;; => {:snapshot-id 12345}

     (parse-time-travel \"2024-01-15T00:00:00Z\")
     ;; => {:as-of-time #inst \"2024-01-15T00:00:00Z\"}"
  [t-val]
  (cond
    (nil? t-val)
    nil

    (integer? t-val)
    {:snapshot-id t-val}

    (string? t-val)
    {:as-of-time (Instant/parse t-val)}

    (and (map? t-val) (:sha t-val))
    (throw (ex-info "SHA-based time travel not supported for Iceberg virtual graphs"
                    {:error :db/invalid-config :t t-val}))

    :else
    (throw (ex-info "Invalid time travel value"
                    {:error :db/invalid-config :t t-val}))))

(defn- validate-snapshot-exists
  "Validate that a snapshot exists in the Iceberg table.
   Returns the snapshot info if valid, throws if not found."
  [source table-name time-travel]
  (let [opts (cond-> {}
               (:snapshot-id time-travel)
               (assoc :snapshot-id (:snapshot-id time-travel))

               (:as-of-time time-travel)
               (assoc :as-of-time (:as-of-time time-travel)))
        stats (tabular/get-statistics source table-name opts)]
    (when-not stats
      (throw (ex-info "Snapshot not found for time-travel specification"
                      {:error :db/invalid-time-travel
                       :time-travel time-travel
                       :table table-name})))
    stats))

(defn with-time-travel
  "Create a view of this IcebergDatabase pinned to a specific snapshot.

   Validates that the snapshot/time exists before returning.
   Returns a new IcebergDatabase with time-travel set.

   Usage (from query resolver when parsing FROM <airlines@t:12345>):
     (let [{:keys [t]} (parse-ledger-alias \"airlines@t:12345\")
           time-travel (parse-time-travel t)]
       (with-time-travel registered-db time-travel))

   The returned database will use the specified snapshot for all queries.
   If time-travel is nil, returns the database unchanged (latest snapshot)."
  [iceberg-db time-travel]
  (if time-travel
    (let [{:keys [sources mappings]} iceberg-db
          ;; Validate against the first table (all tables should have same snapshot time for consistency)
          table-name (some-> mappings vals first :table)
          source (when table-name (get sources table-name))]
      (when (and table-name source)
        (validate-snapshot-exists source table-name time-travel))
      (assoc iceberg-db :time-travel time-travel))
    iceberg-db))

;;; ---------------------------------------------------------------------------
;;; Catalog Resolution by Name
;;; ---------------------------------------------------------------------------

(defn- normalize-catalog-name
  "Accept both 'catalog-name' (kebab) and 'catalogName' (camel).
   Returns the catalog name or nil if not present."
  [catalog]
  (or (:catalog-name catalog)
      (get catalog "catalog-name")
      (get catalog "catalogName")))

(defn- resolve-catalog-config
  "Resolve catalog configuration, either from catalog-name or inline config.
   Returns {:uri :auth-token :allow-vended-credentials? :default-headers} or nil.

   If iceberg-config has a :catalogs map with pre-configured catalogs,
   catalog-name can be used to look up the full config. Otherwise,
   uses inline config from the catalog map directly."
  [catalog iceberg-config]
  (when catalog
    (let [catalog-name (normalize-catalog-name catalog)]
      (if catalog-name
        ;; Resolve from pre-configured catalogs
        (when-let [cfg (get-in iceberg-config [:catalogs catalog-name])]
          {:uri (:uri cfg)
           :auth-token (get-in cfg [:auth :bearer-token])
           :allow-vended-credentials? (:allow-vended-credentials? cfg true)
           :default-headers (:default-headers cfg)})
        ;; Use inline config (dynamic catalog)
        ;; Note: Use contains? for allow-vended-credentials since `or` fails on explicit false
        {:uri (or (:uri catalog) (get catalog "uri"))
         :auth-token (or (:auth-token catalog) (get catalog "auth-token"))
         :allow-vended-credentials? (let [v (if (contains? catalog :allow-vended-credentials)
                                              (:allow-vended-credentials catalog)
                                              (get catalog "allow-vended-credentials"))]
                                      (if (nil? v) true v))
         :default-headers (or (:default-headers catalog) (get catalog "default-headers"))}))))

;;; ---------------------------------------------------------------------------
;;; Store Creation from Configuration
;;; ---------------------------------------------------------------------------

(defn- normalize-store-config
  "Normalize store configuration to a consistent format.
   Accepts both keyword and string keys (kebab and camelCase).
   Returns nil if store-config is nil or not a map."
  [store-config]
  (when (map? store-config)
    {:type     (keyword (or (:type store-config)
                            (get store-config "type")
                            :s3))
     :bucket   (or (:bucket store-config)
                   (get store-config "bucket"))
     :prefix   (or (:prefix store-config)
                   (get store-config "prefix")
                   "")
     :endpoint (or (:endpoint store-config)
                   (get store-config "endpoint"))}))

(defn- create-store-from-config
  "Create a storage store from configuration data.

   Store config format:
     {:type :s3
      :bucket \"warehouse\"
      :prefix \"\"           ; optional, defaults to \"\"
      :endpoint \"http://localhost:9000\"}  ; optional for real S3

   Currently supports :s3 type. Returns nil if config is invalid."
  [store-config]
  (let [{:keys [type bucket prefix endpoint]} (normalize-store-config store-config)]
    (when (and type bucket)
      (case type
        :s3 (s3/open nil bucket (or prefix "") endpoint)
        ;; Add other store types here as needed
        (do
          (log/warn "Unknown store type:" type)
          nil)))))

;;; ---------------------------------------------------------------------------
;;; IcebergDatabase Construction
;;; ---------------------------------------------------------------------------

(defn create-iceberg-database
  "Create an IcebergDatabase record instance.

   This is called by the public `create` function after all configuration
   is resolved. Separating this allows the main `create` to return
   the IcebergDatabase record which is defined in the parent namespace.

   Returns a map suitable for map->IcebergDatabase."
  [{:keys [alias config sources mappings routing-indexes join-graph]}]
  {:alias alias
   :config config
   :sources sources
   :mappings mappings
   :routing-indexes routing-indexes
   :join-graph join-graph
   :time-travel nil
   :query-pushdown (atom nil)
   :aggregation-spec (atom nil)
   :anti-join-spec (atom nil)
   :expression-evaluators (atom nil)
   :transitive-spec (atom nil)})

(defn resolve-config
  "Resolve and validate configuration for an IcebergDatabase.

   Returns a map with resolved configuration suitable for creating sources.

   Args:
     alias          - Virtual graph alias with optional branch (required)
     config         - Configuration map containing warehouse/store/mapping info
     iceberg-config - Optional publisher-level Iceberg config (catalogs, cache, etc.)
     cache-instance - Optional shared cache instance from publisher

   Returns:
     {:base-alias string
      :warehouse-path string-or-nil
      :store store-or-nil
      :metadata-location string-or-nil
      :resolved-catalog map-or-nil
      :catalog-type keyword-or-nil
      :rest-catalog? boolean
      :vended-enabled? boolean
      :cache-settings map-or-nil
      :block-size int-or-nil
      :file-io-opts map
      :mappings map
      :table-names seq}"
  [alias config iceberg-config cache-instance]
  ;; Reject @ in alias - reserved character
  (when (str/includes? alias "@")
    (throw (ex-info (str "Virtual graph name cannot contain '@' character. Provided: " alias)
                    {:error :db/invalid-config :alias alias})))

  ;; Parse alias for name and branch only
  (let [{:keys [ledger branch]} (util.ledger/parse-ledger-alias alias)
        base-alias (if branch (str ledger ":" branch) ledger)

        ;; Get warehouse/store config
        warehouse-path (or (:warehouse-path config)
                           (get config "warehouse-path")
                           (get config "warehousePath"))
        ;; Store can be either:
        ;; 1. Configuration data: {:type :s3 :bucket "..." :endpoint "..."}
        ;; 2. Already a store object (legacy/internal use)
        ;; We detect config data by checking for :bucket or "bucket" keys
        store-raw (or (:store config) (get config "store"))
        store (if (and (map? store-raw)
                       (or (:bucket store-raw) (get store-raw "bucket")))
                ;; It's configuration data - create the store
                (create-store-from-config store-raw)
                ;; It's already a store object (or nil)
                store-raw)
        metadata-location (or (:metadata-location config)
                              (get config "metadata-location")
                              (get config "metadataLocation"))

        ;; Catalog config (REST) - resolve by name or use inline
        catalog (or (:catalog config) (get config "catalog"))
        resolved-catalog (resolve-catalog-config catalog iceberg-config)
        catalog-type (keyword (or (:type catalog) (get catalog "type")))
        rest-catalog? (= catalog-type :rest)
        vended-enabled? (:allow-vended-credentials? resolved-catalog true)

        ;; Cache settings from publisher config
        cache-settings (:cache iceberg-config)
        block-size (when cache-settings (* (:block-size-mb cache-settings 4) 1024 1024))

        _ (when-not (or warehouse-path store rest-catalog?)
            (throw (ex-info "Iceberg virtual graph requires :warehouse-path or :store (REST catalog mode also requires :store)"
                            {:error :db/invalid-config :config config})))
        _ (when (and rest-catalog? (nil? store) (not vended-enabled?))
            (throw (ex-info "Iceberg virtual graph REST :catalog requires :store config {:type :s3 :bucket \"...\" :endpoint \"...\"} unless vended credentials are enabled"
                            {:error :db/invalid-config :config config})))

        ;; Get mapping
        mapping-source (or (:mappingInline config)
                           (get config "mappingInline")
                           (:mapping config)
                           (get config "mapping"))
        _ (when-not mapping-source
            (throw (ex-info "Iceberg virtual graph requires :mapping or :mappingInline"
                            {:error :db/invalid-config :config config})))

        ;; Parse R2RML mappings first to discover all tables
        mappings (r2rml/parse-r2rml mapping-source)

        ;; Extract unique table names from all mappings
        table-names (->> mappings
                         vals
                         (map :table)
                         (remove nil?)
                         distinct)

        ;; File IO options for shared cache
        file-io-opts {:cache-instance cache-instance
                      :block-size block-size}]

    {:base-alias base-alias
     :warehouse-path warehouse-path
     :store store
     :metadata-location metadata-location
     :resolved-catalog resolved-catalog
     :catalog-type catalog-type
     :rest-catalog? rest-catalog?
     :vended-enabled? vended-enabled?
     :cache-settings cache-settings
     :block-size block-size
     :file-io-opts file-io-opts
     :mappings mappings
     :table-names table-names
     :config config}))

(defn create-sources
  "Create IcebergSource instances for each table in the mappings.

   Returns a map of {table-name -> IcebergSource}."
  [{:keys [table-names rest-catalog? vended-enabled? store
           resolved-catalog file-io-opts warehouse-path]}]
  (let [;; Create source factory function
        ;; When vended credentials are enabled for REST catalogs, create a VendedCredentialsStore
        create-source-fn (cond
                           ;; REST catalog with vended credentials enabled (no explicit store)
                           (and rest-catalog? vended-enabled? (nil? store))
                           (let [;; Create credential provider once, reused for all tables
                                 cred-provider (iceberg-rest/make-credential-provider
                                                (:uri resolved-catalog)
                                                (:auth-token resolved-catalog))]
                             (fn [table-name]
                               (let [vended-store (vended-s3/create-vended-s3-store
                                                   cred-provider
                                                   table-name)]
                                 (iceberg/create-rest-iceberg-source
                                  {:uri (:uri resolved-catalog)
                                   :store vended-store
                                   :auth-token (:auth-token resolved-catalog)
                                   :file-io-opts file-io-opts}))))

                           ;; REST catalog with explicit store - must check before store-only!
                           (and rest-catalog? store)
                           (fn [_table-name]
                             (iceberg/create-rest-iceberg-source
                              {:uri (:uri resolved-catalog)
                               :store store
                               :auth-token (:auth-token resolved-catalog)
                               :file-io-opts file-io-opts}))

                           ;; Explicit store provided without REST catalog (store-backed warehouse)
                           store
                           (fn [_table-name]
                             (iceberg/create-fluree-iceberg-source
                              {:store store
                               :warehouse-path (or warehouse-path "")
                               :file-io-opts file-io-opts}))

                           ;; Hadoop-based (legacy, no store)
                           :else
                           (fn [_table-name]
                             (iceberg/create-iceberg-source
                              {:warehouse-path warehouse-path})))]
    ;; Create an IcebergSource for each unique table
    ;; For vended credentials, each table gets its own store with table-specific credentials
    (into {}
          (for [table-name table-names]
            [table-name (create-source-fn table-name)]))))

(defn backend-description
  "Generate a human-readable description of the Iceberg backend configuration."
  [{:keys [rest-catalog? vended-enabled? store resolved-catalog warehouse-path]}]
  (cond
    (and rest-catalog? vended-enabled? (nil? store)) (str "rest+vended:" (:uri resolved-catalog))
    store "store-backed"
    rest-catalog? (str "rest:" (:uri resolved-catalog))
    :else (str "warehouse:" warehouse-path)))

(defn build-database-map
  "Build the complete database configuration map for IcebergDatabase creation.

   This performs all the resolution, validation, and source creation needed
   to instantiate an IcebergDatabase.

   Args:
     alias          - Virtual graph alias
     config         - Configuration map
     iceberg-config - Publisher-level Iceberg config
     cache-instance - Shared cache instance

   Returns a map suitable for map->IcebergDatabase."
  [alias config iceberg-config cache-instance]
  (let [resolved (resolve-config alias config iceberg-config cache-instance)
        sources (create-sources resolved)
        routing-indexes (query/build-routing-indexes (:mappings resolved))
        join-graph (join/build-join-graph (:mappings resolved))
        backend-desc (backend-description resolved)]

    (log/info "Created Iceberg virtual graph:" (:base-alias resolved) backend-desc
              "tables:" (vec (:table-names resolved))
              "mappings:" (count (:mappings resolved))
              "join-edges:" (count (:edges join-graph)))

    (create-iceberg-database
     {:alias (:base-alias resolved)
      :config (cond-> (:config resolved)
                (:metadata-location resolved)
                (assoc :metadata-location (:metadata-location resolved)))
      :sources sources
      :mappings (:mappings resolved)
      :routing-indexes routing-indexes
      :join-graph join-graph})))
