(ns fluree.db.virtual-graph.create
  "Handles creation of virtual graphs, delegating to type-specific implementations."
  (:require #?(:clj [fluree.db.connection.system :as system])
            [clojure.string :as str]
            [fluree.db.connection :as connection]
            [fluree.db.ledger :as ledger]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.virtual-graph :as vg]
            [fluree.db.virtual-graph.nameservice-loader :as vg-loader]))

(defmulti create-vg
  "Creates a virtual graph based on its type. Dispatches on :type key."
  (fn [_conn config] (:type config)))

(defn- validate-common-config
  "Validates common configuration parameters for all virtual graph types."
  [{vg-type :type :keys [name] :as config}]
  (cond
    (not name)
    (throw (ex-info "Virtual graph requires :name"
                    {:error :db/invalid-config :config config}))

    (not (string? name))
    (throw (ex-info "Virtual graph :name must be a string"
                    {:error :db/invalid-config :name name :type (type name)}))

    (not vg-type)
    (throw (ex-info "Virtual graph requires :type"
                    {:error :db/invalid-config :config config}))

    ;; @ is reserved for query-time time-travel, not registration
    (str/includes? name "@")
    (throw (ex-info (str "Virtual graph name cannot contain '@' character. Provided: " name)
                    {:error :db/invalid-config :name name}))))

#?(:clj
   (defn- enforce-vg-publish-policy
     "Enforce global VG publishing policy.
      Throws if virtualGraphAllowPublish=false.
      This blocks ALL VG types (Iceberg, BM25, R2RML, etc.)."
     [publisher]
     (when-let [iceberg-cfg (system/get-iceberg-config publisher)]
       (when-not (:allow-vg-publish? iceberg-cfg true)
         (throw (ex-info "Virtual graph publishing is disabled"
                         {:error :db/policy-violation
                          :policy :virtualGraphAllowPublish}))))))

#?(:clj
   (defn- normalize-catalog-name
     "Accept both 'catalog-name' (kebab) and 'catalogName' (camel).
      Normalize at the edge."
     [catalog-config]
     (or (:catalog-name catalog-config)
         (get catalog-config "catalog-name")
         (get catalog-config "catalogName"))))

#?(:clj
   (defn- enforce-iceberg-policy
     "Enforce Iceberg-specific governance policies.
      Throws if policy violated."
     [publisher config]
     (when-let [iceberg-cfg (system/get-iceberg-config publisher)]
       (let [{:keys [allow-dynamic-vgs? allow-dynamic-catalogs?
                     catalogs allowed-catalog-names]} iceberg-cfg
             catalog-config (or (:catalog config) (get config "catalog"))
             catalog-name (normalize-catalog-name catalog-config)]

         ;; Check if dynamic Iceberg VGs are allowed
         (when-not allow-dynamic-vgs?
           (throw (ex-info "Dynamic Iceberg virtual graph creation is disabled"
                           {:error :db/policy-violation
                            :policy :icebergAllowDynamicVirtualGraphs})))

         ;; If specifying a catalog, check if it's allowed
         (when catalog-config
           (cond
             ;; Named catalog - verify it exists in pre-configured catalogs
             catalog-name
             (do
               (when-not (get catalogs catalog-name)
                 (throw (ex-info (str "Unknown Iceberg catalog: " catalog-name
                                      ". Configured catalogs: " (vec (keys catalogs)))
                                 {:error :db/invalid-config
                                  :catalog-name catalog-name
                                  :available (vec (keys catalogs))})))
               ;; If allowlist exists, check catalog name is in it
               ;; NOTE: Only checked when catalog-name is present
               (when (and (seq allowed-catalog-names)
                          (not (contains? (set allowed-catalog-names) catalog-name)))
                 (throw (ex-info "Iceberg catalog not in allowed list"
                                 {:error :db/policy-violation
                                  :catalog-name catalog-name
                                  :allowed allowed-catalog-names}))))

             ;; Inline catalog (dynamic, no catalog-name) - check if allowed
             (not allow-dynamic-catalogs?)
             (throw (ex-info "Dynamic Iceberg catalog configuration is disabled. Use a pre-configured catalog name."
                             {:error :db/policy-violation
                              :policy :icebergAllowDynamicCatalogs}))))))))

(defn create
  "Main entry point for creating virtual graphs."
  [conn config]
  (go-try
    (validate-common-config config)
    ;; Global gate - applies to ALL VG types (JVM only where Iceberg is supported)
    #?(:clj (let [publisher (connection/primary-publisher conn)]
              (enforce-vg-publish-policy publisher)))
    (<? (create-vg conn config))))

(defn- validate-bm25-config
  "Validates BM25-specific configuration."
  [{:keys [config]}]
  (let [ledgers (get-in config [:ledgers] [])]
    (when (not= 1 (count ledgers))
      (throw (ex-info "BM25 virtual graphs currently support only a single ledger. Multi-ledger support is not yet implemented."
                      {:error :db/invalid-config
                       :type :bm25
                       :ledgers ledgers
                       :count (count ledgers)})))))

(defn- prepare-bm25-config
  "Prepares the BM25 configuration for publishing.
   VG names follow the same convention as ledgers - normalized with branch (default :main)."
  [{:keys [name config dependencies]}]
  (let [normalized-name (util.ledger/ensure-ledger-branch name)]
    {:vg-name normalized-name
     :vg-type "fidx:BM25"
     :config config
     :dependencies (or dependencies
                       (mapv util.ledger/ensure-ledger-branch (get-in config [:ledgers] [])))}))

(defn- load-and-validate-ledgers
  "Loads all ledgers and validates they exist. Returns loaded ledgers."
  [conn ledger-aliases]
  (go-try
    (loop [remaining ledger-aliases
           loaded {}]
      (if-let [ledger-alias (first remaining)]
        (if-let [ledger (<? (connection/load-ledger-alias conn ledger-alias))]
          (recur (rest remaining) (assoc loaded ledger-alias ledger))
          (throw (ex-info (str "Ledger does not exist: " ledger-alias)
                          {:error :db/invalid-config
                           :ledger ledger-alias})))
        loaded))))

(defn- initialize-bm25-for-ledgers
  "Initializes the BM25 virtual graph for all dependent ledgers.
   Returns the loaded virtual graph instance."
  [loaded-ledgers publisher vg-name dependencies conn]
  (go-try
    ;; Single ledger support only for now
    (let [[_alias ledger] (first loaded-ledgers)
          db (ledger/current-db ledger)
          vg (<? (vg-loader/load-virtual-graph-from-nameservice db publisher vg-name))
          ;; Start subscriptions to source ledgers
          vg-with-conn (assoc vg :conn conn)
          vg-with-subs (vg/start-subscriptions vg-with-conn publisher dependencies)]
      vg-with-subs)))

(defmethod create-vg :bm25
  [conn vg-config]
  (go-try
    (validate-bm25-config vg-config)

    (let [full-config (prepare-bm25-config vg-config)
          {:keys [vg-name dependencies]} full-config
          publisher (connection/primary-publisher conn)
          ledger-aliases (get-in vg-config [:config :ledgers] [])]

      ;; Check if virtual graph already exists
      (when (<? (nameservice/lookup publisher vg-name))
        (throw (ex-info (str "Virtual graph already exists: " vg-name)
                        {:error :db/invalid-config
                         :vg-name vg-name})))

      (let [loaded-ledgers (<? (load-and-validate-ledgers conn ledger-aliases))]
        (<? (nameservice/publish-vg publisher full-config))
        (<? (initialize-bm25-for-ledgers loaded-ledgers publisher vg-name dependencies conn))))))

;; R2RML implementation (minimal v1)
(defn- validate-r2rml-config
  [{:keys [config]}]
  (let [{:keys [mapping mappingInline rdb]} config
        {:keys [jdbcUrl driver]} rdb]
    (when (and (nil? mapping) (nil? mappingInline))
      (throw (ex-info "R2RML virtual graph requires :mapping (address) or :mappingInline (Turtle)."
                      {:error :db/invalid-config :type :r2rml})))
    (when (or (str/blank? jdbcUrl) (str/blank? driver))
      (throw (ex-info "R2RML virtual graph requires :rdb {:jdbcUrl ... :driver ...}."
                      {:error :db/invalid-config :type :r2rml})))))

(defn- prepare-r2rml-config
  [{:keys [name config dependencies]}]
  (let [normalized-name (util.ledger/ensure-ledger-branch name)]
    {:vg-name normalized-name
     :vg-type "fidx:R2RML"
     :config  config
     :dependencies (or dependencies [])}))

(defmethod create-vg :r2rml
  [conn vg-config]
  (go-try
    (validate-r2rml-config vg-config)
    (let [full-config (prepare-r2rml-config vg-config)
          {:keys [vg-name]} full-config
          publisher (connection/primary-publisher conn)]
      ;; Check if VG already exists
      (when (<? (nameservice/lookup publisher vg-name))
        (throw (ex-info (str "Virtual graph already exists: " vg-name)
                        {:error :db/invalid-config :vg-name vg-name})))
      ;; Publish the R2RML VG record. Initialization occurs lazily on first use.
      (<? (nameservice/publish-vg publisher full-config))
      ;; Return a minimal descriptor; callers will load via query paths
      {:id vg-name :alias vg-name :type ["fidx:R2RML"] :config (:config full-config)})))

;; Iceberg implementation (JVM only - requires Iceberg deps)
#?(:clj
   (defn- validate-iceberg-config
     [{:keys [config]}]
     (let [{:keys [mapping mappingInline warehouse-path warehousePath store catalog]} config
           wh-path (or warehouse-path warehousePath (get config "warehouse-path"))
           has-store (or store (get config "store"))
           catalog-map (or catalog (get config "catalog"))
           catalog-type (keyword (or (:type catalog-map) (get catalog-map "type")))]
       (when (and (nil? mapping) (nil? mappingInline) (nil? (get config "mappingInline")))
         (throw (ex-info "Iceberg virtual graph requires :mapping or :mappingInline"
                         {:error :db/invalid-config :type :iceberg})))
       ;; Either warehouse-path (HadoopTables) or store required.
       ;;
       ;; Note: REST catalog mode currently requires a Fluree store for reading table metadata/data files
       ;; via Fluree's FileIO abstraction (see fluree.db.tabular.iceberg.rest/create-rest-iceberg-source).
       (when (and (= catalog-type :rest) (nil? has-store))
         (throw (ex-info "Iceberg virtual graph REST :catalog requires :store config {:type :s3 :bucket \"...\" :endpoint \"...\"}"
                         {:error :db/invalid-config :type :iceberg})))
       (when (and (nil? wh-path) (nil? has-store) (not= catalog-type :rest))
         (throw (ex-info "Iceberg virtual graph requires :warehouse-path or :store"
                         {:error :db/invalid-config :type :iceberg}))))))

#?(:clj
   (defn- prepare-iceberg-config
     [{:keys [name config dependencies]}]
     (let [normalized-name (util.ledger/ensure-ledger-branch name)]
       {:vg-name normalized-name
        :vg-type "fidx:Iceberg"
        :config config
        :dependencies (or dependencies [])})))

#?(:clj
   (defmethod create-vg :iceberg
     [conn vg-config]
     (go-try
       (validate-iceberg-config vg-config)
       (let [full-config (prepare-iceberg-config vg-config)
             {:keys [vg-name]} full-config
             publisher (connection/primary-publisher conn)]
         ;; Iceberg-specific policy checks (global gate already passed in `create`)
         (enforce-iceberg-policy publisher (:config vg-config))
         ;; Check if VG already exists
         (when (<? (nameservice/lookup publisher vg-name))
           (throw (ex-info (str "Virtual graph already exists: " vg-name)
                           {:error :db/invalid-config :vg-name vg-name})))
         ;; Publish to nameservice so alias resolution works in queries
         (<? (nameservice/publish-vg publisher full-config))
         ;; Return a minimal descriptor; actual VG is loaded lazily on first query
         {:id vg-name :alias vg-name :type ["fidx:Iceberg"] :config (:config full-config)}))))

(defmethod create-vg :default
  [_conn {:keys [type]}]
  (go-try
    (throw (ex-info (str "Unknown virtual graph type: " type)
                    {:error :db/invalid-config
                     :type type}))))