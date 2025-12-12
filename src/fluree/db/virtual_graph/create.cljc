(ns fluree.db.virtual-graph.create
  "Handles creation of virtual graphs, delegating to type-specific implementations."
  (:require [clojure.string :as str]
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

(defn create
  "Main entry point for creating virtual graphs."
  [conn config]
  (go-try
    (validate-common-config config)
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
       ;; Either warehouse-path (HadoopTables) or store (FlureeIcebergSource) or catalog (REST) required
       (when (and (nil? wh-path) (nil? has-store) (not= catalog-type :rest))
         (throw (ex-info "Iceberg virtual graph requires :warehouse-path, :store, or REST :catalog"
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