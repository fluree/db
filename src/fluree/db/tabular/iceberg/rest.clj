(ns fluree.db.tabular.iceberg.rest
  "REST catalog-based Iceberg source using Fluree's storage abstraction.

   Uses HTTP calls to REST catalog for table discovery, but reads data files
   through Fluree's existing storage protocols - avoiding duplicate S3 config.

   Two modes are supported:
   1. With :store - uses Fluree's FileIO for data reads (recommended)
   2. Without :store - uses Iceberg's RESTCatalog with S3 config (legacy)"
  (:require [clojure.string :as str]
            [fluree.db.tabular.file-io :as file-io]
            [fluree.db.tabular.iceberg.core :as core]
            [fluree.db.tabular.protocol :as proto]
            [fluree.db.util.log :as log]
            [jsonista.core :as json])
  (:import [java.net URI]
           [java.net.http HttpClient HttpRequest HttpResponse$BodyHandlers]
           [java.time Duration]
           [org.apache.iceberg BaseTable StaticTableOperations Table]
           [org.apache.iceberg.io FileIO]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; HTTP Client for REST API
;;; ---------------------------------------------------------------------------

(def ^:private ^HttpClient http-client
  (-> (HttpClient/newBuilder)
      (.connectTimeout (Duration/ofSeconds 30))
      (.build)))

(defn- rest-request
  "Make an HTTP GET request to the REST catalog API."
  [uri path auth-token]
  (let [url (str uri path)
        builder (-> (HttpRequest/newBuilder)
                    (.uri (URI/create url))
                    (.timeout (Duration/ofSeconds 60))
                    (.header "Accept" "application/json"))
        builder (if auth-token
                  (.header builder "Authorization" (str "Bearer " auth-token))
                  builder)
        request (.build (.GET builder))
        response (.send http-client request (HttpResponse$BodyHandlers/ofString))]
    (when (= 200 (.statusCode response))
      (json/read-value (.body response) json/keyword-keys-object-mapper))))

;;; ---------------------------------------------------------------------------
;;; Table Loading with Fluree FileIO
;;; ---------------------------------------------------------------------------

(defn- load-table-from-metadata
  "Load an Iceberg Table from a metadata location using StaticTableOperations."
  ^Table [^FileIO file-io ^String metadata-location ^String table-name]
  (let [ops (StaticTableOperations. metadata-location file-io)]
    ;; BaseTable constructor takes (TableOperations, String name)
    (BaseTable. ops table-name)))

(defn- get-table-metadata-location
  "Get the metadata-location for a table from the REST catalog API.

   Uses canonical table identifier format (namespace.table).
   Multi-level namespaces are supported (e.g., db.schema.table)."
  [uri auth-token table-name]
  (if-let [{:keys [namespace-path table]} (core/table-id->rest-path table-name)]
    (let [path (str "/v1/namespaces/" namespace-path "/tables/" table)
          response (rest-request uri path auth-token)]
      (when response
        (:metadata-location response)))
    (throw (ex-info "Table name must include namespace prefix"
                    {:table-name table-name}))))

;;; ---------------------------------------------------------------------------
;;; REST Iceberg Source (Fluree FileIO mode)
;;; ---------------------------------------------------------------------------

(defrecord FlureeRestIcebergSource [^FileIO file-io uri auth-token metadata-cache]
  proto/ITabularSource

  (scan-batches [_ table-name {:keys [columns predicates snapshot-id as-of-time batch-size limit]
                               :or {batch-size 4096}}]
    (let [meta-loc (or (get @metadata-cache table-name)
                       (let [loc (get-table-metadata-location uri auth-token table-name)]
                         (when loc (swap! metadata-cache assoc table-name loc))
                         loc))
          _ (when-not meta-loc
              (throw (ex-info (str "Cannot resolve metadata for table: " table-name)
                              {:table table-name :uri uri})))
          ^Table table (load-table-from-metadata file-io meta-loc table-name)]
      (log/debug "FlureeRestIcebergSource scan-batches:" {:table table-name
                                                          :metadata meta-loc
                                                          :batch-size batch-size})
      (core/scan-with-arrow table {:columns columns
                                   :predicates predicates
                                   :snapshot-id snapshot-id
                                   :as-of-time as-of-time
                                   :batch-size batch-size
                                   :limit limit})))

  (scan-arrow-batches [_ table-name {:keys [columns predicates snapshot-id as-of-time batch-size copy-batches]
                                     :or {batch-size 4096 copy-batches true}}]
    (let [meta-loc (or (get @metadata-cache table-name)
                       (let [loc (get-table-metadata-location uri auth-token table-name)]
                         (when loc (swap! metadata-cache assoc table-name loc))
                         loc))
          _ (when-not meta-loc
              (throw (ex-info (str "Cannot resolve metadata for table: " table-name)
                              {:table table-name :uri uri})))
          ^Table table (load-table-from-metadata file-io meta-loc table-name)]
      (log/debug "FlureeRestIcebergSource scan-arrow-batches (filtered):" {:table table-name
                                                                           :metadata meta-loc
                                                                           :batch-size batch-size
                                                                           :predicates (count predicates)
                                                                           :copy-batches copy-batches})
      ;; Use filtered Arrow batches for correct row-level filtering
      ;; When copy-batches is true (default), data is copied to avoid buffer reuse issues
      ;; When false, batches share underlying buffers - use for streaming consumption
      (core/scan-filtered-arrow-batches table {:columns columns
                                               :predicates predicates
                                               :snapshot-id snapshot-id
                                               :as-of-time as-of-time
                                               :batch-size batch-size
                                               :copy-batches copy-batches})))

  (scan-rows [this table-name opts]
    (proto/scan-batches this table-name opts))

  (get-schema [_ table-name {:keys [snapshot-id as-of-time]}]
    (let [meta-loc (or (get @metadata-cache table-name)
                       (get-table-metadata-location uri auth-token table-name))
          _ (when-not meta-loc
              (throw (ex-info (str "Cannot resolve metadata for table: " table-name)
                              {:table table-name :uri uri})))
          ^Table table (load-table-from-metadata file-io meta-loc table-name)]
      (core/extract-schema table {:snapshot-id snapshot-id :as-of-time as-of-time})))

  (get-statistics [_ table-name {:keys [snapshot-id]}]
    (let [meta-loc (or (get @metadata-cache table-name)
                       (get-table-metadata-location uri auth-token table-name))
          _ (when-not meta-loc
              (throw (ex-info (str "Cannot resolve metadata for table: " table-name)
                              {:table table-name :uri uri})))
          ^Table table (load-table-from-metadata file-io meta-loc table-name)]
      (core/extract-statistics table {:snapshot-id snapshot-id})))

  (supported-predicates [_]
    core/supported-predicate-ops)

  proto/ICatalogDiscovery
  (list-namespaces [_]
    (let [response (rest-request uri "/v1/namespaces" auth-token)]
      (if response
        (->> (:namespaces response)
             (mapv #(str/join "." %)))
        (throw (ex-info "Failed to list namespaces from REST catalog"
                        {:error :db/catalog-error :uri uri})))))

  (list-tables [_ namespace-name]
    ;; URL-encode namespace for multi-level namespaces (e.g., "db.schema" -> "db%1Fschema")
    ;; Use a dummy table name with table-id->rest-path to get encoded namespace
    (let [{:keys [namespace-path]} (core/table-id->rest-path (str namespace-name ".dummy"))
          path (str "/v1/namespaces/" namespace-path "/tables")
          response (rest-request uri path auth-token)]
      (if response
        (->> (:identifiers response)
             (mapv (fn [{:keys [namespace] table-name :name}]
                     ;; Use join-table-id for consistent canonical formatting
                     (core/join-table-id namespace table-name))))
        (throw (ex-info (str "Failed to list tables in namespace: " namespace-name)
                        {:error :db/catalog-error :namespace namespace-name :uri uri})))))

  proto/ICloseable
  (close [_]
    (.close file-io)))

;;; ---------------------------------------------------------------------------
;;; Catalog Discovery
;;; ---------------------------------------------------------------------------

(defn discover-catalog
  "Discover all namespaces and tables in a REST catalog.

   Returns a map of namespace -> [table-info...] where table-info is:
   {:name \"namespace.table\"
    :schema {...}
    :statistics {...}}

   Options:
     :include-schema?     - include schema info (default true)
     :include-statistics? - include stats (default false, can be slow)

   Example:
     (discover-catalog source)
     ;; => {\"openflights\" [{:name \"openflights.airlines\"
     ;;                      :schema {:columns [...]}}
     ;;                     {:name \"openflights.airports\" ...}]}"
  ([source] (discover-catalog source {}))
  ([source {:keys [include-schema? include-statistics?]
            :or {include-schema? true include-statistics? false}}]
   (let [namespaces (proto/list-namespaces source)]
     (into {}
           (for [ns namespaces]
             [ns (vec
                  (for [table-name (proto/list-tables source ns)]
                    (cond-> {:name table-name}
                      include-schema?
                      (assoc :schema (proto/get-schema source table-name {}))

                      include-statistics?
                      (assoc :statistics (proto/get-statistics source table-name {})))))])))))

;;; ---------------------------------------------------------------------------
;;; Factory Function
;;; ---------------------------------------------------------------------------

(defn create-rest-iceberg-source
  "Create an Iceberg source that uses REST catalog for discovery and
   Fluree's storage protocols for data access.

   Config keys:
   - :uri        (required) REST catalog endpoint
   - :store      (required) Fluree storage store (S3Store, FileStore, etc.)
   - :auth-token (optional) bearer token for REST API auth

   Example:
     (create-rest-iceberg-source {:uri \"http://localhost:8181\"
                                  :store my-s3-store})

   This approach:
   - Uses REST API for catalog discovery (list namespaces, tables)
   - Uses Fluree's existing storage for all file reads
   - Eliminates duplicate S3/storage configuration"
  [{:keys [uri store auth-token]}]
  {:pre [(string? uri) (some? store)]}
  (log/info "Creating REST Iceberg source with Fluree storage:" {:uri uri})
  (let [file-io (file-io/create-fluree-file-io store)]
    (->FlureeRestIcebergSource file-io uri auth-token (atom {}))))

