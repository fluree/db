(ns fluree.db.nameservice.query
  (:require [fluree.db.connection :as connection]
            [fluree.db.connection.config :as config]
            [fluree.db.connection.system :as system]
            [fluree.db.ledger :as ledger]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.query.api :as query-api]
            [fluree.db.transact :as transact]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]))

(defn create-query-ledger
  "Creates an in-memory ledger from all nameservice records"
  [nameservice-records]
  (go-try
    (let [;; Create in-memory connection using lower-level functions
          memory-config {"@context" {"@base"  "https://ns.flur.ee/config/connection/"
                                     "@vocab" "https://ns.flur.ee/system#"}
                         "@id"      "memory"
                         "@graph"   [{"@id"   "memoryStorage"
                                      "@type" "Storage"}
                                     {"@id"              "connection"
                                      "@type"            "Connection"
                                      "parallelism"      4
                                      "cacheMaxMb"       1000
                                      "commitStorage"    {"@id" "memoryStorage"}
                                      "indexStorage"     {"@id" "memoryStorage"}
                                      "primaryPublisher" {"@type"   "Publisher"
                                                          "storage" {"@id" "memoryStorage"}}}]}
          system-map (system/initialize (config/parse memory-config))
          conn (reduce-kv (fn [x k v]
                            (if (isa? k :fluree.db/connection)
                              (reduced v)
                              x))
                          nil system-map)
          ;; Create a temporary ledger for nameservice metadata
          temp-ledger (<? (connection/create-ledger conn "nameservice-query" {}))
          ;; Get the initial database
          db (ledger/current-db temp-ledger)]

      ;; Insert all nameservice records as JSON-LD entities
      (if (seq nameservice-records)
        (let [;; Convert records to insert format
              insert-data {"@graph" nameservice-records}
              ;; Insert all records using transact API
              updated-db (<? (transact/stage db nil insert-data {}))]
          ;; Return the system map, connection and updated database
          {:system-map system-map
           :connection conn
           :ledger temp-ledger
           :db updated-db})
        ;; No records to insert, return empty ledger
        {:system-map system-map
         
         :connection conn
         :ledger temp-ledger
         :db db}))))

(defn query-nameservice
  "Execute a query against all nameservice records"
  [nameservice query opts]
  (go-try
    (log/debug "Querying nameservice with query:" query)

    ;; Get all nameservice records
    (let [records (<? (nameservice/all-records nameservice))
          ;; Create temporary in-memory ledger with records
          {:keys [system-map db]} (<? (create-query-ledger records))]
      (try
        ;; Execute the query using query-api
        (let [result (<? (query-api/query db query opts))]
          ;; Clean up system
          (system/terminate system-map)
          result)
        (catch #?(:clj Exception :cljs js/Error) e
          ;; Ensure cleanup even on error
          (system/terminate system-map)
          (throw e))))))