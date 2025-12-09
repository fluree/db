(ns fluree.db.tabular.protocol
  "Protocol for tabular data sources (Iceberg, Parquet, etc.)

   This protocol provides a unified interface for querying columnar data
   with predicate pushdown, projection pushdown, and time-travel support.")

(defprotocol ITabularSource
  "Protocol for tabular data sources.

   Implementations should support:
   - Column projection (only read requested columns)
   - Predicate pushdown (filter at storage layer)
   - Time-travel via snapshots (for Iceberg)
   - Statistics for query planning"

  (scan-rows [this table-name opts]
    "Scan table returning lazy seq of row maps.

     Options:
       :columns     - seq of column names to project (nil = all)
       :predicates  - seq of predicate maps (see Predicate Format)
       :snapshot-id - specific snapshot ID for time travel
       :as-of-time  - java.time.Instant for time travel
       :limit       - max total rows to return

     Returns: lazy seq of row maps {\"column-name\" value ...}")

  (get-schema [this table-name opts]
    "Returns schema for a table.

     Options:
       :snapshot-id - specific snapshot ID
       :as-of-time  - timestamp for time travel

     Returns:
       {:columns [{:name string
                   :type keyword (:long :string :double :int :boolean :timestamp)
                   :nullable? boolean
                   :is-partition-key? boolean}...]
        :partition-spec {...}}")

  (get-statistics [this table-name opts]
    "Returns statistics for query planning.

     Options:
       :snapshot-id - specific snapshot ID
       :columns     - specific columns (nil = all)

     Returns:
       {:row-count long
        :file-count long
        :column-stats {col-name {:min :max :null-count :distinct-count}}
        :partition-stats [{:partition-values {...} :row-count}]}")

  (supported-predicates [this]
    "Returns set of supported predicate operations.

     Minimum: #{:eq :ne :gt :gte :lt :lte :in :between :is-null :not-null}
     May also: #{:like :starts-with :or :and}"))

;; Predicate Format Documentation
;;
;; Simple equality:
;;   {:column \"status\" :op :eq :value \"completed\"}
;;
;; Range (useful for date filtering):
;;   {:column \"sale_date\" :op :between :value [\"2024-01-01\" \"2024-12-31\"]}
;;
;; IN list (common in SPARQL VALUES clauses):
;;   {:column \"region\" :op :in :value [\"US\" \"EU\" \"APAC\"]}
;;
;; NULL checks:
;;   {:column \"deleted_at\" :op :is-null}
;;   {:column \"customer_id\" :op :not-null}
;;
;; Boolean combinations:
;;   {:op :and
;;    :predicates [{:column \"status\" :op :eq :value \"active\"}
;;                 {:column \"amount\" :op :gte :value 100}]}
;;
;;   {:op :or
;;    :predicates [{:column \"priority\" :op :eq :value \"high\"}
;;                 {:column \"escalated\" :op :eq :value true}]}
;;
;; Partition column hint (enables partition pruning):
;;   {:column \"year\" :op :eq :value 2024 :partition-key? true}

(defprotocol ICloseable
  "Lifecycle protocol for resource cleanup."
  (close [this] "Release resources held by this source."))
