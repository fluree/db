(ns fluree.db.virtual-graph.iceberg.ledger-info
  "ledger-info support for Iceberg+R2RML virtual graphs.

   Goal: return a ledger-info-like response for virtual graphs so applications
   can treat them similarly to native Fluree ledgers."
  (:require [clojure.string :as str]
            [fluree.db.tabular.protocol :as tabular]
            [fluree.db.virtual-graph.iceberg.r2rml :as r2rml])
  (:import [java.time Instant ZoneOffset]
           [java.time.format DateTimeFormatter]))

(set! *warn-on-reflection* true)

(def ^:private xsd-ns "http://www.w3.org/2001/XMLSchema#")

(def ^:private iceberg-type->xsd
  "Best-effort mapping from Iceberg schema types (our keywords) to XSD datatype IRIs."
  {:long      (str xsd-ns "long")
   :int       (str xsd-ns "integer")
   :double    (str xsd-ns "double")
   :float     (str xsd-ns "float")
   :boolean   (str xsd-ns "boolean")
   :string    (str xsd-ns "string")
   :timestamp (str xsd-ns "dateTime")
   :date      (str xsd-ns "date")})

(defn- epoch-ms->iso
  ^String [^long ms]
  (-> (Instant/ofEpochMilli ms)
      (.atOffset ZoneOffset/UTC)
      (.format DateTimeFormatter/ISO_INSTANT)))

(defn- mapping->columns
  "Collect columns referenced by a per-table mapping.
   Includes:
   - all rr:column object maps
   - join condition child columns (for RefObjectMap counts)
   - subject template columns (useful for subject-level stats)"
  [mapping]
  (let [pred-cols (for [[_pred {:keys [type value]}] (:predicates mapping)
                        :when (= :column type)]
                    value)
        ref-cols  (for [[_pred {:keys [type join-conditions]}] (:predicates mapping)
                        :when (= :ref type)
                        jc join-conditions]
                    (:child jc))
        subj-cols (r2rml/extract-template-cols (:subject-template mapping))]
    (->> (concat pred-cols ref-cols subj-cols)
         (remove nil?)
         distinct
         vec)))

(defn- schema-col->type
  "Lookup a column's type keyword in a schema map from ITabularSource.get-schema."
  [schema col]
  (some (fn [{:keys [name type]}]
          (when (= name col) type))
        (:columns schema)))

(defn- non-null-count
  "Derive non-null count from Iceberg column stats, when present."
  [col-stats]
  (when (map? col-stats)
    (let [vc (:value-count col-stats)
          nc (:null-count col-stats)]
      (when (and (integer? vc) (integer? nc))
        (max 0 (- vc nc))))))

(defn- ref-count
  "Best-effort count for a RefObjectMap: minimum non-null count across join key columns."
  [column-stats join-conditions]
  (when (seq join-conditions)
    (let [counts (keep (fn [{:keys [child]}]
                         (some-> (get column-stats child) non-null-count))
                       join-conditions)]
      (when (seq counts)
        (apply min counts)))))

(defn- build-table-schemas
  "Load Iceberg schemas for all mapped tables (used to infer datatypes)."
  [sources mappings time-travel]
  (into {}
        (for [[_k {:keys [table]}] mappings
              :let [source (get sources table)]
              :when source]
          [table (tabular/get-schema source table (merge time-travel {}))])))

(defn- build-table-stats
  "Load Iceberg snapshot statistics for all mapped tables."
  [sources mappings time-travel include-column-stats?]
  (into {}
        (for [[_k m] mappings
              :let [table (:table m)
                    source (get sources table)]
              :when source]
          (let [cols (mapping->columns m)
                stats (tabular/get-statistics
                       source table
                       (merge time-travel
                              {:columns cols
                               :include-column-stats? include-column-stats?}))]
            [table stats]))))

(defn- triples-map-index
  "Build a TriplesMap IRI -> mapping index for resolving RefObjectMap parentTriplesMap."
  [mappings]
  (into {}
        (for [[_k m] mappings
              :when (:triples-map-iri m)]
          [(:triples-map-iri m) m])))

(defn- mapping->class-stats
  "Build a single class stats entry from one per-table mapping."
  [mappings-by-tm table->schema table->stats mapping]
  (let [cls (:class mapping)
        table (:table mapping)
        row-count (get-in table->stats [table :row-count])
        column-stats (get-in table->stats [table :column-stats] {})
        schema (get table->schema table)
        preds (:predicates mapping)
        prop-map
        (into {}
              (for [[pred obj-map] preds
                    :let [{:keys [type value datatype language parent-triples-map join-conditions]} obj-map]]
                (cond
                  (= :column type)
                  (let [cnt (or (some-> (get column-stats value) non-null-count)
                                row-count
                                0)
                        dtype (or datatype
                                  (some-> (schema-col->type schema value) iceberg-type->xsd)
                                  (str xsd-ns "string"))]
                    [pred {:types {dtype cnt}
                           :ref-classes {}
                           :langs (if (and (string? language) (not (str/blank? language)))
                                    {language cnt}
                                    {})}])

                  (= :ref type)
                  (let [parent (get mappings-by-tm parent-triples-map)
                        parent-class (:class parent)
                        cnt (or (ref-count column-stats join-conditions)
                                row-count
                                0)]
                    [pred {:types {}
                           :ref-classes (if parent-class {parent-class cnt} {})
                           :langs {}}])

                  :else
                  [pred {:types {} :ref-classes {} :langs {}}])))]
    (when cls
      [cls {:count (or row-count 0)
            :properties prop-map}])))

(defn- build-classes
  [mappings table->schema table->stats]
  (let [by-tm (triples-map-index mappings)]
    (into {}
          (keep (fn [[_k m]]
                  (mapping->class-stats by-tm table->schema table->stats m))
                mappings))))

(defn- build-properties
  "Aggregate property stats across all class entries.

   Note: this is best-effort. We don’t have Fluree NDV/selectivity for Iceberg VGs."
  [classes snapshot-id]
  (reduce-kv
   (fn [acc _cls {:keys [count properties]}]
     (reduce-kv
      (fn [acc2 pred prop-data]
        (let [pred-entry (get acc2 pred {})
              inferred-count (or (some->> (:types prop-data) vals first)
                                 (some->> (:ref-classes prop-data) vals first)
                                 count
                                 0)
              next-count (+ (long (get pred-entry :count 0)) (long inferred-count))]
          (assoc acc2 pred (cond-> {:count next-count}
                             snapshot-id (assoc :last-modified-t snapshot-id)))))
      acc
      properties))
   {}
   classes))

(defn- estimate-flakes
  "Estimate total triples as rdf:type triples + predicate triples."
  [classes properties]
  (let [type-triples (reduce + 0 (map :count (vals classes)))
        pred-triples (reduce + 0 (map :count (vals properties)))]
    (+ type-triples pred-triples)))

(defn ledger-info
  "Return a ledger-info-like map for an Iceberg+R2RML virtual graph.

   This is intentionally best-effort and metadata-only:
   - Class/predicate structure comes from the R2RML mapping (authoritative)
   - Counts come from Iceberg snapshot + manifest stats (no full scan)

   Options:
     :include-column-stats? (default true) - if false, predicate counts fall back to row-count.

   Returns an API-ready map with IRI keys (no SID decoding required)."
  ([iceberg-db] (ledger-info iceberg-db {}))
  ([{:keys [alias sources mappings time-travel] :as _iceberg-db}
    {:keys [include-column-stats?]
     :or   {include-column-stats? true}}]
   (let [table->schema (build-table-schemas sources mappings time-travel)
         table->stats  (build-table-stats sources mappings time-travel include-column-stats?)
         any-stats     (some-> table->stats vals first)
         snapshot-id   (:snapshot-id any-stats)
         timestamp-ms  (:timestamp-ms any-stats)
         classes       (build-classes mappings table->schema table->stats)
         properties    (build-properties classes snapshot-id)
         flakes        (estimate-flakes classes properties)]
     {:commit {"@context" "https://ns.flur.ee/ledger/v1"
               "type" ["Commit"]
               "alias" alias
               "time" (when (integer? timestamp-ms) (epoch-ms->iso (long timestamp-ms)))
               "data" (cond-> {"type" ["DB"]}
                        snapshot-id (assoc "t" snapshot-id))}
      :nameservice nil
      :namespace-codes {}
      :stats {:flakes flakes
              :size 0
              :indexed 1
              :properties properties
              :classes classes}
      :virtual-graph {:type :iceberg
                      :alias alias
                      :snapshot-id snapshot-id
                      :timestamp-ms timestamp-ms
                      :tables (->> mappings vals (mapv :table) distinct vec)}})))

