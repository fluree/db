(ns fluree.db.virtual-graph.iceberg
  "Iceberg implementation of virtual graph using ITabularSource.

   Supports R2RML mappings over Iceberg tables with predicate pushdown.

   Naming Convention:
     Iceberg virtual graphs use the same naming as ledgers:
       <name>:<branch>@iso:<time-travel-iso-8601>
       <name>:<branch>@t:<snapshot-id>

     Examples:
       \"sales-vg\"              - defaults to :main branch, latest snapshot
       \"sales-vg:main\"         - explicit main branch
       \"sales-vg@iso:2024-01-15T00:00:00Z\"  - time travel to specific time
       \"sales-vg@t:12345\"      - specific snapshot ID

   Configuration:
     {:type :iceberg
      :name \"my-vg\"
      :config {:warehouse-path \"/path/to/warehouse\"    ; for HadoopTables
               :store my-fluree-store                    ; for FlureeIcebergSource
               :metadata-location \"s3://...\"            ; direct metadata location
               :mapping \"path/to/mapping.ttl\"
               :table \"namespace/tablename\"}}"
  (:require [clojure.core.async :as async :refer [go]]
            [clojure.string :as str]
            [fluree.db.constants :as const]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.fql.parse :as fql-parse]
            [fluree.db.query.optimize :as optimize]
            [fluree.db.query.turtle.parse :as turtle]
            [fluree.db.tabular.iceberg :as iceberg]
            [fluree.db.tabular.protocol :as tabular]
            [fluree.db.util.async :refer [empty-channel]]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.util.log :as log]
            [fluree.db.virtual-graph :as vg])
  (:import [java.time Instant]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; R2RML Vocabulary (shared with r2rml.db)
;;; ---------------------------------------------------------------------------

(def ^:const r2rml-ns "http://www.w3.org/ns/r2rml#")
(def ^:const r2rml-triples-map (str r2rml-ns "TriplesMap"))
(def ^:const r2rml-logical-table (str r2rml-ns "logicalTable"))
(def ^:const r2rml-table-name (str r2rml-ns "tableName"))
(def ^:const r2rml-subject-map (str r2rml-ns "subjectMap"))
(def ^:const r2rml-template (str r2rml-ns "template"))
(def ^:const r2rml-class (str r2rml-ns "class"))
(def ^:const r2rml-predicate-object-map (str r2rml-ns "predicateObjectMap"))
(def ^:const r2rml-predicate (str r2rml-ns "predicate"))
(def ^:const r2rml-object-map (str r2rml-ns "objectMap"))
(def ^:const r2rml-column (str r2rml-ns "column"))

;;; ---------------------------------------------------------------------------
;;; R2RML Parsing (reused from r2rml.db)
;;; ---------------------------------------------------------------------------

(defn- extract-template-cols
  [template]
  (when template
    (->> (re-seq #"\{([^}]+)\}" template)
         (map second)
         vec)))

(defn- get-iri
  [x]
  (if (string? x) x (::where/iri x)))

(defn- parse-r2rml-from-triples
  [by-subject]
  (->> by-subject
       (filter (fn [[_subject triples]]
                 (some (fn [[_s p o]]
                         (and (= const/iri-rdf-type (get-iri p))
                              (= r2rml-triples-map (get-iri o))))
                       triples)))
       (map (fn [[_subject triples]]
              (let [props (into {} (map (fn [[_s p o]] [(get-iri p) o]) triples))
                    logical-table-node (get-iri (get props r2rml-logical-table))
                    logical-table (when logical-table-node
                                    (let [lt-triples (get by-subject logical-table-node)
                                          table-name (some (fn [[_s p o]]
                                                             (when (= r2rml-table-name (get-iri p))
                                                               (::where/val o)))
                                                           lt-triples)]
                                      (when table-name
                                        {:type :table-name :name table-name})))
                    subject-map-node (get-iri (get props r2rml-subject-map))
                    [template rdf-class] (when subject-map-node
                                           (let [sm-triples (get by-subject subject-map-node)
                                                 template (some (fn [[_s p o]]
                                                                  (when (= r2rml-template (get-iri p))
                                                                    (::where/val o)))
                                                                sm-triples)
                                                 rdf-class (some (fn [[_s p o]]
                                                                   (when (= r2rml-class (get-iri p))
                                                                     (get-iri o)))
                                                                 sm-triples)]
                                             [template rdf-class]))
                    pom-nodes (keep (fn [[_s p o]]
                                      (when (= r2rml-predicate-object-map (get-iri p))
                                        (get-iri o)))
                                    triples)
                    predicates (reduce (fn [acc pom-node]
                                         (let [pom-id (get-iri pom-node)
                                               pom-triples (get by-subject pom-id)
                                               pred (some (fn [[_s p o]]
                                                            (when (= r2rml-predicate (get-iri p))
                                                              (or (get-iri o) (::where/val o))))
                                                          pom-triples)
                                               obj-map-node (some (fn [[_s p o]]
                                                                    (when (= r2rml-object-map (get-iri p))
                                                                      (get-iri o)))
                                                                  pom-triples)
                                               object-map (when obj-map-node
                                                            (let [om-triples (get by-subject obj-map-node)
                                                                  column (some (fn [[_s p o]]
                                                                                 (when (= r2rml-column (get-iri p))
                                                                                   (::where/val o)))
                                                                               om-triples)
                                                                  datatype (some (fn [[_s p o]]
                                                                                   (when (= "http://www.w3.org/ns/r2rml#datatype" (get-iri p))
                                                                                     (get-iri o)))
                                                                                 om-triples)]
                                                              (when column
                                                                {:type :column :value column :datatype datatype})))]
                                           (if (and pred object-map)
                                             (assoc acc pred object-map)
                                             acc)))
                                       {}
                                       pom-nodes)]
                (when logical-table
                  (let [table-key (keyword (str/replace (:name logical-table) "\"" ""))]
                    [table-key
                     {:logical-table logical-table
                      :table (:name logical-table)
                      :subject-template template
                      :class rdf-class
                      :predicates predicates}])))))
       (filter some?)
       (into {})))

(defn- parse-r2rml
  [mapping-source]
  (let [content (cond
                  (and (string? mapping-source)
                       (.exists (java.io.File. ^String mapping-source)))
                  (slurp mapping-source)
                  :else mapping-source)
        turtle? (and (string? content)
                     (not (or (str/starts-with? (str/trim content) "{")
                              (str/starts-with? (str/trim content) "["))))
        triples (if turtle?
                  (turtle/parse content)
                  (fql-parse/jld->parsed-triples content nil
                                                 {"@vocab" "http://www.w3.org/ns/r2rml#"
                                                  "rr" "http://www.w3.org/ns/r2rml#"
                                                  "rdf" "http://www.w3.org/1999/02/22-rdf-syntax-ns#"}))
        by-subject (group-by #(get-iri (first %)) triples)]
    (parse-r2rml-from-triples by-subject)))

;;; ---------------------------------------------------------------------------
;;; Pattern Analysis & Multi-Table Routing
;;; ---------------------------------------------------------------------------

(defn- build-routing-indexes
  "Build indexes for routing patterns to the correct table.

   Returns:
     {:class->mapping {rdf-class -> mapping}
      :predicate->mapping {predicate-iri -> mapping}}"
  [mappings]
  (let [class->mapping (->> mappings
                            vals
                            (filter :class)
                            (map (fn [m] [(:class m) m]))
                            (into {}))
        predicate->mapping (->> mappings
                                vals
                                (mapcat (fn [m]
                                          (for [pred (keys (:predicates m))]
                                            [pred m])))
                                (into {}))]
    {:class->mapping class->mapping
     :predicate->mapping predicate->mapping}))

(defn- extract-pattern-info
  "Extract type and predicates from a pattern item."
  [item]
  (let [triple (if (= :class (first item)) (second item) item)
        [s p o] triple
        subject-var (when (and (map? s) (get s ::where/var))
                      (get s ::where/var))
        pred-iri (when (map? p) (get p ::where/iri))
        is-type? (= const/iri-rdf-type pred-iri)
        rdf-type (when (and is-type? (or (string? o) (map? o)))
                   (if (string? o) o (get o ::where/iri)))]
    {:subject-var subject-var
     :predicate pred-iri
     :is-type? is-type?
     :rdf-type rdf-type
     :item item}))

(defn- group-patterns-by-table
  "Group patterns by which table they should be routed to.

   Uses the routing indexes to determine which table handles each pattern.
   Patterns are grouped by subject variable to keep related patterns together.

   Returns: [{:mapping mapping :patterns [...]} ...]"
  [patterns mappings routing-indexes]
  (let [{:keys [class->mapping predicate->mapping]} routing-indexes
        pattern-infos (map extract-pattern-info patterns)

        ;; Find mapping for each pattern
        find-mapping (fn [{:keys [rdf-type predicate]}]
                       (or (when rdf-type (get class->mapping rdf-type))
                           (when predicate (get predicate->mapping predicate))
                           (first (vals mappings))))

        ;; Group by subject variable first, then by mapping
        by-subject (group-by :subject-var pattern-infos)

        ;; For each subject group, determine the primary mapping
        groups (for [[_subj-var infos] by-subject
                     :let [;; Find mappings for patterns with type info first
                           type-patterns (filter :rdf-type infos)
                           mapping (if (seq type-patterns)
                                     (find-mapping (first type-patterns))
                                     (find-mapping (first infos)))]]
                 {:mapping mapping
                  :patterns (mapv :item infos)})]
    (vec groups)))

(defn- analyze-clause-for-mapping
  "Find the mapping that matches the query patterns."
  [clause mappings]
  (when (seq mappings)
    (let [type-triple (first (filter (fn [item]
                                       (let [triple (if (= :class (first item))
                                                      (second item)
                                                      item)
                                             [_ p o] triple]
                                         (and (map? p)
                                              (= const/iri-rdf-type (get p ::where/iri))
                                              (or (string? o)
                                                  (and (map? o) (get o ::where/iri))))))
                                     clause))
          rdf-type (when type-triple
                     (let [triple (if (= :class (first type-triple))
                                    (second type-triple)
                                    type-triple)
                           o (nth triple 2)]
                       (if (string? o) o (get o ::where/iri))))
          predicates (->> clause
                          (map second)
                          (filter map?)
                          (map ::where/iri)
                          set)
          relevant (if rdf-type
                     (->> mappings
                          (filter (fn [[_ m]] (= (:class m) rdf-type)))
                          (map second))
                     (->> mappings
                          (filter (fn [[_ m]]
                                    (some #(get-in m [:predicates %]) predicates)))
                          (map second)))]
      (or (first relevant) (first (vals mappings))))))

(defn- extract-predicate-bindings
  "Extract predicate IRI -> variable name mappings."
  [clause]
  (->> clause
       (map (fn [item]
              (let [[_ p o] (if (= :class (first item)) (second item) item)]
                (when (and (map? p) (map? o) (get o ::where/var))
                  [(get p ::where/iri) (get o ::where/var)]))))
       (remove nil?)
       (into {})))

(defn- extract-literal-filters
  "Extract predicate IRI -> literal value for WHERE conditions."
  [clause]
  (->> clause
       (map (fn [item]
              (let [[_ p o] (if (= :class (first item)) (second item) item)]
                (when (and (map? p) (get p ::where/iri)
                           (map? o) (get o ::where/val))
                  [(get p ::where/iri) (get o ::where/val)]))))
       (remove nil?)
       (into {})))

(defn- extract-subject-variable
  [item]
  (cond
    (and (vector? item) (= :class (first item)) (vector? (second item)))
    (let [[subject] (second item)]
      (when (and (map? subject) (get subject ::where/var))
        (get subject ::where/var)))
    (vector? item)
    (let [[subject] item]
      (when (and (map? subject) (get subject ::where/var))
        (get subject ::where/var)))))

;;; ---------------------------------------------------------------------------
;;; Predicate Pushdown Translation
;;; ---------------------------------------------------------------------------

(defn- literal-filters->predicates
  "Convert literal filters to ITabularSource predicates."
  [pred->literal mapping]
  (for [[pred-iri literal-val] pred->literal
        :let [object-map (get-in mapping [:predicates pred-iri])
              column (when (and (map? object-map) (= :column (:type object-map)))
                       (:value object-map))]
        :when column]
    {:column column :op :eq :value literal-val}))

;;; ---------------------------------------------------------------------------
;;; Result Transformation
;;; ---------------------------------------------------------------------------

(defn- process-template-subject
  [template row]
  (when template
    (reduce (fn [tmpl col]
              (let [col-val (or (get row col)
                                (get row (str/lower-case col))
                                (get row (str/upper-case col)))]
                (if col-val
                  (str/replace tmpl (str "{" col "}") (str col-val))
                  tmpl)))
            template
            (extract-template-cols template))))

(defn- value->rdf-match
  [value var-sym]
  (cond
    (nil? value)
    (where/unmatched-var var-sym)

    (integer? value)
    (where/match-value {} value const/iri-xsd-integer)

    (float? value)
    (where/match-value {} value const/iri-xsd-double)

    (instance? Double value)
    (where/match-value {} value const/iri-xsd-double)

    :else
    (where/match-value {} value const/iri-string)))

(defn- row->solution
  "Transform an Iceberg row to a SPARQL solution map."
  [row mapping var-mappings subject-var base-solution]
  (let [subject-id (process-template-subject (:subject-template mapping) row)
        subject-binding (when subject-var
                          (let [subj-sym (if (symbol? subject-var) subject-var (symbol subject-var))]
                            [[subj-sym (where/match-iri {} (or subject-id "urn:unknown"))]]))
        pred-bindings (for [[pred-iri var-name] var-mappings
                            :when (and var-name
                                       (not= pred-iri const/iri-rdf-type))
                            :let [object-map (get-in mapping [:predicates pred-iri])
                                  column (when (and (map? object-map) (= :column (:type object-map)))
                                           (:value object-map))
                                  value (when column
                                          (or (get row column)
                                              (get row (str/lower-case column))))
                                  var-sym (if (symbol? var-name) var-name (symbol var-name))]
                            :when value]
                        [var-sym (value->rdf-match value var-sym)])]
    (into (or base-solution {})
          (concat subject-binding pred-bindings))))

;;; ---------------------------------------------------------------------------
;;; Query Execution
;;; ---------------------------------------------------------------------------

(defn- execute-iceberg-query
  "Execute query against Iceberg source with predicate pushdown.

   time-travel can be:
   - nil (latest snapshot)
   - {:snapshot-id Long} (specific Iceberg snapshot)
   - {:as-of-time Instant} (time-travel to specific time)"
  [source mapping patterns base-solution time-travel]
  (let [table-name (:table mapping)
        pred->var (extract-predicate-bindings patterns)
        pred->literal (extract-literal-filters patterns)
        subject-var (some extract-subject-variable patterns)

        ;; Build columns to select
        columns (->> pred->var
                     keys
                     (keep (fn [pred-iri]
                             (let [om (get-in mapping [:predicates pred-iri])]
                               (when (= :column (:type om))
                                 (:value om)))))
                     (concat (extract-template-cols (:subject-template mapping)))
                     distinct
                     vec)

        ;; Build predicates for pushdown
        predicates (vec (literal-filters->predicates pred->literal mapping))

        _ (log/debug "Iceberg query:" {:table table-name
                                       :columns columns
                                       :predicates predicates
                                       :time-travel time-travel})

        ;; Execute scan with time-travel options
        rows (tabular/scan-rows source table-name
                                (cond-> {:columns (when (seq columns) columns)
                                         :predicates (when (seq predicates) predicates)}
                                  (:snapshot-id time-travel)
                                  (assoc :snapshot-id (:snapshot-id time-travel))

                                  (:as-of-time time-travel)
                                  (assoc :as-of-time (:as-of-time time-travel))))]

    ;; Transform to solutions
    (map #(row->solution % mapping pred->var subject-var base-solution) rows)))

;;; ---------------------------------------------------------------------------
;;; IcebergDatabase Record (Multi-Table Support)
;;; ---------------------------------------------------------------------------

(defrecord IcebergDatabase [alias config sources mappings routing-indexes time-travel]
  ;; sources: {table-name -> IcebergSource}
  ;; mappings: {table-key -> {:table, :class, :predicates, ...}}
  ;; routing-indexes: {:class->mapping {...} :predicate->mapping {...}}

  vg/UpdatableVirtualGraph
  (upsert [this _source-db _new-flakes _remove-flakes]
    (go this))
  (initialize [this _source-db]
    (go this))

  where/Matcher
  (-match-id [_ _tracker _solution _s-mch _error-ch]
    empty-channel)

  (-match-triple [_this _tracker solution triple _error-ch]
    (go
      (let [iceberg-patterns (get solution ::iceberg-patterns [])
            updated (conj iceberg-patterns triple)]
        (assoc solution ::iceberg-patterns updated))))

  (-match-class [_this _tracker solution class-triple _error-ch]
    (go
      (let [iceberg-patterns (get solution ::iceberg-patterns [])
            updated (conj iceberg-patterns class-triple)]
        (assoc solution ::iceberg-patterns updated))))

  (-activate-alias [this _alias]
    (go this))

  (-aliases [_]
    [alias])

  (-finalize [_ _tracker error-ch solution-ch]
    (let [out-ch (async/chan 1 (map #(dissoc % ::iceberg-patterns)))]
      (async/pipeline-async
       2
       out-ch
       (fn [solution ch]
         (go
           (try
             (let [patterns (get solution ::iceberg-patterns)]
               (if (seq patterns)
                 ;; Group patterns by table and execute each group
                 (let [pattern-groups (group-patterns-by-table patterns mappings routing-indexes)]
                   (if (= 1 (count pattern-groups))
                     ;; Single table - simple case
                     (let [{:keys [mapping patterns]} (first pattern-groups)
                           table-name (:table mapping)
                           source (get sources table-name)]
                       (when-not source
                         (throw (ex-info (str "No source found for table: " table-name)
                                         {:error :db/missing-source
                                          :table table-name
                                          :available-sources (keys sources)})))
                       (let [solutions (execute-iceberg-query source mapping patterns solution time-travel)]
                         (doseq [sol solutions]
                           (async/>! ch sol))
                         (async/close! ch)))
                     ;; Multiple tables - nested loop join
                     (let [execute-group (fn [base-solution {:keys [mapping patterns]}]
                                           (let [table-name (:table mapping)
                                                 source (get sources table-name)]
                                             (when-not source
                                               (throw (ex-info (str "No source found for table: " table-name)
                                                               {:error :db/missing-source
                                                                :table table-name
                                                                :available-sources (keys sources)})))
                                             (execute-iceberg-query source mapping patterns base-solution time-travel)))
                           ;; Execute first group to get initial solutions
                           first-group (first pattern-groups)
                           initial-solutions (execute-group solution first-group)]
                       ;; Short-circuit if first group returns empty
                       (if (empty? initial-solutions)
                         (async/close! ch)
                         ;; For each subsequent group, join with existing solutions
                         (let [final-solutions (reduce
                                                (fn [solutions group]
                                                  (if (empty? solutions)
                                                    (reduced []) ;; Short-circuit on empty
                                                    (mapcat #(execute-group % group) solutions)))
                                                initial-solutions
                                                (rest pattern-groups))]
                           (doseq [sol final-solutions]
                             (async/>! ch sol))
                           (async/close! ch))))))
                 (do (async/>! ch solution)
                     (async/close! ch))))
             (catch Exception e
               (log/error e "Error in Iceberg query execution")
               (async/>! error-ch e)
               (async/close! ch)))))
       solution-ch)
      out-ch))

  optimize/Optimizable
  (-reorder [_ parsed-query]
    (go parsed-query))
  (-explain [_ parsed-query]
    (go {:original parsed-query
         :optimized parsed-query
         :segments []
         :changed? false})))

;;; ---------------------------------------------------------------------------
;;; Factory
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

(defn create
  "Create an IcebergDatabase virtual graph with multi-table support.

   Registration-time alias format:
     <name>           - defaults to :main branch
     <name>:<branch>  - explicit branch

   Time-travel is a QUERY-TIME concern, not registration-time.
   At query time, use FROM <alias@t:snapshot-id> or FROM <alias@iso:timestamp>
   to specify which snapshot to query.

   Multi-Table Support:
     The R2RML mapping can define multiple TriplesMap entries, each mapping
     a different table to a different RDF class. This VG will automatically:
     - Create an IcebergSource for each unique table in the mappings
     - Route query patterns to the appropriate table based on class/predicate
     - Execute cross-table joins using nested loop join strategy

   Examples:
     Registration: 'openflights-vg' (with R2RML mapping airlines, airports, routes)
     Query: SELECT ?airline ?airport WHERE { ?airline a :Airline . ?airport a :Airport }

   Config:
     :alias          - Virtual graph alias with optional branch (required)
     :config         - Configuration map containing:
       :warehouse-path  - Path to Iceberg warehouse (for HadoopTables)
       :store           - Fluree storage store (for FlureeIcebergSource)
       :metadata-location - Direct path to metadata JSON (optional)
       :mapping         - Path to R2RML mapping file
       :mappingInline   - Inline R2RML mapping (Turtle or JSON-LD)

   Either :warehouse-path or :store must be provided."
  [{:keys [alias config]}]
  (let [;; Reject @ in alias - reserved character
        _ (when (str/includes? alias "@")
            (throw (ex-info (str "Virtual graph name cannot contain '@' character. Provided: " alias)
                            {:error :db/invalid-config :alias alias})))

        ;; Parse alias for name and branch only
        {:keys [ledger branch]} (util.ledger/parse-ledger-alias alias)
        base-alias (if branch (str ledger ":" branch) ledger)

        ;; Get warehouse/store config
        warehouse-path (or (:warehouse-path config)
                           (get config "warehouse-path")
                           (get config "warehousePath"))
        store (or (:store config) (get config "store"))
        metadata-location (or (:metadata-location config)
                              (get config "metadata-location")
                              (get config "metadataLocation"))

        _ (when-not (or warehouse-path store)
            (throw (ex-info "Iceberg virtual graph requires :warehouse-path or :store"
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
        mappings (parse-r2rml mapping-source)

        ;; Extract unique table names from all mappings
        table-names (->> mappings
                         vals
                         (map :table)
                         (remove nil?)
                         distinct)

        ;; Create source factory function
        create-source-fn (if store
                           #(iceberg/create-fluree-iceberg-source
                             {:store store
                              :warehouse-path (or warehouse-path "")})
                           #(iceberg/create-iceberg-source
                             {:warehouse-path warehouse-path}))

        ;; Create an IcebergSource for each unique table
        ;; Note: Currently we use the same source for all tables in the same warehouse
        ;; In the future, we could optimize by sharing the source instance
        sources (into {}
                      (for [table-name table-names]
                        [table-name (create-source-fn)]))

        ;; Build routing indexes for efficient pattern-to-table mapping
        routing-indexes (build-routing-indexes mappings)]

    (log/info "Created Iceberg virtual graph:" base-alias
              (if store "store-backed" (str "warehouse:" warehouse-path))
              "tables:" (vec table-names)
              "mappings:" (count mappings))

    (map->IcebergDatabase {:alias base-alias
                           :config (cond-> config
                                     metadata-location
                                     (assoc :metadata-location metadata-location))
                           :sources sources
                           :mappings mappings
                           :routing-indexes routing-indexes
                           :time-travel nil})))
