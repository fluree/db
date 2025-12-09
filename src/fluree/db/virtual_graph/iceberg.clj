(ns fluree.db.virtual-graph.iceberg
  "Iceberg implementation of virtual graph using ITabularSource.

   Supports R2RML mappings over Iceberg tables with predicate pushdown.

   Configuration:
     {:type :iceberg
      :name \"my-vg\"
      :config {:warehouse-path \"/path/to/warehouse\"
               :mapping \"path/to/mapping.ttl\"  ; or :mappingInline
               :table \"namespace/tablename\"}}   ; optional default table"
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
            [fluree.db.util.log :as log]
            [fluree.db.virtual-graph :as vg]))

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
;;; Pattern Analysis
;;; ---------------------------------------------------------------------------

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
  "Execute query against Iceberg source with predicate pushdown."
  [source mapping patterns base-solution]
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
                                       :predicates predicates})

        ;; Execute scan
        rows (tabular/scan-rows source table-name
                                {:columns (when (seq columns) columns)
                                 :predicates (when (seq predicates) predicates)})]

    ;; Transform to solutions
    (map #(row->solution % mapping pred->var subject-var base-solution) rows)))

;;; ---------------------------------------------------------------------------
;;; IcebergDatabase Record
;;; ---------------------------------------------------------------------------

(defrecord IcebergDatabase [alias config source mappings]
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
                 (let [mapping (analyze-clause-for-mapping patterns mappings)
                       solutions (execute-iceberg-query source mapping patterns solution)]
                   (doseq [sol solutions]
                     (async/>! ch sol))
                   (async/close! ch))
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

(defn create
  "Create an IcebergDatabase virtual graph.

   Config:
     :alias          - Virtual graph alias (required)
     :config         - Configuration map containing:
       :warehouse-path - Path to Iceberg warehouse (required)
       :mapping        - Path to R2RML mapping file
       :mappingInline  - Inline R2RML mapping (Turtle or JSON-LD)
       :table          - Default table name (optional)"
  [{:keys [alias config]}]
  (let [warehouse-path (or (:warehouse-path config)
                           (get config "warehouse-path")
                           (get config "warehousePath"))
        _ (when-not warehouse-path
            (throw (ex-info "Iceberg virtual graph requires :warehouse-path"
                            {:error :db/invalid-config :config config})))
        mapping-source (or (:mappingInline config)
                           (get config "mappingInline")
                           (:mapping config)
                           (get config "mapping"))
        _ (when-not mapping-source
            (throw (ex-info "Iceberg virtual graph requires :mapping or :mappingInline"
                            {:error :db/invalid-config :config config})))
        source (iceberg/create-iceberg-source {:warehouse-path warehouse-path})
        mappings (parse-r2rml mapping-source)]
    (log/info "Created Iceberg virtual graph:" alias
              "warehouse:" warehouse-path
              "mappings:" (count mappings))
    (map->IcebergDatabase {:alias alias
                           :config config
                           :source source
                           :mappings mappings})))
