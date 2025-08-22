(ns fluree.db.virtual-graph.r2rml.db
  (:require [clojure.core.async :as async :refer [go]]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [fluree.db.constants :as const]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.turtle.parse :as turtle]
            [fluree.db.util.log :as log]
            [fluree.db.virtual-graph :as vg]))

(set! *warn-on-reflection* true)

;; R2RML vocabulary IRIs
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
(def ^:const r2rml-datatype (str r2rml-ns "datatype"))

(defn- extract-template-cols
  [template]
  (when template
    (->> (re-seq #"\{([^}]+)\}" template)
         (map (fn [[_ c]] c))
         (vec))))

(defn- get-iri
  "Extract IRI from either a string or a ::where/iri map"
  [x]
  (if (string? x)
    x
    (::where/iri x)))

(defn- parse-r2rml-ttl
  "Parse R2RML TTL content using the turtle parser and extract mapping information
   from the expanded triples."
  [ttl-content]
  (let [triples (turtle/parse ttl-content)
        ;; Group triples by subject IRI
        by-subject (group-by #(get-iri (first %)) triples)]
    ;; Find all TriplesMap instances
    (->> by-subject
         (filter (fn [[_subject triples]]
                   (some (fn [[_s p o]]
                           (and (= const/iri-rdf-type (get-iri p))
                                (= r2rml-triples-map (get-iri o))))
                         triples)))
         (map (fn [[subject triples]]
                (let [props (into {} (map (fn [[_s p o]]
                                            [(get-iri p) o])
                                          triples))
                      ;; Get logical table
                      logical-table-node (get-iri (get props r2rml-logical-table))
                      table-name (when logical-table-node
                                   (let [lt-triples (get by-subject logical-table-node)]
                                     (some (fn [[_s p o]]
                                             (when (= r2rml-table-name (get-iri p))
                                               (::where/val o)))
                                           lt-triples)))
                      ;; Get subject map
                      subject-map-node (get-iri (get props r2rml-subject-map))
                      [template rdf-class] (when subject-map-node
                                             (let [sm-triples (get by-subject subject-map-node)
                                                   sm-props (into {} (map (fn [[_s p o]]
                                                                            [(get-iri p) o])
                                                                          sm-triples))]
                                               [(::where/val (get sm-props r2rml-template))
                                                (get-iri (get sm-props r2rml-class))]))
                      ;; Get predicate-object maps
                      pom-nodes (keep (fn [[_s p o]]
                                        (when (= r2rml-predicate-object-map (get-iri p))
                                          (get-iri o)))
                                      triples)
                      predicates (into {}
                                       (keep (fn [pom-node]
                                               (let [pom-triples (get by-subject pom-node)
                                                     pom-props (into {} (map (fn [[_s p o]]
                                                                               [(get-iri p) o])
                                                                             pom-triples))
                                                     pred-iri (get-iri (get pom-props r2rml-predicate))
                                                     obj-map-node (get-iri (get pom-props r2rml-object-map))
                                                     obj-props (when obj-map-node
                                                                 (let [om-triples (get by-subject obj-map-node)]
                                                                   (into {} (map (fn [[_s p o]]
                                                                                   [(get-iri p) o])
                                                                                 om-triples))))]
                                                 (when (and pred-iri obj-props)
                                                   [pred-iri {:column (::where/val (get obj-props r2rml-column))
                                                              :template (::where/val (get obj-props r2rml-template))
                                                              :datatype (get-iri (get obj-props r2rml-datatype))}])))
                                             pom-nodes))]
                  [subject {:table table-name
                            :subject-template template
                            :class rdf-class
                            :predicates predicates}])))
         (into {}))))

(defn- parse-min-r2rml
  [mapping-path]
  (let [content (slurp mapping-path)
        mappings (parse-r2rml-ttl content)]
    (log/debug "Parsed R2RML mappings:" mappings)
    ;; Return all mappings as is - the analyze-clause-for-mapping function will select the right one
    mappings))

(defn- jdbc-spec
  [rdb]
  (let [jdbc-url (or (:jdbcUrl rdb) (get rdb "jdbcUrl"))
        driver   (or (:driver rdb) (get rdb "driver"))
        user     (or (:user rdb) (get rdb "user"))
        password (or (:password rdb) (get rdb "password"))]
    (cond-> {:connection-uri jdbc-url}
      driver (assoc :classname driver)
      user (assoc :user user)
      password (assoc :password password))))

(defn- analyze-clause-for-mapping
  "Analyze the clause to determine which mapping(s) to use based on predicates or types."
  [clause mappings]
  (if (empty? mappings)
    nil
    (let [;; Check if this is a type query
          type-triple (first (filter (fn [triple-wrapper]
                                       (let [triple (if (= :class (first triple-wrapper))
                                                      (second triple-wrapper)
                                                      triple-wrapper)
                                             [_ p o] triple]
                                         (and (map? p)
                                              (= "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                                                 (get p ::where/iri))
                                              (or (string? o)
                                                  (and (map? o) (get o ::where/iri))))))
                                     clause))
          rdf-type (when type-triple
                     (let [triple (if (= :class (first type-triple))
                                    (second type-triple)
                                    type-triple)
                           o (nth triple 2)]
                       (if (string? o) o (get o ::where/iri))))
          ;; Extract predicates from the clause - the clause is a list of triples [s p o]
          ;; where predicate is a map with ::where/iri key
          predicate-maps (filter map? (map second clause))
          predicates (->> predicate-maps
                          (map ::where/iri) ; Extract the IRI using the correct namespaced key
                          (set))
          relevant-mappings (if rdf-type
                             ;; Find mapping by class
                              (->> mappings
                                   (filter (fn [[_ mapping]]
                                             (= (:class mapping) rdf-type)))
                                   (map second))
                             ;; Find mapping by predicates
                              (->> mappings
                                   (filter (fn [[_ mapping]]
                                             (some (fn [pred] (get-in mapping [:predicates pred])) predicates)))
                                   (map second)))]
      (if (seq relevant-mappings)
        (first relevant-mappings)
        (first (vals mappings))))))

(defn- extract-predicate-bindings
  "Extract predicate IRI to variable mappings from a query clause (excluding rdf:type)."
  [clause]
  (->> clause
       (map (fn [[_ p o]]
              (when (and (map? p) (map? o) (get o ::where/var))
                [(get p ::where/iri)
                 (get o ::where/var)])))
       (remove nil?)
       (into {})))

(defn- extract-predicate-bindings-full
  "Extract all predicate IRI to variable mappings including rdf:type handling."
  [clause]
  (->> clause
       (map (fn [item]
              (let [[_ p o] (if (= :class (first item))
                              (second item)
                              item)]
                (cond
                  ;; Handle rdf:type queries where o is a constant IRI
                  (and (map? p)
                       (= "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                          (get p ::where/iri))
                       (map? o)
                       (get o ::where/iri))
                  ;; Don't add to var-mappings, will be handled separately
                  nil
                  ;; Handle rdf:type queries where o is a variable
                  (and (map? p)
                       (= "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                          (get p ::where/iri))
                       (map? o)
                       (get o ::where/var))
                  [(get p ::where/iri)
                   (get o ::where/var)]
                  ;; Handle regular predicate-variable pairs
                  (and (map? p) (map? o) (get o ::where/var))
                  [(get p ::where/iri)
                   (get o ::where/var)]
                  :else nil))))
       (remove nil?)
       (into {})))

(defn- extract-literal-filters
  "Extract predicate IRI to literal value mappings for WHERE clause generation."
  [clause]
  (->> clause
       (map (fn [item]
              (let [[_ p o] (if (= :class (first item))
                              (second item)
                              item)]
                (when (and (map? p)
                           (get p ::where/iri)
                           (map? o)
                           (get o ::where/val))
                  [(get p ::where/iri)
                   (get o ::where/val)]))))
       (remove nil?)
       (into {})))

(defn- extract-filter-expressions
  "Extract filter expressions from clause patterns."
  [clause]
  (->> clause
       (filter #(and (vector? %) (= :filter (first %))))
       (map second)))

(defn- extract-subject-variable
  "Extract the subject variable from a query clause item."
  [item]
  (cond
    ;; Handle JSON-LD patterns
    (map? item)
    (let [id (get item "@id")]
      (when (and (string? id) (str/starts-with? id "?"))
        id))
    ;; Handle :class wrapper format [:class [s p o]]
    (and (vector? item) (= :class (first item)) (vector? (second item)))
    (let [triple (second item)
          subject (first triple)]
      (when (and (map? subject) (get subject ::where/var))
        (get subject ::where/var)))
    ;; Handle regular triple patterns [s p o]
    (vector? item)
    (let [subject (first item)]
      (when (and (map? subject) (get subject ::where/var))
        (get subject ::where/var)))))

(defn- extract-type-variable
  "Extract the type variable from a clause item (for rdf:type queries)."
  [item]
  (let [[_ p o] (if (= :class (first item))
                  (second item)
                  item)]
    (when (and (map? p)
               (= "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                  (get p ::where/iri))
               (map? o)
               (get o ::where/var))
      (get o ::where/var))))

(defn- get-column-value
  "Get column value from row, trying different case variations."
  [row col]
  (or
   ;; Try exact match first
   (get row (keyword col))
   ;; Try lowercase
   (get row (keyword (str/lower-case col)))
   ;; Try uppercase
   (get row (keyword (str/upper-case col)))
   ;; Try with underscores converted
   (get row (keyword (str/replace (str/lower-case col) "_" "-")))
   (get row (keyword (str/replace (str/upper-case col) "_" "-")))))

(defn- value->rdf-match
  "Convert a raw value to an RDF match object with appropriate datatype."
  [value var-sym]
  (if value
    (cond
      (instance? java.sql.Timestamp value)
      (where/match-value {} (.toString ^java.sql.Timestamp value) const/iri-xsd-dateTime)
      (instance? java.util.Date value)
      (where/match-value {} (.toString ^java.util.Date value) const/iri-xsd-dateTime)
      (decimal? value)
      (where/match-value {} value const/iri-xsd-decimal)
      (integer? value)
      (where/match-value {} value const/iri-xsd-integer)
      :else
      (where/match-value {} value const/iri-string))
    (where/unmatched-var var-sym)))

(defn- generate-column-alias
  "Generate SQL column alias from variable name or predicate IRI."
  [var-name pred]
  (or (when var-name
        (subs (name var-name) 1))
      (-> pred
          (str/split #"/")
          last
          (str/replace #"[#:-]" "_"))))

(defn- variable->sql-column
  "Convert a Fluree query variable to its SQL column name based on predicate mappings."
  [var-name pred->var predicates]
  (when var-name
    (let [var-str (if (str/starts-with? var-name "?")
                    (subs var-name 1)
                    var-name)
          ;; Find which predicate maps to this variable
          pred-iri (some (fn [[p v]]
                           (when (or (= v var-name)
                                     (= v (symbol var-name))
                                     (= (name v) var-str))
                             p))
                         pred->var)]
      (when-let [pred-mapping (get predicates pred-iri)]
        (:column pred-mapping)))))

(defn- build-select-columns
  "Build SELECT column list with aliases for the given predicates."
  [predicates pred->var clause-predicates]
  (str/join ", "
            (for [pred clause-predicates
                  :when (get predicates pred)
                  :let [{:keys [column]} (get predicates pred)
                        var-name (get pred->var pred)
                        sql-alias (generate-column-alias var-name pred)]
                  :when column]
              (str column " AS " sql-alias))))

(defn- filter-expr->sql
  "Convert a Fluree filter expression to SQL WHERE condition.
   Handles basic comparison operators and functions."
  [expr pred->var predicates]
  ;; This is a simplified version - in production you'd want a proper parser
  ;; For now, handle basic patterns like (> ?age 45) or (= ?name \"Alice\")
  (let [expr-str (if (string? expr) expr (str expr))
        ;; Replace Fluree variables (which use ?-prefix notation) with SQL column names
        replaced (reduce (fn [s [_pred-iri var-name]]
                           (if-let [column (variable->sql-column var-name pred->var predicates)]
                             (str/replace s
                                          (re-pattern (str "\\?" (name var-name)))
                                          column)
                             s))
                         expr-str
                         pred->var)]
    ;; Convert filter operators to SQL equivalents
    (-> replaced
        (str/replace "=" "=")
        (str/replace "!=" "<>")
        (str/replace ">" ">")
        (str/replace "<" "<")
        (str/replace ">=" ">=")
        (str/replace "<=" "<=")
        ;; Remove outer parentheses if present
        (str/replace #"^\((.*)\)$" "$1"))))

(defn- build-where-clause
  "Build WHERE clause from literal filter conditions and filter expressions."
  [predicates pred->literal filter-exprs pred->var]
  (let [literal-conditions (for [[pred-iri literal-val] pred->literal
                                 :when (get predicates pred-iri)
                                 :let [{:keys [column]} (get predicates pred-iri)]]
                             (if (string? literal-val)
                               (format "%s = '%s'" column literal-val)
                               (format "%s = %s" column literal-val)))
        filter-conditions (map #(filter-expr->sql % pred->var predicates)
                               filter-exprs)
        all-conditions (concat literal-conditions filter-conditions)]
    (when (seq all-conditions)
      (str " WHERE " (str/join " AND " all-conditions)))))

(defn- combine-select-columns
  "Combine selected columns with template columns for final SELECT clause."
  [select-cols template-cols id-col]
  (let [template-col-selects (when template-cols
                               (str/join ", " template-cols))]
    (cond
      (and (empty? select-cols) template-col-selects)
      template-col-selects

      (and (seq select-cols) template-col-selects)
      (str select-cols ", " template-col-selects)

      (empty? select-cols)
      (str id-col " AS id")

      :else
      (str/join ", " (conj (vec (str/split select-cols #", ")) (str id-col " AS id"))))))

(defn- sql-for-mapping
  [mapping clause]
  (if (nil? mapping)
    "SELECT 1 WHERE 1=0" ; Return no results if no mapping
    (let [table (:table mapping)
          predicates (:predicates mapping)
          template-cols (extract-template-cols (:subject-template mapping))
          id-col (or (first template-cols) "id")

          ;; Extract variable bindings, literal filters, and filter expressions
          pred->var (extract-predicate-bindings clause)
          pred->literal (extract-literal-filters clause)
          filter-exprs (extract-filter-expressions clause)

          ;; Build SELECT and WHERE clauses
          clause-predicates (set (keys pred->var))
          select-cols (build-select-columns predicates pred->var clause-predicates)
          all-selects (combine-select-columns select-cols template-cols id-col)
          where-clause (build-where-clause predicates pred->literal filter-exprs pred->var)

          ;; Generate final SQL
          final-sql (format "SELECT %s FROM %s%s"
                            all-selects
                            (str/upper-case table)
                            (or where-clause ""))]

      (when (or (seq pred->literal) (seq filter-exprs))
        (log/debug "Literal filters:" pred->literal)
        (log/debug "Filter expressions:" filter-exprs)
        (log/debug "Generated SQL:" final-sql))

      final-sql)))

(defn- process-template-subject
  "Generate subject IRI from template and row data."
  [template row]
  (when template
    (let [template-cols (extract-template-cols template)]
      (reduce (fn [tmpl col]
                (let [col-val (get-column-value row col)]
                  (if col-val
                    (str/replace tmpl (str "{" col "}") (str col-val))
                    tmpl)))
              template
              template-cols))))

(defn- build-subject-binding
  "Build subject variable binding for solution map."
  [subject-var subject-id row-id]
  (when subject-var
    (let [subj-symbol (if (symbol? subject-var) subject-var (symbol subject-var))
          subj-iri (or subject-id (str "http://example.com/id/" (or row-id "unknown")))]
      [[subj-symbol (where/match-iri {} subj-iri)]])))

(defn- build-type-binding
  "Build type variable binding for solution map."
  [type-var mapping-class]
  (when (and type-var mapping-class)
    (let [type-sym (if (symbol? type-var) type-var (symbol type-var))]
      [[type-sym (where/match-iri {} mapping-class)]])))

(defn- build-predicate-bindings
  "Build predicate variable bindings for solution map."
  [var-mappings row]
  (for [[pred-iri var-name] var-mappings
        :when (and var-name
                   (not= pred-iri "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))]
    (let [var-str (if (symbol? var-name) (name var-name) (str var-name))
          sql-alias (if (and var-str (str/starts-with? var-str "?"))
                      (subs var-str 1)
                      var-str)
          value (or (get row (keyword (str/lower-case sql-alias)))
                    (get row (keyword sql-alias)))
          var-sym (if (symbol? var-name) var-name (symbol var-name))]
      [var-sym (value->rdf-match value var-sym)])))

(defn- row->solution
  "Transform a database row into a solution map with all variable bindings."
  [row mapping var-mappings subject-var type-var base-solution]
  (let [id (or (:id row) (get row :ID) (get row "ID"))
        subject-id (process-template-subject (:subject-template mapping) row)
        subject-bindings (build-subject-binding subject-var subject-id id)
        type-bindings (build-type-binding type-var (:class mapping))
        predicate-bindings (build-predicate-bindings var-mappings row)]
    (into (or base-solution {})
          (concat subject-bindings type-bindings predicate-bindings))))

(defn- prepare-r2rml-query
  "Prepare R2RML query by parsing mapping and generating SQL."
  [config mapping-spec patterns]
  (let [rdb (or (:rdb config) (get config "rdb"))
        db-spec (jdbc-spec rdb)
        mapping-file (or (:mapping config)
                         (get config "mapping")
                         (:mapping mapping-spec)
                         (get mapping-spec "mapping"))
        mappings (parse-min-r2rml mapping-file)
        mapping (analyze-clause-for-mapping patterns mappings)
        sql (sql-for-mapping mapping patterns)]
    {:db-spec db-spec
     :sql sql
     :mapping mapping}))

(defn- extract-query-variables
  "Extract all variable information from patterns."
  [patterns]
  {:var-mappings (extract-predicate-bindings-full patterns)
   :subject-var (some extract-subject-variable patterns)
   :type-var (some extract-type-variable patterns)})

(defn- execute-r2rml-query
  "Execute SQL query and transform results to solution maps."
  [db-spec sql mapping variables base-solution]
  (let [{:keys [var-mappings subject-var type-var]} variables
        rows (jdbc/query db-spec [sql])]
    (map (fn [row]
           (row->solution row mapping var-mappings
                          subject-var type-var base-solution))
         rows)))

(defn- stream-r2rml-results
  "Stream R2RML query results to output channel.
   Returns immediately, processing happens in background."
  [config mapping-spec patterns base-solution error-ch output-ch]
  (async/thread
    (try
      (let [{:keys [db-spec sql mapping]} (prepare-r2rml-query config mapping-spec patterns)
            variables (extract-query-variables patterns)
            solutions (execute-r2rml-query db-spec sql mapping variables base-solution)]
        ;; Stream each solution to the output channel using blocking put
        (doseq [solution solutions]
          (async/>!! output-ch solution))
        (async/close! output-ch))
      (catch Exception e
        (log/error e "Error in R2RML processing")
        (async/>!! error-ch e)
        (async/close! output-ch)))))

(defrecord R2RMLDatabase [alias config mapping-spec datasource]
  vg/UpdatableVirtualGraph
  (upsert [this _source-db _new-flakes _remove-flakes]
    (go this))
  (initialize [this _source-db]
    (go this))

  where/Matcher
  (-match-id [_ _tracker _solution _s-mch _error-ch]
    ;; R2RML doesn't support direct subject ID matching
    where/nil-channel)

  (-match-triple [_this _tracker solution triple _error-ch]
    ;; Collect R2RML pattern information in the solution, like BM25 does
    ;; Each triple adds to the accumulated pattern context
    (go
      (let [r2rml-patterns (get solution ::r2rml-patterns [])
            updated-patterns (conj r2rml-patterns triple)]
        (assoc solution ::r2rml-patterns updated-patterns))))

  (-match-class [_this _tracker solution class-triple _error-ch]
    ;; Handle class patterns - the class-triple is actually the complete map pattern
    ;; when coming from a where clause with a map
    (go
      (let [r2rml-patterns (get solution ::r2rml-patterns [])
            updated-patterns (conj r2rml-patterns class-triple)]
        (assoc solution ::r2rml-patterns updated-patterns))))

  (-activate-alias [this _alias]
    (go this))
  (-aliases [_]
    [alias])

  (-finalize [_ _tracker error-ch solution-ch]
    ;; Execute accumulated R2RML patterns, similar to BM25's approach
    (let [out-ch (async/chan 1 (map #(dissoc % ::r2rml-patterns)))]
      (async/pipeline-async 2
                            out-ch
                            (fn [solution ch]
                              (go
                                (try
                                  (let [patterns (get solution ::r2rml-patterns)]
                                    (if (seq patterns)
                                      ;; Stream R2RML results using refactored functions
                                      (stream-r2rml-results config mapping-spec patterns
                                                            solution error-ch ch)
                                      ;; No R2RML patterns, just pass through
                                      (do (async/>! ch solution)
                                          (async/close! ch))))
                                  (catch Exception e
                                    (async/>! error-ch e)
                                    (async/close! ch)))))
                            solution-ch)
      out-ch)))

(defn create
  "Create and initialize an R2RML virtual database with the provided configuration."
  [{:keys [alias config] :as vg-opts}]
  (let [cfg (or config
                (select-keys vg-opts [:mapping :mappingInline :rdb :baseIRI "mapping" "mappingInline" "rdb" "baseIRI"]))]
    (map->R2RMLDatabase {:alias alias
                         :config cfg
                         :mapping-spec (select-keys cfg [:mapping :mappingInline :baseIRI
                                                         "mapping" "mappingInline" "baseIRI"])

                         :datasource nil})))
