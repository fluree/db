(ns fluree.db.virtual-graph.r2rml.db
  (:require [clojure.core.async :as async :refer [go]]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [fluree.db.constants :as const]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util.log :as log]
            [fluree.db.virtual-graph :as vg]))

(set! *warn-on-reflection* true)

(defn- extract-template-cols
  [template]
  (when template
    (->> (re-seq #"\{([^}]+)\}" template)
         (map (fn [[_ c]] c))
         (vec))))

(defn- parse-prefixes
  [content]
  (->> (re-seq #"@prefix\s+([a-zA-Z][\w\-]*)\:\s*<([^>]+)>\s*\." content)
       (reduce (fn [acc [_ p iri]] (assoc acc (str p) iri)) {})))

(defn- expand-qname
  [prefixes qname]
  (if (str/starts-with? qname "<")
    (subs qname 1 (dec (count qname)))
    (let [[p local] (str/split qname #":" 2)]
      (str (get prefixes p "") local))))

(defn- parse-triples-map
  [content prefixes]
  (let [tbl (some-> (re-find #"rr:tableName\s+\"([^\"]+)\"" content) second)
        ;; Use Pattern.DOTALL flag to handle multiline content
        template (some-> (re-find #"(?s)rr:subjectMap\s*\[.*?rr:template\s+\"([^\"]+)\"" content) second)
        ;; Extract class from subject map
        subject-map-block (some-> (re-find #"rr:subjectMap\s*\[([^\]]+)\]" content) second)
        rdf-class (when subject-map-block
                    (some-> (re-find #"rr:class\s+([^;\s]+)" subject-map-block) second))
        pom-blocks (re-seq #"rr:predicateObjectMap\s*\[([^\]]+)\]" content)
        preds (->> pom-blocks
                   (map second)
                   (keep (fn [blk]
                           (when-let [pred (or (some-> (re-find #"rr:predicate\s+([^;\s]+)\s*;" blk) second)
                                               (some-> (re-find #"rr:predicate\s+([^;\s]+)" blk) second))]
                             (let [col (some-> (re-find #"rr:objectMap\s*\[\s*rr:column\s+\"([^\"]+)\"" blk) second)
                                   obj-template (some-> (re-find #"rr:objectMap\s*\[\s*rr:template\s+\"([^\"]+)\"" blk) second)
                                   datatype (some-> (re-find #"rr:datatype\s+([^;\s]+)\s*;" blk) second)]
                               (when (or col obj-template)
                                 (let [pred-iri (expand-qname prefixes pred)
                                       obj-map (cond-> {}
                                                 col (assoc :column col)
                                                 obj-template (assoc :template obj-template)
                                                 datatype (assoc :datatype (expand-qname prefixes datatype)))]
                                   [pred-iri obj-map]))))))
                   (into {}))]
    {:table tbl
     :subject-template template
     :class (when rdf-class (expand-qname prefixes rdf-class))
     :predicates preds}))

(defn- parse-min-r2rml
  [mapping-path]
  (let [content (slurp mapping-path)
        prefixes (parse-prefixes content)]
    ;; Find all triples maps by looking for the pattern
    (let [triples-map-pattern #"([a-zA-Z][\w\-]*:[\w\-]+)\s+a\s+rr:TriplesMap\s*;"
          matches (re-seq triples-map-pattern content)]
      (if (seq matches)
        (let [result (into {}
                           (for [[_ map-name] matches]
                             (let [start-pattern (re-pattern (str "\\Q" map-name "\\E\\s+a\\s+rr:TriplesMap\\s*;"))
                                   start-match (re-find start-pattern content)
                                   start-pos (str/index-of content start-match)
                                  ;; Find the end by looking for the period that ends this triples map
                                   remaining-content (subs content start-pos)
                                  ;; Look for the period that ends this triples map (after all predicate-object maps)
                                   end-pos (str/index-of remaining-content " .\n")
                                   map-content (if end-pos
                                                 (subs remaining-content 0 (+ end-pos 3)) ; Include the " .\n"
                                                 remaining-content)]
                               [map-name (parse-triples-map map-content prefixes)])))]
          result)
        {}))))

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

(defn- build-where-clause
  "Build WHERE clause from literal filter conditions."
  [predicates pred->literal]
  (let [conditions (for [[pred-iri literal-val] pred->literal
                         :when (get predicates pred-iri)
                         :let [{:keys [column]} (get predicates pred-iri)]]
                     (if (string? literal-val)
                       (format "%s = '%s'" column literal-val)
                       (format "%s = %s" column literal-val)))]
    (when (seq conditions)
      (str " WHERE " (str/join " AND " conditions)))))

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

          ;; Extract variable bindings and literal filters
          pred->var (extract-predicate-bindings clause)
          pred->literal (extract-literal-filters clause)

          ;; Build SELECT and WHERE clauses
          clause-predicates (set (keys pred->var))
          select-cols (build-select-columns predicates pred->var clause-predicates)
          all-selects (combine-select-columns select-cols template-cols id-col)
          where-clause (build-where-clause predicates pred->literal)

          ;; Generate final SQL
          final-sql (format "SELECT %s FROM %s%s"
                            all-selects
                            (str/upper-case table)
                            (or where-clause ""))]

      (when (seq pred->literal)
        (log/debug "Literal filters:" pred->literal)
        (log/debug "Generated SQL:" final-sql))

      final-sql)))

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
                                      ;; Execute R2RML processing directly (like BM25 does search directly)
                                      (try
                                        (let [cfg config
                                              rdb (or (:rdb cfg) (get cfg "rdb"))
                                              db-spec (jdbc-spec rdb)
                                              mapping-file (or (:mapping cfg) (get cfg "mapping") (:mapping mapping-spec) (get mapping-spec "mapping"))
                                              mappings (parse-min-r2rml mapping-file)
                                              mapping (analyze-clause-for-mapping patterns mappings)
                                              sql (sql-for-mapping mapping patterns)
                                              rows (jdbc/query db-spec [sql])
                                              template (:subject-template mapping)
                                              var-mappings (extract-predicate-bindings-full patterns)
                                              subject-var (some extract-subject-variable patterns)]

                                          ;; Process all rows and stream them
                                          (doseq [row rows]
                                            (let [id (or (:id row) (get row :ID) (get row "ID"))
                                                  subject-id (when template
                                                               (let [template-cols (extract-template-cols template)]
                                                                 (reduce (fn [tmpl col]
                                                                           (let [col-val (get-column-value row col)]
                                                                             (if col-val
                                                                               (str/replace tmpl (str "{" col "}") (str col-val))
                                                                               tmpl)))
                                                                         template
                                                                         template-cols)))
                                                  type-var (some extract-type-variable patterns)
                                                  solution-map (into (or solution {})
                                                                     (concat
                                                                      (when subject-var
                                                                        (let [subj-symbol (if (symbol? subject-var) subject-var (symbol subject-var))
                                                                              subj-iri (or subject-id (str "http://example.com/id/" (or id "unknown")))]
                                                                          [[subj-symbol (where/match-iri {} subj-iri)]]))
                                                                      (when (and type-var (:class mapping))
                                                                        (let [type-sym (if (symbol? type-var) type-var (symbol type-var))]
                                                                          [[type-sym (where/match-iri {} (:class mapping))]]))
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
                                                                          [var-sym (value->rdf-match value var-sym)]))))]
                                              (async/>! ch solution-map)))
                                          (async/close! ch))
                                        (catch Exception e
                                          (log/error e "Error in R2RML processing")
                                          (async/>! error-ch e)
                                          (async/close! ch)))
                                      ;; No R2RML patterns, just pass through
                                      (do (async/>! ch solution)
                                          (async/close! ch))))
                                  (catch Exception e
                                    (async/>! error-ch e)
                                    (async/close! ch)))))
                            solution-ch)
      out-ch)))

(defn ->R2RMLDatabase
  [{:keys [alias config] :as vg-opts}]
  (let [cfg (or config
                (select-keys vg-opts [:mapping :mappingInline :rdb :baseIRI "mapping" "mappingInline" "rdb" "baseIRI"]))]
    (map->R2RMLDatabase {:alias alias
                         :config cfg
                         :mapping-spec (select-keys cfg [:mapping :mappingInline :baseIRI
                                                         "mapping" "mappingInline" "baseIRI"])

                         :datasource nil})))
