(ns fluree.db.virtual-graph.r2rml.db
  (:require [clojure.core.async :as async :refer [go >!]]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util.log :as log]
            [fluree.db.virtual-graph :as vg]))

(set! *warn-on-reflection* true)

(defn- read-subject-template
  [mapping-path]
  (try
    (when mapping-path
      (let [content (slurp mapping-path)
            m (re-find #"rr:template\s+\"([^\"]+)\"" content)]
        (second m)))
    (catch Throwable _ nil)))

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
        template (some-> (re-find #"rr:subjectMap\s*\[\s*rr:template\s+\"([^\"]+)\"" content) second)
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

(defn- clause->sparql
  "Very minimal conversion from a where clause vector of triples into a SPARQL BGP string."
  [clause]
  (let [triple->s (fn [[s p o]]
                    (let [fmt (fn [m]
                                (cond
                                  (where/matched-iri? m) (str "<" (where/get-iri m) ">")
                                  (where/matched-value? m) (pr-str (where/get-value m))
                                  :else (name (where/get-variable m))))]
                      (str (fmt s) " " (fmt p) " " (fmt o) " .")))]
    (->> clause (map triple->s) (str/join "\n"))))

(defn- solution->bindings
  [solution]
  (->> solution
       (map (fn [[k v]]
              (when (and (symbol? k) (where/matched? v))
                (let [var (if (-> k name (str/starts-with? "?")) (name k) (str "?" (name k)))
                      val (or (where/get-iri v)
                              (where/get-value v))]
                  [var [val]]))))
       (remove nil?)
       (into {})))

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

(defn- row->solution
  [solution row]
  (reduce (fn [sol [k v]]
            (let [k-str (name k)
                  var-sym (symbol (if (str/starts-with? k-str "?") k-str (str "?" k-str)))]
              (assoc sol var-sym (where/match-value where/unmatched v))))
          solution
          row))

(defn- find-mapping-for-predicate
  [mappings predicate]
  (some (fn [[_ mapping]]
          (when (get-in mapping [:predicates predicate])
            mapping))
        mappings))

(defn- analyze-clause-for-mapping
  "Analyze the clause to determine which mapping(s) to use based on predicates."
  [clause mappings]
  (if (empty? mappings)
    nil
    (let [;; Extract predicates from the clause - the clause is a list of triples [s p o]
          ;; where predicate is a map with :fluree.db.query.exec.where/iri key
          predicate-maps (filter map? (map second clause))
          predicates (->> predicate-maps
                          (map :fluree.db.query.exec.where/iri) ; Extract the IRI using the correct namespaced key
                          (set))
          relevant-mappings (->> mappings
                                 (filter (fn [[_ mapping]]
                                           (some (fn [pred] (get-in mapping [:predicates pred])) predicates)))
                                 (map second))]
      (if (seq relevant-mappings)
        (first relevant-mappings)
        (first (vals mappings))))))

(defn- sql-for-mapping
  [mapping]
  (if (nil? mapping)
    "SELECT 1 WHERE 1=0" ; Return no results if no mapping
    (let [table (:table mapping)
          predicates (:predicates mapping)
          id-col (or (some->> (extract-template-cols (:subject-template mapping)) first)
                     "id")
          select-cols (->> predicates
                           (filter (fn [[_ {:keys [column template]}]]
                                     (or column template))) ; Only include predicates with column or template
                           (map (fn [[pred {:keys [column template]}]]
                                  (if column
                                    (let [alias (-> pred (str/split #"/") last)]
                                      (str column " AS " alias))
                                    (when template
                                      (let [alias (-> pred (str/split #"/") last)]
                                        (str "NULL AS " alias)))))) ; Use NULL for template-based predicates
                           (remove nil?)
                           (cons (str id-col " AS id"))
                           (str/join ", "))]
      (format "SELECT %s FROM %s" select-cols table))))

(defn- sql-for-predicates
  [mappings predicates]
  (let [table-mappings (->> predicates
                            (map (fn [pred] (find-mapping-for-predicate mappings pred)))
                            (remove nil?)
                            (group-by :table))
        sqls (->> table-mappings
                  (map (fn [[table table-maps]]
                         (let [table-map (first table-maps)
                               relevant-preds (->> predicates
                                                   (filter (fn [pred]
                                                             (get-in table-map [:predicates pred]))))
                               select-cols (->> relevant-preds
                                                (map (fn [pred]
                                                       (let [{:keys [column]} (get-in table-map [:predicates pred])
                                                             alias (-> pred (str/split #"/") last)]
                                                         (str column " AS " alias))))
                                                (str/join ", "))]
                           (format "SELECT %s FROM %s" select-cols table))))
                  (str/join " UNION ALL "))]
    sqls))

(defrecord R2RMLDatabase [alias config mapping-spec datasource]
  vg/UpdatableVirtualGraph
  (upsert [this _source-db _new-flakes _remove-flakes]
    (go this))
  (initialize [this _source-db]
    (go this))

  where/Matcher
  (-match-id [_ _tracker _solution _s-mch _error-ch]
    where/nil-channel)
  (-match-triple [_ _tracker _solution _triple _error-ch]
    where/nil-channel)
  (-match-class [_ _tracker _solution _triple _error-ch]
    where/nil-channel)
  (-activate-alias [this _alias]
    (go this))
  (-aliases [_]
    [alias])
  (-finalize [_ _tracker _error-ch solution-ch]
    solution-ch)

  where/GraphClauseExecutor
  (-execute-graph-clause [_ _tracker solution clause error-ch]
    (let [out (async/chan 1)]
      (async/thread
        (try
          (let [cfg config
                rdb (or (:rdb cfg) (get cfg "rdb"))
                db-spec (jdbc-spec rdb)
                mapping-file (or (:mapping cfg) (get cfg "mapping") (:mapping mapping-spec) (get mapping-spec "mapping"))
                mappings (parse-min-r2rml mapping-file)
                ;; Analyze clause to determine which mapping to use
                mapping (analyze-clause-for-mapping clause mappings)
                sql (sql-for-mapping mapping)
                rows (jdbc/query db-spec [sql])
                template (:subject-template mapping)
                ;; Extract variable mappings from clause: [s p o] where p is predicate and o is variable
                var-mappings (->> clause
                                  (map (fn [[s p o]]
                                         (when (and (map? p) (map? o))
                                           [(get p :fluree.db.query.exec.where/iri)
                                            (get o :fluree.db.query.exec.where/var)])))
                                  (remove nil?)
                                  (into {}))]
            (doseq [row rows]
              (let [id    (or (:id row) (get row :ID) (get row "ID"))
                    s-iri (when (and template id)
                            (str/replace template (re-pattern (str "\\{" (or (some-> (extract-template-cols template) first) "ID") "\\}")) (str id)))
                    sol1  (if s-iri
                            (assoc solution '?s (-> (where/unmatched-var '?s)
                                                    (where/match-iri s-iri)))
                            solution)
                    sol2  (reduce (fn [acc [pred {:keys [column]}]]
                                    (let [var-sym (get var-mappings pred)
                                          alias (-> pred (str/split #"/") last)
                                          v (or (get row (keyword (str/lower-case alias)))
                                                (get row (keyword alias))
                                                (get row alias))]
                                      (if (and (some? v) var-sym)
                                        (assoc acc var-sym (where/match-value (where/unmatched-var var-sym) v))
                                        acc)))
                                  sol1
                                  (:predicates mapping))]
                (async/>!! out sol2)))
            (async/close! out))
          (catch Throwable e
            (log/error e "R2RML clause execution error")
            (async/>!! error-ch e)
            (async/close! out))))
      out)))

(defn ->R2RMLDatabase
  [{:keys [alias config] :as vg-opts}]
  (let [cfg (or config
                (select-keys vg-opts [:mapping :mappingInline :rdb :baseIRI "mapping" "mappingInline" "rdb" "baseIRI"]))]
    (map->R2RMLDatabase {:alias alias
                         :config cfg
                         :mapping-spec (select-keys cfg [:mapping :mappingInline :baseIRI "mapping" "mappingInline" "baseIRI"])
                         :datasource nil})))
