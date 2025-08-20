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

(defn- parse-min-r2rml
  [mapping-path]
  (let [content (slurp mapping-path)
        prefixes (parse-prefixes content)
        tbl (some-> (re-find #"rr:tableName\s+\"([^\"]+)\"" content) second)
        template (some-> (re-find #"rr:subjectMap\s*\[\s*rr:template\s+\"([^\"]+)\"" content) second)
        pom-matches (re-seq #"rr:predicateObjectMap\s*\[([^\]]+)\]" content)
        preds (->> pom-matches
                   (map second)
                   (keep (fn [blk]
                           (when-let [pred (or (some-> (re-find #"rr:predicate\s+([^;\s]+)\s*;" blk) second)
                                               (some-> (re-find #"rr:predicate\s+([^;\s]+)" blk) second))]
                             (when-let [col (some-> (re-find #"rr:objectMap\s*\[\s*rr:column\s+\"([^\"]+)\"" blk) second)]
                               [(expand-qname prefixes pred) {:column col}]))))
                   (into {}))]
    {:table tbl
     :subject-template template
     :predicates preds}))

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

(defn- sql-for-mapping
  [{:keys [table subject-template predicates]}]
  (let [id-col (or (some->> (extract-template-cols subject-template) first)
                   "ID")
        select-cols (->> predicates
                         (map (fn [[pred {:keys [column]}]]
                                (let [alias (-> pred (str/split #"/") last)]
                                  (str column " AS " alias))))
                         (cons (str id-col " AS id"))
                         (str/join ", "))]
    (format "SELECT %s FROM %s" select-cols table)))

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
  (-execute-graph-clause [_ _tracker solution _clause error-ch]
    (let [out (async/chan 1)]
      (async/thread
        (try
          (let [cfg config
                rdb (or (:rdb cfg) (get cfg "rdb"))
                db-spec (jdbc-spec rdb)
                mapping-file (or (:mapping cfg) (get cfg "mapping") (:mapping mapping-spec) (get mapping-spec "mapping"))
                mapping (parse-min-r2rml mapping-file)
                sql (sql-for-mapping mapping)
                rows (jdbc/query db-spec [sql])
                template (:subject-template mapping)]
            (doseq [row rows]
              (let [id    (or (:id row) (get row :ID) (get row "ID"))
                    s-iri (when (and template id)
                            (str/replace template (re-pattern (str "\\{" (or (some-> (extract-template-cols template) first) "ID") "\\}")) (str id)))
                    sol1  (if s-iri
                            (assoc solution '?s (-> (where/unmatched-var '?s)
                                                    (where/match-iri s-iri)))
                            solution)
                    sol2  (reduce (fn [acc [pred {:keys [column]}]]
                                    (let [alias (-> pred (str/split #"/") last)
                                          v (or (get row (keyword (str/lower-case alias)))
                                                (get row (keyword alias))
                                                (get row alias))]
                                      (if (some? v)
                                        (assoc acc (symbol (str "?" alias)) (where/match-value (where/unmatched-var (symbol (str "?" alias))) v))
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


