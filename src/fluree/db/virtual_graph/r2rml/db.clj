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

(defn- clause->sparql
  "Very minimal conversion from a where clause vector of triples into a SPARQL BGP string.
   This is a placeholder; we will wire a proper FQL->SPARQL translator for the subgraph.
   Assumes `clause` is a vector of :tuple patterns already assigned matched vars."
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
  "Build solution-bindings map for pushdown from currently bound variables.
   Returns a map of var-name (with leading ?) -> vector of bound values as SPARQL terms (simple strings)."
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
  "Convert a SQL row map into a Fluree where solution extending base `solution`.
   Treats values as plain literals for now."
  [solution row]
  (reduce (fn [sol [k v]]
            (let [k-str (name k)
                  var-sym (symbol (if (str/starts-with? k-str "?") k-str (str "?" k-str)))]
              (assoc sol var-sym (where/match-value where/unmatched v))))
          solution
          row))

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
                _ (when (and (nil? (:mapping cfg)) (nil? (get cfg "mapping")))
                    (log/debug "R2RML mapping not found in config; proceeding with minimal SQL builder for test"))
                rdb (or (:rdb cfg) (get cfg "rdb"))
                db-spec (jdbc-spec rdb)
                mapping-file (or (:mapping cfg) (get cfg "mapping") (:mapping mapping-spec) (get mapping-spec "mapping"))
                subject-template (read-subject-template mapping-file)
                rows (jdbc/query db-spec ["SELECT ID AS id, NAME AS name FROM PEOPLE"])]
            (log/info "R2RML minimal query returned rows:" (count rows) (when (seq rows) (first rows)))
            (doseq [row rows]
              (let [id    (or (:id row) (get row :ID) (get row "ID"))
                    nm    (or (:name row) (get row :NAME) (get row "NAME"))
                    s-iri (when (and subject-template id)
                            (str/replace subject-template "{ID}" (str id)))
                    sol1  (if s-iri
                            (assoc solution '?s (-> (where/unmatched-var '?s)
                                                    (where/match-iri s-iri)))
                            solution)
                    sol2  (if (some? nm)
                            (assoc sol1 '?name (where/match-value (where/unmatched-var '?name) nm))
                            sol1)]
                (async/>!! out sol2)))
            (async/close! out))
          (catch Throwable e
            (log/error e "R2RML clause execution error")
            (async/>!! error-ch e)
            (async/close! out))))
      out)))

(defn ->R2RMLDatabase
  "Constructs an R2RMLDatabase from vg-opts.
   Accepts both stored {:config {...}} and flattened keys (as vg-opts is built in nameservice-loader)."
  [{:keys [alias config] :as vg-opts}]
  (let [cfg (or config
                (select-keys vg-opts [:mapping :mappingInline :rdb :baseIRI "mapping" "mappingInline" "rdb" "baseIRI"]))]
    (map->R2RMLDatabase {:alias alias
                         :config cfg
                         :mapping-spec (select-keys cfg [:mapping :mappingInline :baseIRI "mapping" "mappingInline" "baseIRI"])
                         :datasource nil})))


