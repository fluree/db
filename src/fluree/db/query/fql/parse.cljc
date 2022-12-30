(ns fluree.db.query.fql.parse
  (:require [fluree.db.query.exec :as exec]
            [fluree.db.query.parse.aggregate :refer [parse-aggregate]]
            [fluree.db.query.json-ld.select :refer [parse-subselection]]
            [fluree.db.query.subject-crawl.legacy :refer [basic-to-analytical-transpiler]]
            [fluree.db.query.fql.syntax :as syntax]
            [clojure.spec.alpha :as spec]
            [clojure.string :as str]
            [clojure.set :as set]
            [clojure.walk :refer [postwalk]]
            [fluree.json-ld :as json-ld]
            [fluree.db.query.range :as query-range]
            [clojure.core.async :as async :refer [<! >! go go-loop]]
            [fluree.db.flake :as flake]
            [fluree.db.util.async :refer [<? go-try merge-into?]]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.query.analytical-filter :as filter]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const]))

(defn basic-query?
  [q]
  (contains? q :from))

(def rdf-type-preds #{"http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                      "a"
                      :a
                      "rdf:type"
                      :rdf/type
                      "@type"})

(defn rdf-type?
  [p]
  (contains? rdf-type-preds p))

(defn variable?
  [x]
  (and (or (string? x) (symbol? x) (keyword? x))
       (-> x name first (= \?))))

(defn query-fn?
  [x]
  (or (syntax/fn-string? x)
      (syntax/fn-list? x)))

(defn select-map?
  [x]
  (map? x))

(def read-str #?(:clj read-string :cljs cljs.reader/read-string))

(defn safe-read
  [code-str]
  (try*
    (let [code (read-str code-str)]
      (when-not (list? code)
        (throw (ex-info (code-str "Invalid function: " code-str)
                        {:status 400 :error :db/invalid-query})))
      code)
    (catch* e
            (log/warn "Invalid query function attempted: " code-str " with error message: " (ex-message e))
            (throw (ex-info (code-str "Invalid query function: " code-str)
                            {:status 400 :error :db/invalid-query})))))

(defn parse-var-name
  "Returns a `x` as a symbol if `x` is a valid '?variable'."
  [x]
  (when (variable? x)
    (symbol x)))

(defn variables
  "Returns the set of items within the arbitrary data structure `data` that
  are variables ."
  [data]
  (postwalk (fn [x]
              (if (coll? x)
                (apply set/union x)
                (if-let [var (parse-var-name x)]
                  #{var}
                  #{})))
            data))

(defn find-filtered-var
  "Returns the var that will represent flake/o when passed in a flake to execute
  filter fn.

  There can be multiple vars in the filter function which can utilize the
  original query's 'vars' map, however there should be exactly one var in the
  filter fn that isn't in that map - which should be the var that will receive
  flake/o."
  [params vars]
  (let [non-assigned-vars (set/difference params vars)]
    (case (count non-assigned-vars)
      1 (first non-assigned-vars)
      0 (throw (ex-info (str "Query filter function has no variable assigned to it, all parameters "
                             "exist in the 'vars' map. Filter function params: " params ". "
                             "Vars assigned in query: " vars ".")
                        {:status 400
                         :error  :db/invalid-query}))
      ;; else
      (throw (ex-info (str "Vars used in a filter function are not included in the 'vars' map "
                           "or as a binding. Should only be missing one var, but missing: " (vec non-assigned-vars) ".")
                      {:status 400
                       :error  :db/invalid-query})))))

(defn parse-filter-function
  "Evals, and returns query function."
  [code-str vars]
  (let [code      (safe-read code-str)
        code-vars (or (not-empty (variables code))
                      (throw (ex-info (str "Filter function must contain a valid variable. Provided: " code-str)
                                      {:status 400 :error :db/invalid-query})))
        var-name  (find-filtered-var code-vars vars)
        params    (vec code-vars)
        [fun _]   (filter/extract-filter-fn code code-vars)]
    {::exec/var    var-name
     ::exec/params params
     ::exec/fn-str (str "(fn " params " " fun ")")
     ::exec/fn     (filter/make-executable params fun)}))

(def ^:const default-recursion-depth 100)

(defn recursion-predicate
  "A predicate that ends in a '+', or a '+' with some integer afterwards is a recursion
  predicate. e.g.: person/follows+3

  Returns a two-tuple of predicate followed by # of times to recur.

  If not a recursion predicate, returns nil."
  [predicate context]
  (when (or (string? predicate)
            (keyword? predicate))
    (when-let [[_ pred recur-n] (re-find #"(.+)\+(\d+)?$" (name predicate))]
      [(json-ld/expand (keyword (namespace predicate) pred)
                       context)
       (if recur-n
         (util/str->int recur-n)
         default-recursion-depth)])))

(defn parse-variable
  [x]
  (when-let [var-name (parse-var-name x)]
    {::exec/var var-name}))

(defn parse-pred-ident
  [x]
  (when (util/pred-ident? x)
    {::exec/ident x}))

(defn parse-subject-id
  ([x]
   (when (syntax/sid? x)
     {::exec/val x}))

  ([x context]
   (if-let [parsed (parse-subject-id x)]
     parsed
     (when context
       {::exec/val (json-ld/expand-iri x context)}))))

(defn parse-subject-pattern
  [s-pat context]
  (when s-pat
    (or (parse-variable s-pat)
        (parse-pred-ident s-pat)
        (parse-subject-id s-pat context)
        (throw (ex-info (str "Subject values in where statement must be integer subject IDs or two-tuple identies. "
                             "Provided: " s-pat ".")
                        {:status 400 :error :db/invalid-query})))))

(defn parse-class-predicate
  [x]
  (when (rdf-type? x)
    {::exec/val const/$rdf:type}))

(defn parse-iri-predicate
  [x]
  (when (= "@id" x)
    {::exec/val const/$iri}))

(defn iri->id
  [iri db context]
  (let [full-iri (json-ld/expand-iri iri context)]
    (dbproto/-p-prop db :id full-iri)))

(defn iri->pred-id
  [iri db context]
  (or (iri->id iri db context)
      (throw (ex-info (str "Invalid predicate: " iri)
                      {:status 400 :error :db/invalid-query}))))

(defn parse-recursion-predicate
  [x db context]
  (when-let [[p-iri recur-n] (recursion-predicate x context)]
    {::exec/val   (iri->pred-id p-iri db context)
     ::exec/recur (or recur-n util/max-integer)}))

(defn parse-full-text-predicate
  [x db context]
  (when (and (string? x)
             (str/starts-with? x "fullText:"))
    {::exec/full-text (iri->pred-id (subs x 9) db context)}))

(defn parse-predicate-id
  [x db context]
  {::exec/val (iri->pred-id x db context)})

(defn parse-predicate-pattern
  [p-pat db context]
  (or (parse-iri-predicate p-pat)
      (parse-class-predicate p-pat)
      (parse-variable p-pat)
      (parse-recursion-predicate p-pat db context)
      (parse-full-text-predicate p-pat db context)
      (parse-predicate-id p-pat db context)))

(defn parse-class
  [o-iri db context]
  (if-let [id (iri->id o-iri db context)]
    (parse-subject-id id)
    (throw (ex-info (str "Undefined RDF type specified: " (json-ld/expand-iri o-iri context))
                    {:status 400 :error :db/invalid-query}))))

(defn parse-object-pattern
  [o-pat context]
  (or (parse-variable o-pat)
      (parse-pred-ident o-pat)
      {::exec/val o-pat}))

(defmulti parse-pattern
  (fn [pattern vars db context]
    (if (map? pattern)
      (->> pattern keys first)
      :tuple)))

(defn filter-pattern?
  [x]
  (and (map? x)
       (-> x keys first (= :filter))))

(defn parse-filter-maps
  [vars filters]
  (let [vars (set vars)]
    (->> filters
         (mapcat vals)
         flatten
         (map (fn [f-str]
                (parse-filter-function f-str vars)))
         (reduce (fn [m fltr]
                   (let [var-name (::exec/var fltr)]
                     (update m var-name (fn [var-fltrs]
                                          (-> var-fltrs
                                              (or [])
                                              (conj fltr))))))
                 {}))))

(defn parse-where-clause
  [clause vars db context]
  (let [patterns (->> clause
                      (remove filter-pattern?)
                      (mapv (fn [pattern]
                              (parse-pattern pattern vars db context))))
        filters  (->> clause
                      (filter filter-pattern?)
                      (parse-filter-maps vars))]
    (exec/->where-clause patterns filters)))

(defn parse-tuple
  [[s-pat p-pat o-pat] db context]
  (let [s (parse-subject-pattern s-pat context)
        p (parse-predicate-pattern p-pat db context)]
    (if (= const/$rdf:type (::exec/val p))
      (let [cls (parse-class o-pat db context)]
        (exec/->pattern :class [s p cls]))
      (let [o     (parse-object-pattern o-pat context)
            tuple [s p o]]
        (if (= const/$iri (::exec/val p))
          (let [o*     (-> o
                           (update ::exec/val json-ld/expand-iri context)
                           (assoc ::exec/datatype const/$xsd:anyURI))
                tuple* [s p o*]]
            (exec/->pattern :iri tuple*))
          tuple)))))

(defmethod parse-pattern :tuple
  [tuple _ db context]
  (parse-tuple tuple db context))

(defmethod parse-pattern :union
  [{:keys [union]} vars db context]
  (let [parsed (mapv (fn [clause]
                       (parse-where-clause clause vars db context))
                     union)]
    (exec/->pattern :union parsed)))

(defmethod parse-pattern :optional
  [{:keys [optional]} vars db context]
  (let [clause (if (coll? (first optional))
                 optional
                 [optional])
        parsed (parse-where-clause clause vars db context)]
    (exec/->pattern :optional parsed)))

(defn from->tuple
  [from-clause context]
  (let [s-var (symbol "?s")]
    (cond
      (syntax/sid? from-clause)      [s-var :_id from-clause]
      (or (string? from-clause)
          (keyword? from-clause))    [s-var "@id" (json-ld/expand-iri from-clause context)]
      (util/pred-ident? from-clause) [s-var (first from-clause) (second from-clause)])))

(defn parse-where
  [q vars db context]
  (if-let [where (:where q)]
    [(parse-where-clause where vars db context) vars]
    (let [from-clause (:from q)]
      (if (coll? from-clause)
        (let [vars* (assoc vars '?__subj from-clause)
              where `[[?s :_id ?__subj]]]
          [(parse-where-clause where vars* db context) vars*])
        (let [where [(from->tuple from-clause context)]]
          [(parse-where-clause where vars db context) vars])))))

(defn parse-context
  [q db]
  (let [db-ctx (get-in db [:schema :context])
        q-ctx  (or (:context q) (get q "@context"))]
    (json-ld/parse-context db-ctx q-ctx)))

(defn parse-selector
  [db context depth s]
  (cond
    (variable? s)   (parse-var-name s)
    (query-fn? s)   (let [{:keys [variable function]} (parse-aggregate s)]
                      (exec/->aggregate-selector variable function))
    (select-map? s) (let [{:keys [variable selection depth spec]}
                          (parse-subselection db context s depth)]
                      (exec/->subgraph-selector variable selection spec depth))))

(defn parse-select-clause
  [clause db context depth]
  (if (sequential? clause)
    (mapv (partial parse-selector db context depth)
          clause)
    (parse-selector db context depth clause)))

(defn parse-select
  [q db context]
  (let [depth  (or (:depth q) 0)]
    (if (:selectOne q)
      (update q :selectOne parse-select-clause db context depth)
      (update q :select parse-select-clause db context depth))))

(defn parse-vars
  [{:keys [vars] :as _q}]
  (when vars
    (reduce-kv (fn [m var val]
                 (let [variable (-> (parse-variable var)
                                    (assoc ::val val))]
                   (assoc m var variable)))
               {} vars)))

(defn ensure-vector
  [x]
  (if (vector? x)
    x
    [x]))

(defn parse-grouping
  [q]
  (some->> (or (:groupBy q)
               (:group-by q))
           ensure-vector
           (mapv parse-var-name)))

(defn parse-ordering
  [q]
  (some->> (or (:order-by q)
               (:orderBy q))
           ensure-vector
           (mapv (fn [ord]
                   (if-let [v (parse-var-name ord)]
                     [v :asc]
                     (let [[dir dim] ord
                           v         (parse-var-name dim)]
                       (if (syntax/asc? dir)
                         [v :asc]
                         [v :desc])))))))

(defn parse
  [q db]
  (let [context       (parse-context q db)
        q             (cond->> q
                        (basic-query? q) (basic-to-analytical-transpiler db))
        supplied-vars (parse-vars q)
        [where vars]  (parse-where q supplied-vars db context)
        grouping      (parse-grouping q)
        ordering      (parse-ordering q)]
    (cond-> (assoc q
                   :context context
                   :vars    supplied-vars
                   :where   where)
      grouping (assoc :group-by grouping)
      ordering (assoc :order-by ordering)
      true     (parse-select db context))))
