(ns fluree.db.query.fql.parse
  (:require [fluree.db.query.exec.eval :as eval]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.exec.select :as select]
            [fluree.db.query.json-ld.select :refer [parse-subselection]]
            [fluree.db.datatype :as datatype]
            [fluree.db.query.subject-crawl.reparse :refer [re-parse-as-simple-subj-crawl]]
            [fluree.db.query.fql.syntax :as syntax]
            [clojure.string :as str]
            [clojure.set :as set]
            [clojure.walk :refer [postwalk]]
            [fluree.json-ld :as json-ld]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const]
            #?(:cljs [cljs.reader :refer [read-string]])))

#?(:clj (set! *warn-on-reflection* true))

(defn parse-context
  [q db]
  (dbproto/-context db (:context q)))

(defn parse-var-name
  "Returns a `x` as a symbol if `x` is a valid '?variable'."
  [x]
  (when (syntax/variable? x)
    (symbol x)))

(defn parse-variable
  [x]
  (some-> x parse-var-name where/unmatched))

(defn parse-value-binding
  [vars vals]
  (let [var-matches (mapv parse-variable vars)
        binding     (mapv (fn [var-match value]
                            (let [dt (datatype/infer value)]
                              (where/match-value var-match value dt)))
                          var-matches vals)]
    (zipmap vars binding)))

(defn parse-values
  [q]
  (when-let [values (:values q)]
    (let [[vars vals] values
          vars*     (util/sequential vars)
          vals*     (mapv util/sequential vals)
          var-count (count vars*)]
      (if (every? (fn [bdg]
                    (= (count bdg) var-count))
                  vals*)
        [vars* (mapv (partial parse-value-binding vars*)
                     vals*)]
        (throw (ex-info (str "Invalid value binding: "
                             "number of variables and values don't match: "
                             values)
                        {:status 400 :error :db/invalid-query}))))))

(def rdf-type-preds #{"http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                      "a"
                      :a
                      "rdf:type"
                      :rdf/type
                      "@type"})

(defn rdf-type?
  [p]
  (contains? rdf-type-preds p))

(defn select-map?
  [x]
  (map? x))

(defn safe-read
  [code-str]
  (try*
   (let [code (read-string code-str)]
     (when-not (list? code)
       (throw (ex-info (code-str "Invalid function: " code-str)
                       {:status 400 :error :db/invalid-query})))
     code)
   (catch* e
     (log/warn e "Invalid query function attempted: " code-str)
     (throw (ex-info (str "Invalid query function: " code-str)
                     {:status 400 :error :db/invalid-query})))))

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

(defn parse-code
  [x]
  (log/debug "parse-code:" x)
  (if (list? x)
    x
    (safe-read x)))

(defn parse-filter-function
  "Evals and returns filter function."
  [fltr vars]
  (let [code      (parse-code fltr)
        code-vars (or (not-empty (variables code))
                      (throw (ex-info (str "Filter function must contain a valid variable. Provided: " code)
                                      {:status 400 :error :db/invalid-query})))
        var-name  (find-filtered-var code-vars vars)
        f         (eval/compile-filter code var-name)]
    (where/->function var-name f)))

(defn parse-bind-function
  "Evals and returns bind function."
  [var-name fn-code]
  (let [code (parse-code fn-code)
        _    (log/debug "parse-bind-function code:" code)
        f    (eval/compile code false)]
    (log/debug "parse-bind-function f:" f)
    (where/->function var-name f)))

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

(defn parse-pred-ident
  [x]
  (when (util/pred-ident? x)
    (where/->ident x)))

(defn parse-subject-iri
  [x context]
  (-> x
      (json-ld/expand-iri context)
      (where/anonymous-value const/$xsd:anyURI)))

(defn parse-sid
  [x]
  (when (syntax/sid? x)
    (where/anonymous-value x)))

(defn parse-subject
  ([x]
   (parse-sid x))

  ([x context]
   (if-let [parsed (parse-subject x)]
     parsed
     (when context
       (parse-subject-iri x context)))))

(defn parse-subject-pattern
  [s-pat context]
  (when s-pat
    (or (parse-variable s-pat)
        (parse-pred-ident s-pat)
        (parse-subject s-pat context)
        (throw (ex-info (str "Subject values in where statement must be integer subject IDs or two-tuple identies. "
                             "Provided: " s-pat ".")
                        {:status 400 :error :db/invalid-query})))))

(defn parse-class-predicate
  [x]
  (when (rdf-type? x)
    (where/anonymous-value const/$rdf:type)))

(defn parse-iri-predicate
  [x]
  (when (syntax/iri-key? x)
    (where/->predicate const/$xsd:anyURI)))

(defn iri->pred-id
  [iri db context]
  (let [full-iri (json-ld/expand-iri iri context)]
    (dbproto/-p-prop db :id full-iri)))

(defn iri->pred-id-strict
  [iri db context]
  (or (iri->pred-id iri db context)
      (throw (ex-info (str "Invalid predicate: " iri)
                      {:status 400 :error :db/invalid-query}))))

(defn parse-recursion-predicate
  [x db context]
  (when-let [[p-iri recur-n] (recursion-predicate x context)]
    (let [iri     (iri->pred-id-strict p-iri db context)
          recur-n (or recur-n util/max-integer)]
      (where/->predicate iri recur-n))))

(defn parse-full-text-predicate
  [x db context]
  (when (and (string? x)
             (str/starts-with? x "fullText:"))
    (-> x
        (subs 9)
        (iri->pred-id-strict db context)
        where/->full-text)))

(defn parse-predicate-id
  [x db context]
  (-> x
      (iri->pred-id-strict db context)
      where/anonymous-value))

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
  (if-let [id (iri->pred-id o-iri db context)]
    (where/anonymous-value id const/$xsd:anyURI)
    (throw (ex-info (str "Undefined RDF type specified: " (json-ld/expand-iri o-iri context))
                    {:status 400 :error :db/invalid-query}))))

(defn parse-object-iri
  [x context]
  (-> x
      (json-ld/expand-iri context)
      where/anonymous-value))

(defn iri-map?
  [x context]
  (and (map? x)
       (= (count x) 1)
       (-> x
           keys
           first
           (json-ld/expand-iri context)
           syntax/iri-key?)))

(defn parse-iri-map
  [x context]
  (when (iri-map? x context)
    (let [o-iri (-> x
                    vals
                    first
                    (json-ld/expand-iri context))]
      (where/->iri-ref o-iri))))

(defn parse-object-pattern
  [o-pat context]
  (or (parse-variable o-pat)
      (parse-pred-ident o-pat)
      (parse-iri-map o-pat context)
      (where/anonymous-value o-pat)))

(defmulti parse-pattern
  (fn [pattern _vars _db _context]
    (log/debug "parse-pattern pattern:" pattern)
    (cond
      (map? pattern) (->> pattern keys first keyword)
      (map-entry? pattern) :binding
      :else :triple)))

(defn type-pattern?
  [typ x]
  (and (map? x)
       (-> x keys first keyword (= typ))))

(def filter-pattern?
  (partial type-pattern? :filter))

(defn parse-filter-maps
  [vars filters]
  (let [vars (set vars)]
    (->> filters
         (mapcat vals)
         flatten
         (map (fn [fltr]
                (parse-filter-function fltr vars)))
         (reduce (fn [m fltr]
                   (let [var-name (::where/var fltr)]
                     (update m var-name (fn [var-fltrs]
                                          (-> var-fltrs
                                              (or [])
                                              (conj fltr))))))
                 {}))))

(defn parse-bind-map
  [bind]
  (reduce (fn [m k] (update m k #(parse-bind-function k %)))
          bind (keys bind)))

(defn parse-where-clause
  [clause vars db context]
  (let [patterns (->> clause
                      (remove filter-pattern?)
                      (log/debug->>val "patterns to parse:")
                      (mapv (fn [pattern]
                              (parse-pattern pattern vars db context))))
        filters  (->> clause
                      (filter filter-pattern?)
                      (parse-filter-maps vars))]
    (log/debug "parse-where-clause patterns:" patterns)
    (where/->where-clause patterns filters)))

(defn parse-triple
  [[s-pat p-pat o-pat] db context]
  (log/debug "parse-triple:" s-pat p-pat o-pat)
  (let [s (parse-subject-pattern s-pat context)
        p (parse-predicate-pattern p-pat db context)]
    (if (and (= const/$rdf:type (::where/val p))
             (not (syntax/variable? o-pat)))
      (let [cls (parse-class o-pat db context)]
        (where/->pattern :class [s p cls]))
      (if (= const/$xsd:anyURI (::where/val p))
        (let [o (parse-object-iri o-pat context)]
          [s p o])
        (let [o (parse-object-pattern o-pat context)]
          [s p o])))))

(defmethod parse-pattern :triple
  [triple _ db context]
  (log/debug "parse-triple:" triple)
  (parse-triple triple db context))

(defmethod parse-pattern :union
  [{:keys [union]} vars db context]
  (let [parsed (mapv (fn [clause]
                       (parse-where-clause clause vars db context))
                     union)]
    (where/->pattern :union parsed)))

(defmethod parse-pattern :optional
  [{:keys [optional]} vars db context]
  (let [clause (if (coll? (first optional))
                 optional
                 [optional])
        parsed (parse-where-clause clause vars db context)]
    (where/->pattern :optional parsed)))

(defmethod parse-pattern :bind
  [{:keys [bind]} _vars _db _context]
  (let [parsed  (parse-bind-map bind)
        _       (log/debug "parsed bind map:" parsed)
        pattern (where/->pattern :bind parsed)]
    (log/debug "parse-pattern :bind pattern:" pattern)
    pattern))

(defmethod parse-pattern :binding
  [[v f] _vars _db _context]
  (log/debug "parse-pattern binding v:" v "- f:" f)
  (where/->pattern :binding [v f]))

(defn parse-where
  [q vars db context]
  (when-let [where (:where q)]
    (parse-where-clause where vars db context)))

(defn parse-selector
  [db context depth s]
  (cond
    (syntax/variable? s) (-> s parse-var-name select/variable-selector)
    (syntax/query-fn? s) (-> s parse-code eval/compile select/aggregate-selector)
    (select-map? s) (let [{:keys [variable selection depth spec]}
                          (parse-subselection db context s depth)]
                      (select/subgraph-selector variable selection depth spec))))

(defn parse-select-clause
  [clause db context depth]
  (if (sequential? clause)
    (mapv (partial parse-selector db context depth)
          clause)
    (parse-selector db context depth clause)))

(defn parse-select
  [q db context]
  (let [depth      (get q :depth 0)
        select-key (some (fn [k]
                           (when (contains? q k) k))
                         [:select :select-one :select-distinct])
        select     (-> q
                       (get select-key)
                       (parse-select-clause db context depth))]
    (assoc q select-key select)))

(defn ensure-vector
  [x]
  (if (vector? x)
    x
    [x]))

(defn parse-grouping
  [q]
  (some->> q
           :group-by
           ensure-vector
           (mapv parse-var-name)))

(defn parse-ordering
  [q]
  (some->> q
           :order-by
           ensure-vector
           (mapv (fn [ord]
                   (if-let [v (parse-var-name ord)]
                     [v :asc]
                     (let [[dir dim] ord
                           v (parse-var-name dim)]
                       (if (syntax/asc? dir)
                         [v :asc]
                         [v :desc])))))))

(defn parse-having
  [q]
  (if-let [code (some-> q :having parse-code)]
    (assoc q :having (eval/compile code))
    q))

(defn parse-analytical-query*
  [q db]
  (log/debug "parse-analytical-query*:" q)
  (let [context  (parse-context q db)
        [vars values] (parse-values q)
        where    (parse-where q vars db context)
        grouping (parse-grouping q)
        ordering (parse-ordering q)]
    (-> q
        (assoc :context context
               :where where)
        (cond-> (seq values) (assoc :values values)
                grouping (assoc :group-by grouping)
                ordering (assoc :order-by ordering))
        parse-having
        (parse-select db context))))

(defn parse-analytical-query
  [q db]
  (let [parsed    (parse-analytical-query* q db)
        re-parsed (or (re-parse-as-simple-subj-crawl parsed)
                      parsed)]
    (log/debug "parse-analytical-query re-parsed:" re-parsed)
    re-parsed))

(defn parse
  [q db]
  (-> q
      syntax/validate-query
      syntax/encode-internal-query
      (log/debug->val "encoded query:")
      (parse-analytical-query db)))

(defn parse-delete
  [q db]
  (when (:delete q)
    (let [context (parse-context q db)
          [vars values] (parse-values q)
          where   (parse-where q vars db context)]
      (-> q
          (assoc :context context
                 :where where)
          (cond-> (seq values) (assoc :values values))
          (update :delete parse-triple db context)))))
