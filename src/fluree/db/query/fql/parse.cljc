(ns fluree.db.query.fql.parse
  (:require [fluree.db.query.exec.eval :as eval]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.exec.select :as select]
            [fluree.db.query.json-ld.select :refer [parse-subselection]]
            [fluree.db.query.subject-crawl.reparse :refer [re-parse-as-simple-subj-crawl]]
            [fluree.db.query.fql.syntax :as syntax]
            [clojure.string :as str]
            [clojure.set :as set]
            [clojure.walk :refer [postwalk]]
            [fluree.json-ld :as json-ld]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.query.analytical-filter :as filter]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const]
            #?(:cljs [cljs.reader :refer [read-string]])))

#?(:clj (set! *warn-on-reflection* true))

(defn parse-context
  [{:keys [opts] :as q} db]
  (let [ctx-key (if (= :string (or (:context-type opts)
                                   (:contextType opts)))
                  :context-str
                  :context)
        db-ctx (get-in db [:schema ctx-key])
        q-ctx  (or (:context q) (get q "@context"))]
    (json-ld/parse-context db-ctx q-ctx)))

(defn parse-var-name
  "Returns a `x` as a symbol if `x` is a valid '?variable'."
  [x]
  (when (syntax/variable? x)
    (symbol x)))

(defn parse-variable
  [x]
  (when-let [var-name (parse-var-name x)]
    (where/->variable var-name)))

(defn parse-vars
  [{:keys [vars] :as _q}]
  (reduce-kv (fn [m var val]
               (let [variable (-> (parse-variable var)
                                  (assoc ::where/val val))]
                 (assoc m var variable)))
             {} vars))

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
  (if (list? x)
    x
    (safe-read x)))

(defn parse-filter-function
  "Evals, and returns query function."
  [fltr vars]
  (let [code      (parse-code fltr)
        code-vars (or (not-empty (variables code))
                      (throw (ex-info (str "Filter function must contain a valid variable. Provided: " code)
                                      {:status 400 :error :db/invalid-query})))
        var-name  (find-filtered-var code-vars vars)
        f         (eval/compile-filter code var-name)]
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

(defn parse-iri
  [x context]
  (-> x
      (json-ld/expand-iri context)
      (where/->value const/$xsd:anyURI)))

(defn parse-sid
  [x]
  (when (syntax/sid? x)
    (where/->value x)))

(defn parse-subject
  ([x]
   (parse-sid x))

  ([x context]
   (if-let [parsed (parse-subject x)]
     parsed
     (when context
       (parse-iri x context)))))

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
    (where/->value const/$rdf:type)))

(defn parse-iri-predicate
  [x]
  (when (= "@id" x)
    (where/->predicate const/$iri)))

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
      where/->value))

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
    (where/->value id const/$xsd:anyURI)
    (throw (ex-info (str "Undefined RDF type specified: " (json-ld/expand-iri o-iri context))
                    {:status 400 :error :db/invalid-query}))))

(defn parse-object-pattern
  [o-pat]
  (or (parse-variable o-pat)
      (parse-pred-ident o-pat)
      (where/->value o-pat)))

(defmulti parse-pattern
  (fn [pattern _vars _db _context]
    (if (map? pattern)
      (->> pattern keys first)
      :triple)))

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
         (map (fn [fltr]
                (parse-filter-function fltr vars)))
         (reduce (fn [m fltr]
                   (let [var-name (::where/var fltr)]
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
    (where/->where-clause patterns filters)))

(defn parse-triple
  [[s-pat p-pat o-pat] db context]
  (let [s (parse-subject-pattern s-pat context)
        p (parse-predicate-pattern p-pat db context)]
    (if (= const/$rdf:type (::where/val p))
      (let [cls (parse-class o-pat db context)]
        (where/->pattern :class [s p cls]))
      (if (= const/$iri (::where/val p))
        (let [o (-> o-pat
                    (json-ld/expand-iri context)
                    where/->value)]
          (where/->pattern :iri [s p o]))
        (let [o (parse-object-pattern o-pat)]
          [s p o])))))

(defmethod parse-pattern :triple
  [triple _ db context]
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

(defn parse-where
  [q vars db context]
  (when-let [where (:where q)]
    (parse-where-clause where vars db context)))

(defn parse-selector
  [db context depth s]
  (cond
    (syntax/variable? s) (-> s parse-var-name select/variable-selector)
    (syntax/query-fn? s) (-> s parse-code eval/compile select/aggregate-selector)
    (select-map? s)      (let [{:keys [variable selection depth spec]}
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
  (let [depth      (or (:depth q) 0)
        select-key (some (fn [k]
                           (and (contains? q k) k))
                         [:select :selectOne :select-one])
        select     (-> q
                       (get select-key)
                       (parse-select-clause db context depth))]
    (case select-key
      :select     (assoc q :select select)
      :select-one (assoc q :select-one select)
      :selectOne  (-> q
                      (dissoc :selectOne)
                      (assoc :select-one select)))))

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

(defn parse-having
  [q]
  (if-let [code (some-> q :having parse-code)]
    (assoc q :having (eval/compile code))
    q))

(defn parse-analytical-query*
  [q db]
  (let [context  (parse-context q db)
        vars     (parse-vars q)
        where    (parse-where q vars db context)
        grouping (parse-grouping q)
        ordering (parse-ordering q)]
    (-> q
        (assoc :context context
               :vars    vars
               :where   where)
        (cond-> grouping (assoc :group-by grouping)
                ordering (assoc :order-by ordering))
        parse-having
        (parse-select db context))))

(defn parse-analytical-query
  [q db]
  (let [parsed (parse-analytical-query* q db)]
    (or (re-parse-as-simple-subj-crawl parsed)
        parsed)))

(defn parse
  [q db]
  (syntax/validate q)
  (parse-analytical-query q db))

(defn parse-delete
  [q db]
  (when (:delete q)
    (let [context (parse-context q db)
          vars    (parse-vars q)
          where   (parse-where q vars db context)]
      (-> q
          (assoc :context context
                 :where   where
                 :vars    vars)
          (update :delete parse-triple db context)))))
