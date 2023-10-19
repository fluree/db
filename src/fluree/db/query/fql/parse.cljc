(ns fluree.db.query.fql.parse
  (:require [fluree.db.query.exec.eval :as eval]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.exec.update :as update]
            [fluree.db.query.exec.select :as select]
            [fluree.db.query.json-ld.select :refer [parse-subselection]]
            [fluree.db.datatype :as datatype]
            [fluree.db.query.fql.syntax :as syntax]
            [clojure.string :as str]
            [clojure.set :as set]
            [clojure.walk :refer [postwalk]]
            [fluree.json-ld :as json-ld]
            [fluree.db.util.core :as util :refer [try* catch* get-first-value]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.validation :as v]
            [fluree.db.constants :as const]
            #?(:cljs [cljs.reader :refer [read-string]])))

#?(:clj (set! *warn-on-reflection* true))

(defn parse-var-name
  "Returns a `x` as a symbol if `x` is a valid '?variable'."
  [x]
  (when (v/variable? x)
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
          vars*     (keep parse-var-name (util/sequential vars))
          vals*     (mapv util/sequential vals)
          var-count (count vars*)]
      (if (every? (fn [binding]
                    (= (count binding) var-count))
                  vals*)
        [vars* (mapv (partial parse-value-binding vars*)
                     vals*)]
        (throw (ex-info (str "Invalid value binding: "
                             "number of variables and values don't match: "
                             values)
                        {:status 400 :error :db/invalid-query}))))))

(def rdf-type-preds #{"a"
                      :a
                      :type
                      const/iri-type
                      "rdf:type"
                      :rdf/type
                      const/iri-rdf-type})

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
  "Evals and returns filter function."
  [fltr vars]
  (let [code      (parse-code fltr)
        code-vars (or (not-empty (variables code))
                      (throw (ex-info (str "Filter function must contain a valid variable. Provided: " code)
                                      {:status 400 :error :db/invalid-query})))
        var-name  (find-filtered-var code-vars vars)
        f         (eval/compile-filter code var-name)]
    (where/->var-filter var-name f)))

(defn parse-bind-function
  "Evals and returns bind function."
  [var-name fn-code]
  (let [code (parse-code fn-code)
        f    (eval/compile code false)]
    (where/->var-filter var-name f)))

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
      where/->iri-ref))

(defn parse-subject-pattern
  [s-pat context]
  (when s-pat
    (or (parse-variable s-pat)
        (parse-pred-ident s-pat)
        (parse-iri s-pat context)
        (throw (ex-info (str "Subject values in where statement must be integer subject IDs or two-tuple identies. "
                             "Provided: " s-pat ".")
                        {:status 400 :error :db/invalid-query})))))

(defn parse-predicate-iri
  [p context]
  (let [iri (json-ld/expand-iri p context)]
    (where/->predicate iri)))

(defn parse-predicate-pattern
  [p-pat context]
  (or (parse-variable p-pat)
      (parse-predicate-iri p-pat context)))

(defn parse-class
  [o-iri context]
  (-> o-iri
      (json-ld/expand-iri context)
      where/->iri-ref))

(defn parse-object-iri
  [x context]
  (-> x
      (json-ld/expand-iri context)
      where/anonymous-value))

(defn iri-map?
  [m]
  (and (contains? m :id)
       (= (count m) 2))) ; account for :idx key in expanded maps

(defn parse-iri-map
  [m]
  (when (iri-map? m)
    (-> m
        (get :id)
        where/->iri-ref)))

(defn parse-value-map
  [m]
  (when-let [v (get-first-value m :value)]
    (if-let [lang (get-first-value m :language)]
      (let [lang-filter (fn [mch]
                          (-> mch ::where/meta :lang (= lang)))]
        (where/->val-filter v lang-filter))
      (where/anonymous-value v))))

(defn parse-reference-map
  [pat context]
  (when (map? pat)
    (let [expanded (json-ld/expand pat context)]
      (or (parse-iri-map expanded)
          (parse-value-map expanded)))))

(defn parse-object-pattern
  [o-pat context]
  (or (parse-variable o-pat)
      (parse-pred-ident o-pat)
      (parse-reference-map o-pat context)
      (where/anonymous-value o-pat)))

(defmulti parse-pattern
  (fn [pattern _vars _context]
    (if (map? pattern)
      (if (contains? pattern :graph)
        :graph
        (->> pattern keys first))
      (if (map-entry? pattern)
        :binding
        :triple))))

(defn type-pattern?
  [typ x]
  (and (map? x)
       (-> x keys first (= typ))))

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
  (reduce-kv (fn [m k v]
               (let [parsed-k (parse-var-name k)]
                 (assoc m parsed-k (parse-bind-function parsed-k v))))
             {} bind))

(defn parse-where-clause
  [clause vars context]
  (let [patterns (->> clause
                      (remove filter-pattern?)
                      (mapv (fn [pattern]
                              (parse-pattern pattern vars context))))
        filters  (->> clause
                      (filter filter-pattern?)
                      (parse-filter-maps vars))]
    (where/->where-clause patterns filters)))

(defn parse-triple
  [[s-pat p-pat o-pat] context]
  (let [s (parse-subject-pattern s-pat context)
        p (parse-predicate-pattern p-pat context)]
    (if (and (#{const/iri-type const/iri-rdf-type} (::where/iri p))
             (not (v/variable? o-pat)))
      (let [class-ref (parse-class o-pat context)]
        (where/->pattern :class [s p class-ref]))
      (if (= const/iri-id (::where/iri p))
        (let [o (parse-object-iri o-pat context)]
          [s p o])
        (let [o (parse-object-pattern o-pat context)]
          [s p o])))))

(defmethod parse-pattern :triple
  [triple _ context]
  (parse-triple triple context))

(defmethod parse-pattern :union
  [{:keys [union]} vars context]
  (let [parsed (mapv (fn [clause]
                       (parse-where-clause clause vars context))
                     union)]
    (where/->pattern :union parsed)))

(defmethod parse-pattern :optional
  [{:keys [optional]} vars context]
  (let [clause (if (coll? (first optional))
                 optional
                 [optional])
        parsed (parse-where-clause clause vars context)]
    (where/->pattern :optional parsed)))

(defmethod parse-pattern :bind
  [{:keys [bind]} _vars _context]
  (let [parsed  (parse-bind-map bind)
        pattern (where/->pattern :bind parsed)]
    pattern))

(defmethod parse-pattern :binding
  [[v f] _vars _context]
  (where/->pattern :binding [v f]))

(defmethod parse-pattern :graph
  [{:keys [graph where]} vars context]
  (let [graph* (or (parse-variable graph)
                   (json-ld/expand-iri graph context))
        where* (parse-where-clause where vars context)]
    (where/->pattern :graph [graph* where*])))

(defn parse-where
  [q vars context]
  (when-let [where (:where q)]
    (parse-where-clause where vars context)))

(defn parse-as-fn
  [f]
  (let [parsed-fn  (parse-code f)
        fn-name    (some-> parsed-fn second first)
        bind-var   (last parsed-fn)
        aggregate? (when fn-name (eval/allowed-aggregate-fns fn-name))]
    (-> parsed-fn
        eval/compile
        (select/as-selector bind-var aggregate?))))

(defn parse-fn
  [f]
  (-> f parse-code eval/compile select/aggregate-selector))

(defn parse-select-map
  [sm context depth]
  (log/trace "parse-select-map:" sm)
  (let [{:keys [variable selection depth spec]} (parse-subselection context sm depth)]
    (select/subgraph-selector variable selection depth spec)))

(defn parse-selector
  [context depth s]
  (let [[selector-type selector-val] (syntax/parse-selector s)]
    (case selector-type
      :wildcard select/wildcard-selector
      :var (-> selector-val symbol select/variable-selector)
      :aggregate (case (first selector-val)
                   :string-fn (if (re-find #"^\(as " s)
                                (parse-as-fn s)
                                (parse-fn s))
                   :list-fn (if (= 'as (first s))
                              (parse-as-fn s)
                              (parse-fn s)))
      :select-map (parse-select-map s context depth))))

(defn parse-select-clause
  [clause context depth]
  (if (sequential? clause)
    (mapv (partial parse-selector context depth)
          clause)
    (parse-selector context depth clause)))

(defn parse-select
  [q context]
  (let [depth      (or (:depth q) 0)
        select-key (some (fn [k]
                           (when (contains? q k) k))
                         [:select :selectOne :select-one
                          :selectDistinct :select-distinct])
        select     (-> q
                       (get select-key)
                       (parse-select-clause context depth))]
    (case select-key
      (:select
       :select-one
       :select-distinct) (assoc q select-key select)

      :selectOne (-> q
                     (dissoc :selectOne)
                     (assoc :select-one select))

      :selectDistinct (-> q
                          (dissoc :selectDistinct)
                          (assoc :select-distinct select)))))

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
                           v (parse-var-name dim)]
                       (if (syntax/asc? dir)
                         [v :asc]
                         [v :desc])))))))

(defn parse-having
  [q]
  (if-let [code (some-> q :having parse-code)]
    (assoc q :having (eval/compile code))
    q))

(defn parse-fuel
  [{:keys [opts] :as q}]
  (if-let [max-fuel (or (:max-fuel opts) (:maxFuel opts))]
    (assoc q :fuel max-fuel)
    q))

(defn parse-analytical-query
  [q context]
  (let [[vars values] (parse-values q)
        where    (parse-where q vars context)
        grouping (parse-grouping q)
        ordering (parse-ordering q)]
    (-> q
        (assoc :context context
               :where where)
        (cond-> (seq values) (assoc :values values)
                grouping (assoc :group-by grouping)
                ordering (assoc :order-by ordering))
        parse-having
        (parse-select context)
        parse-fuel)))

(defn parse-query
  [q context]
  (log/trace "parse-query" q)
  (-> q
      syntax/coerce-query
      (parse-analytical-query context)))

(defn parse-update-clause
  [clause context]
  (let [clause* (if (syntax/triple? clause)
                  [clause]
                  clause)]
    (mapv (fn [trip]
            (parse-triple trip context))
          clause*)))

(defn parse-ledger-update
  [mdfn context]
  (let [[vars values] (parse-values mdfn)
        where   (parse-where mdfn vars context)]
    (-> mdfn
        (assoc :context context
               :where where)
        (cond-> (seq values) (assoc :values values))
        (as-> mod
            (if (update/retract? mod)
              (update mod :delete parse-update-clause context)
              mod))
        (as-> mod
            (if (update/insert? mod)
              (update mod :insert parse-update-clause context)
              mod)))))

(defn parse-modification
  [json-ld context]
  (-> json-ld
      syntax/coerce-modification
      (parse-ledger-update context)))

(defn temp-bnode-id
  "Generate a temporary bnode id. This will get replaced during flake creation when a sid is generated."
  [bnode-counter]
  (str "_:fdb" (vswap! bnode-counter inc)))

(declare parse-subj-cmp)
(defn parse-obj-cmp
  [bnode-counter subj-cmp pred-cmp m triples {:keys [list id value type language] :as v-map}]
  (cond list
        (reduce (fn [triples [i list-item]]
                  (parse-obj-cmp bnode-counter subj-cmp pred-cmp {:i i} triples list-item))
                triples
                (map vector (range) list))

        (some? value)
        (let [obj-cmp (if (v/variable? value)
                        (parse-variable value)
                        (cond-> {::where/val value}
                          (or m language) (assoc ::where/m (cond-> m language (assoc :lang language)))
                          type (assoc ::where/datatype type)))]
          (conj triples [subj-cmp pred-cmp obj-cmp]))

        :else
        (let [ref-cmp (if (nil? id)
                        {::where/val (temp-bnode-id bnode-counter) ::where/datatype const/iri-id}
                        (cond-> {::where/val id ::where/datatype const/iri-id}
                          m (assoc ::where/m m)))
              v-map* (if (nil? id)
                       ;; project newly created bnode-id into v-map
                       (assoc v-map :id (::where/val ref-cmp))
                       v-map)]
          (conj (parse-subj-cmp bnode-counter triples v-map*) [subj-cmp pred-cmp ref-cmp]))))

(defn parse-pred-cmp
  [bnode-counter subj-cmp triples [pred values]]
  (let [values*  (if (= pred :type)
                   ;; homogenize @type values so they have the same structure as other predicates
                   (map #(do {:id %}) values)
                   values)
        pred-cmp (cond (v/variable? pred) (parse-variable pred)
                       ;; we want the actual iri here, not the keyword
                       (= pred :type)     {::where/val const/iri-type}
                       :else              {::where/val pred})]
    (reduce (partial parse-obj-cmp bnode-counter subj-cmp pred-cmp nil)
            triples
            values*)))

(defn parse-subj-cmp
  [bnode-counter triples {:keys [id] :as node}]
  (let [subj-cmp (cond (nil? id) {::where/val (temp-bnode-id bnode-counter)}
                       (v/variable? id) (parse-variable id)
                       :else {::where/val id})]
    (reduce (partial parse-pred-cmp bnode-counter subj-cmp)
            triples
            (dissoc node :id :idx))))

(defn parse-triples
  "Flattens and parses expanded json-ld into update triples."
  [expanded]
  (let [bnode-counter (volatile! 0)]
    (reduce (partial parse-subj-cmp bnode-counter)
            []
            expanded)))

(defn parse-txn
  [txn context]
  (let [[vars values] (parse-values {:values (util/get-first-value txn const/iri-values)})
        where         (parse-where {:where (util/get-first-value txn const/iri-where)} vars context)

        delete (-> (util/get-first-value txn const/iri-delete)
                   (json-ld/expand context)
                   (util/sequential)
                   (parse-triples))
        insert (-> (util/get-first-value txn const/iri-insert)
                   (json-ld/expand context)
                   (util/sequential)
                   (parse-triples))]
    (cond-> {}
      context            (assoc :context context)
      where              (assoc :where where)
      (seq values)       (assoc :values values)
      (not-empty delete) (assoc :delete delete)
      (not-empty insert) (assoc :insert insert))))
