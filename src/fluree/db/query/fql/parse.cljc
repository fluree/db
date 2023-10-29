(ns fluree.db.query.fql.parse
  (:require [fluree.db.query.exec.eval :as eval]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.exec.update :as update]
            [fluree.db.query.exec.select :as select]
            [fluree.db.query.json-ld.select :refer [parse-subselection]]
            [fluree.db.datatype :as datatype]
            [fluree.db.query.fql.syntax :as syntax]
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
  (some-> x parse-var-name where/unmatched-var))

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

(def type-preds #{const/iri-type const/iri-rdf-type})

(defn type-pred?
  [p]
  (contains? type-preds p))

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

(defn parse-iri
  [x context]
  (-> x
      (json-ld/expand-iri context)
      where/->iri-ref))

(defn parse-class
  [o-iri context]
  (-> o-iri
      (json-ld/expand-iri context)
      where/->iri-ref))

(defmulti parse-pattern
  (fn [pattern _vars _context]
    (v/where-pattern-type pattern)))

(defn filter-pattern?
  [pattern]
  (-> pattern v/where-pattern-type (= :filter)))

(defn parse-filter-maps
  [vars filters]
  (let [vars (set vars)]
    (->> filters
         (mapcat rest)
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
  (let [clause*  (util/sequential clause)
        patterns (->> clause*
                      (remove filter-pattern?)
                      (mapcat (fn [pattern]
                                (parse-pattern pattern vars context))))
        filters  (->> clause*
                      (filter filter-pattern?)
                      (parse-filter-maps vars))]
    (where/->where-clause patterns filters)))

(defn expand-keys
  [m context]
  (reduce-kv (fn [expanded p o]
               (let [p* (if (v/variable? p)
                          p
                          (json-ld/expand-iri p context))]
                 (assoc expanded p* o)))
             {} m))

(defn parse-value-attributes
  [v attrs]
  (if-let [lang (get attrs const/iri-language)]
    (let [matcher (where/lang-matcher lang)]
      (where/->val-filter v matcher))
    (where/anonymous-value v)))

(defn generate-subject-var
  "Generate a unique subject variable"
  []
  (gensym "?s"))

(defn id-or-variable
  [id]
  (or id (generate-subject-var)))

(defn with-id
  [m]
  (update m const/iri-id id-or-variable))

(defn parse-subject
  [id context]
  (if (v/variable? id)
    (parse-variable id)
    (parse-iri id context)))

(defn parse-predicate
  [p]
  (if (v/variable? p)
    (parse-variable p)
    (where/->predicate p)))

(declare parse-statements)

(defn parse-statement*
  [s-mch p o context]
  (let [p-mch (parse-predicate p)]
    (if (map? o)
      (let [o* (expand-keys o context)]
        (if-let [v (get o* const/iri-value)]
          (let [attrs (dissoc o* const/iri-value)
                o-mch (parse-value-attributes v attrs)]
            [[s-mch p-mch o-mch]])
          (let [id-map  (with-id o*)
                o-mch   (-> id-map
                            (get const/iri-id)
                            (parse-subject context))
                o-attrs (dissoc id-map const/iri-id)]
            ;; return a thunk wrapping the recursive call to preserve stack
            ;; space by delaying execution
            #(into [[s-mch p-mch o-mch]]
                   (parse-statements o-mch o-attrs context)))))
      (if (v/variable? o)
        (let [o-mch (parse-variable o)]
          [[s-mch p-mch o-mch]])
        (if (-> p-mch ::where/iri type-pred?)
          (let [class-ref (parse-class o context)]
            [(where/->pattern :class [s-mch p-mch class-ref])])
          (let [o-mch (where/anonymous-value o)]
            [[s-mch p-mch o-mch]]))))))

(defn parse-statement
  [s-mch p o context]
  (trampoline parse-statement* s-mch p o context))

(defn parse-statements*
  [s-mch attrs context]
  #(mapcat (fn [[p o]]
             (parse-statement s-mch p o context))
           attrs))

(defn parse-statements
  [s-mch attrs context]
  (trampoline parse-statements* s-mch attrs context))

(defn parse-id-map-pattern
  [m context]
  (let [s-mch (-> m
                  (get const/iri-id)
                  (parse-subject context))
        attrs (dissoc m const/iri-id)]
    (parse-statements s-mch attrs context)))

(defn parse-node-map
  [m context]
  (-> m
      (expand-keys context)
      with-id
      (parse-id-map-pattern context)))

(defmethod parse-pattern :node
  [m _vars context]
  (parse-node-map m context))

(defmethod parse-pattern :union
  [[_ & union] vars context]
  (let [parsed (mapv (fn [clause]
                       (parse-where-clause clause vars context))
                     union)]
    [(where/->pattern :union parsed)]))

(defmethod parse-pattern :optional
  [[_ optional] vars context]
  (let [parsed (parse-where-clause optional vars context)]
    [(where/->pattern :optional parsed)]))

(defmethod parse-pattern :bind
  [[_ bind] _vars _context]
  (let [parsed  (parse-bind-map bind)]
    [(where/->pattern :bind parsed)]))

(defmethod parse-pattern :graph
  [[_ graph where] vars context]
  (let [graph* (or (parse-variable graph)
                   graph)
        where* (parse-where-clause where vars context)]
    [(where/->pattern :graph [graph* where*])]))

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
  (->> clause
       util/sequential
       (mapv (fn [m]
               (parse-node-map m context)))))

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

        delete (->> (util/get-first-value txn const/iri-delete)
                    util/sequential
                    (mapcat (fn [m]
                              (parse-node-map m context))))
        insert (->> (util/get-first-value txn const/iri-insert)
                    util/sequential
                    (mapcat (fn [m]
                              (parse-node-map m context))))]
    (cond-> {}
      context            (assoc :context context)
      where              (assoc :where where)
      (seq values)       (assoc :values values)
      (not-empty delete) (assoc :delete delete)
      (not-empty insert) (assoc :insert insert))))
