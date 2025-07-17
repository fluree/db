(ns fluree.db.query.fql.parse
  (:require #?(:cljs [cljs.reader :refer [read-string]])
            [clojure.set :as set]
            [clojure.walk :refer [postwalk]]
            [fluree.db.constants :as const]
            [fluree.db.datatype :as datatype]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.query.exec.eval :as eval]
            [fluree.db.query.exec.select :as select]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.fql.syntax :as syntax]
            [fluree.db.query.optimize :as optimize]
            [fluree.db.query.sparql :as sparql]
            [fluree.db.query.sparql.translator :as sparql.translator]
            [fluree.db.query.turtle.parse :as turtle]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.context :as ctx-util]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.util.parse :as util.parse]
            [fluree.db.validation :as v]
            [fluree.json-ld :as json-ld]))

#?(:clj (set! *warn-on-reflection* true))

(defn parse-var-name
  "Returns x as a symbol if x is a valid variable, or nil otherwise. A valid
  variable is a string, symbol, or keyword whose name starts with '?'."
  [x]
  (when (v/query-variable? x)
    (symbol x)))

(defn parse-variable
  [x]
  (some-> x parse-var-name where/unmatched-var))

(defn- parse-variable-if-allowed
  [allowed-vars x]
  (if (->> x symbol (contains? allowed-vars))
    (parse-variable x)
    (throw
     (ex-info (str "variable " x " is not bound in where nor values clause")
              {:status 400, :error :db/invalid-transaction}))))

(defn expand-keys
  [m context]
  (reduce-kv (fn [expanded p o]
               (let [p* (if (v/variable? p)
                          p
                          (json-ld/expand-iri p context))]
                 (assoc expanded p* o)))
             {} m))

(defn get-type
  [attrs]
  (when-let [dt (get attrs const/iri-type)]
    (if (contains? attrs const/iri-language)
      (throw (ex-info "Language tags are not allowed when the data type is specified."
                      {:status 400, :error :db/invalid-query}))
      dt)))

(defn get-lang
  [attrs]
  (when-let [lang (get attrs const/iri-language)]
    (if (contains? attrs const/iri-type)
      (throw (ex-info "Language tags are not allowed when the data type is specified."
                      {:status 400, :error :db/invalid-query}))
      lang)))

(defn parse-value-datatype
  [v attrs context]
  (if-let [dt (get-type attrs)]
    (if (v/variable? dt)
      (-> v where/untyped-value (where/link-dt-var dt))
      (let [dt-iri (json-ld/expand-iri dt context)
            dt-sid (iri/iri->sid dt-iri)
            v*     (datatype/coerce-value v dt-sid)]
        (if (= const/iri-id dt-iri)
          (let [expanded (json-ld/expand-iri v* context)]
            (where/match-iri where/unmatched expanded))
          (where/anonymous-value v* dt-iri))))
    (if-let [lang (get-lang attrs)]
      (if (v/variable? lang)
        (-> where/unmatched
            (where/match-value v const/iri-lang-string)
            (where/link-lang-var lang))
        (where/match-lang where/unmatched v lang))
      (where/anonymous-value v))))

(defn every-binary-pred
  [& fs]
  (fn [x y]
    (every? (fn [f]
              (f x y))
            fs)))

(defn combine-filters
  [& fs]
  (some->> fs
           (remove nil?)
           not-empty
           (apply every-binary-pred)))

(defn parse-value-attributes
  [v attrs context]
  (let [mch          (parse-value-datatype v attrs context)
        t-matcher    (some-> attrs (get const/iri-t) (where/transaction-matcher))
        dt-matcher   (some-> attrs (get const/iri-type) (where/datatype-matcher context))
        lang-matcher (some-> attrs (get const/iri-language) where/lang-matcher)]
    (if-let [f (combine-filters t-matcher lang-matcher dt-matcher)]
      (where/with-filter mch f)
      mch)))

(defn get-expanded-datatype
  [attrs context]
  (some-> attrs
          (get const/iri-type)
          (json-ld/expand-iri context)))

(defn match-value-binding-map
  [var-match binding-map context]
  (let [attrs (expand-keys binding-map context)
        {val const/iri-value} attrs]
    (if-let [dt-iri (get-expanded-datatype attrs context)]
      (if (= const/iri-id dt-iri)
        (let [expanded (json-ld/expand-iri val context)]
          (where/match-iri var-match expanded))
        (let [dt-id (get datatype/default-data-types dt-iri)
              val*  (datatype/coerce-value val dt-id)]
          (where/match-value var-match val* dt-iri)))
      (if-let [lang (get attrs const/iri-language)]
        (where/match-lang var-match val lang)
        (let [dt (datatype/infer-iri val)]
          (where/match-value var-match val dt))))))

(defn match-value-binding
  [var-match value context]
  (if (map? value)
    (match-value-binding-map var-match value context)
    (let [dt (datatype/infer-iri value)]
      (where/match-value var-match value dt))))

(defn parse-value-binding
  [vars vals context]
  (let [var-matches (map parse-variable vars)
        binding     (map (fn [var-match val]
                           (match-value-binding var-match val context))
                         var-matches vals)]
    (zipmap vars binding)))

(defn parse-values
  [values context]
  (when values
    (let [[vars vals] (util.parse/normalize-values values)
          parsed-vars (keep parse-var-name vars)
          var-count   (count vars)]
      (if (every? (fn [binding] (= (count binding) var-count))
                  vals)
        [parsed-vars (mapv (fn [binding] (parse-value-binding parsed-vars binding context))
                           vals)]
        (throw (ex-info (str "Invalid value binding: number of variables and values don't match: "
                             (pr-str values))
                        {:status 400 :error :db/invalid-query}))))))

(def type-pred-iris #{const/iri-type const/iri-rdf-type})

(defn type-pred-match?
  [p-mch]
  (let [p-iri (where/get-iri p-mch)]
    (contains? type-pred-iris p-iri)))

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
      0 (throw (ex-info (str "Variable filter function has no variable assigned to it, all parameters "
                             "exist in the 'values' clause. Filter function params: " params ". "
                             "Values assigned in query: " vars ".")
                        {:status 400
                         :error  :db/invalid-query}))
      (throw (ex-info (str "Vars used in a filter function are not included in the 'values' clause "
                           "or as a binding. Should only be missing one var, but missing: " (vec non-assigned-vars) ".")
                      {:status 400
                       :error  :db/invalid-query})))))

(defn parse-code-data
  [x]
  (cond
    ;; special handling for "in" expressions
    (and (vector? x)
         (= "in" (first x)))
    (let [[f v set] x]
      (list (parse-code-data f)
            (parse-code-data v)
            ;; need to preserve vector as set literal notation
            (mapv parse-code-data set)))

    (sequential? x)
    (map parse-code-data x)

    (v/variable? x)
    (parse-var-name x)

    (and (symbol? x)
         (contains? eval/allowed-symbols x))
    x

    (string? x)
    (if (contains? eval/allowed-symbols (symbol x))
      (symbol x)
      x)

    :else
    x))

(defn parse-code
  [x]
  (cond (list? x)   x
        (vector? x) (parse-code-data (second x))
        :else       (safe-read x)))

(defn parse-filter-function
  "Evals and returns filter function."
  [fltr fltr-var vars ctx]
  (let [code      (parse-code fltr)
        code-vars (or (not-empty (variables code))
                      (throw (ex-info (str "Filter function must contain a valid variable. Provided: " code)
                                      {:status 400 :error :db/invalid-query})))
        var-name  (find-filtered-var code-vars vars)]
    (if (= var-name fltr-var)
      (eval/compile-filter code var-name ctx)
      (throw (ex-info (str "Variable filter must only reference the variable bound in its value map: "
                           fltr-var
                           ". Provided:" code)
                      {:status 400, :error :db/invalid-query})))))

(defn parse-bind-function
  "Evals and returns bind function."
  [var-name fn-code context]
  (let [code (parse-code fn-code)
        f    (eval/compile code context false)]
    (where/->var-filter var-name f)))

(defn parse-static-bind
  [var-name static-value context]
  (let [mch (where/unmatched-var var-name)
        v   (get static-value const/iri-value)]
    (if (some? v)
      ;; value map
      (if-let [dt (get static-value const/iri-type)]
        (where/match-value mch v (json-ld/expand-iri dt context))
        (if-let [lang (get static-value const/iri-language)]
          (where/match-lang mch v lang)
          (where/match-value mch v)))
      (if-let [iri (get static-value const/iri-id)]
        ;; id map
        (where/match-iri mch (json-ld/expand-iri iri context))
        ;; literal value
        (where/match-value mch static-value (datatype/infer-iri static-value))))))

(defn parse-bind-expression
  [var-name expression context]
  (if (syntax/function? expression)
    (parse-bind-function var-name expression context)
    (parse-static-bind var-name expression context)))

(defn parse-subject-iri
  [x context]
  (-> x
      (json-ld/expand-iri context false)
      where/->iri-ref))

(defn parse-class
  [o-iri context]
  (-> o-iri
      (json-ld/expand-iri context)
      where/->iri-ref))

(defmulti parse-pattern
  (fn [pattern _vars _context]
    (v/where-pattern-type pattern)))

(defn parse-bind-map
  [binds context]
  (into {}
        (comp (partition-all 2)
              (map (fn [[k v]]
                     (let [var     (parse-var-name k)
                           binding (parse-bind-expression var v context)]
                       [var binding]))))
        binds))

(defn higher-order-pattern?
  "A non-node where pattern."
  [pattern]
  (and (sequential? pattern) (keyword? (first pattern))))

(defn parse-where-clause
  [clause vars context]
  ;; a single higher-order where pattern is already sequential, so we need to check if it needs wrapping
  (let [clause* (if (higher-order-pattern? clause)
                  [clause]
                  (util/sequential clause))]
    (->> clause*
         (mapcat (fn [pattern]
                   (parse-pattern pattern vars context)))
         where/->where-clause)))

(defn parse-variable-attributes
  [var attrs vars context]
  (if (and (contains? attrs const/iri-type)
           (contains? attrs const/iri-language))
    (throw (ex-info "Language tags are not allowed when the data type is specified."
                    {:status 400, :error :db/invalid-query}))
    (let [var-mch      (where/unmatched-var var)
          t            (get attrs const/iri-t)
          t-matcher    (some-> t (where/transaction-matcher))
          dt           (get attrs const/iri-type)
          dt-matcher   (some-> dt (where/datatype-matcher context))
          lang         (get attrs const/iri-language)
          lang-matcher (some-> lang where/lang-matcher)
          filter-fn    (some-> attrs
                               (get const/iri-filter)
                               (parse-filter-function var vars context))
          filters (cond->> [filter-fn]
                    (not (v/variable? t)) (cons t-matcher)
                    (not (v/variable? dt)) (cons dt-matcher)
                    (not (v/variable? lang)) (cons lang-matcher))
          f       (apply combine-filters filters)]
      (cond-> var-mch
        (v/variable? dt)   (where/link-dt-var dt)
        (v/variable? lang) (where/link-lang-var lang)
        (v/variable? t)    (where/link-t-var t)
        f                  (where/with-filter f)))))

(defn generate-subject-var
  "Generate a unique subject variable"
  []
  (gensym "?s"))

(defn id-or-variable
  [id]
  (or id (generate-subject-var)))

(defn with-id
  "Searches for the id key, expands it or adds a variable as a value."
  [m context]
  (let [[id-key id] (reduce-kv (fn [_res p o]
                                 (let [expanded (if (v/variable? p)
                                                  p
                                                  (json-ld/expand-iri p context))]
                                   (when (= const/iri-id expanded)
                                     (reduced [p o]))))
                               nil m)]
    (-> m
        (dissoc id-key)
        (assoc const/iri-id id)
        (update const/iri-id id-or-variable))))

(defn parse-subject
  [id context]
  (if (v/query-variable? id)
    (parse-variable id)
    (parse-subject-iri id context)))

(declare parse-predicate)
(defn parse-property-path
  [path context]
  (let [path-expr  (subs path 1 (dec (count path))) ; remove angle brackets
        ;; TODO: the parsing is slower than it needs to be
        [pred mod] (->>
                     ;; parse to validate
                    (first (sparql/parse-path-expr path-expr))
                     ;; translate back to string
                    (sparql.translator/parse-term)
                     ;; separate recursion modifier
                    (#(split-at (dec (count %)) %))
                     ;; turn back into strings
                    (map (partial apply str)))
        recur-mod   ({"+" :one+ "*" :zero+} mod)]
    (cond-> (parse-predicate pred context)
      recur-mod (where/add-transitivity recur-mod))))

(defn parse-predicate
  [p context]
  (cond (v/variable? p)
        (parse-variable p)

        (v/property-path? p)
        (parse-property-path p context)

        :else
        (let [[expanded {reverse :reverse}] (json-ld/details p context)]
          (if (contains? type-pred-iris expanded)
            (where/->predicate const/iri-rdf-type reverse)
            (where/->predicate expanded reverse)))))

(declare parse-statement parse-statements)

(defn flip-reverse-pattern
  [[s-mch p-mch o-mch :as pattern]]
  (if (where/get-reverse p-mch)
    [o-mch p-mch s-mch]
    pattern))

(defn parse-object-map
  [s-mch p-mch o vars context]
  (let [o* (expand-keys o context)]
    (if-let [v (get o* const/iri-value)]
      (let [attrs (dissoc o* const/iri-value)
            o-mch (if-let [var (parse-var-name v)]
                    (parse-variable-attributes var attrs vars context)
                    (parse-value-attributes v attrs context))]
        [(flip-reverse-pattern [s-mch p-mch o-mch])])
      ;; ref
      (let [id-map  (with-id o context) ; not o*, we can't use expanded or we'll lose @reverse
            o-mch   (-> id-map
                        (get const/iri-id)
                        (parse-subject context))
            o-attrs (dissoc id-map const/iri-id)]
        ;; return a thunk wrapping the recursive call to preserve stack
        ;; space by delaying execution
        #(into [(flip-reverse-pattern [s-mch p-mch o-mch])]
               (parse-statements o-mch o-attrs vars context))))))

(defn parse-statement*
  [s-mch p-mch o vars context]
  (cond
    (v/query-variable? o)
    (let [o-mch (parse-variable o)]
      [(flip-reverse-pattern [s-mch p-mch o-mch])])

    (map? o)
    (parse-object-map s-mch p-mch o vars context)

    (sequential? o)
    #(mapcat (fn [o*]
               (parse-statement s-mch p-mch o* vars context))
             o)

    (type-pred-match? p-mch)
    (let [class-ref (parse-class o context)]
      [(where/->pattern :class (flip-reverse-pattern [s-mch p-mch class-ref]))])

    :else
    (let [o-mch (where/anonymous-value o)]
      [(flip-reverse-pattern [s-mch p-mch o-mch])])))

(defn parse-statement
  [s-mch p-mch o vars context]
  (trampoline parse-statement* s-mch p-mch o vars context))

(defn parse-statements*
  [s-mch attrs vars context]
  #(mapcat (fn [[p o]]
             (let [p-mch (parse-predicate p context)]
               (parse-statement s-mch p-mch o vars context)))
           attrs))

(defn parse-statements
  [s-mch attrs vars context]
  (trampoline parse-statements* s-mch attrs vars context))

(defn parse-id-map-pattern
  [m vars context]
  (let [s-mch (-> m
                  (get const/iri-id)
                  (parse-subject context))
        attrs (dissoc m const/iri-id)]
    (if (empty? attrs)
      [(where/->pattern :id s-mch)]
      (let [statements (parse-statements s-mch attrs vars context)]
        (sort optimize/compare-triples statements)))))

(defn parse-node-map
  [m vars context]
  (-> m
      (with-id context)
      (parse-id-map-pattern vars context)))

(defmethod parse-pattern :node
  [m vars context]
  (parse-node-map m vars context))

(defmethod parse-pattern :filter
  [[_ & codes] _vars context]
  (let [f (->> codes
               (map parse-code)
               (map (fn [code] (comp (fn [tv] (:value tv))
                                     (eval/compile code context))))
               (apply every-pred))]
    [(where/->pattern :filter (with-meta f {:fns codes}))]))

(defmethod parse-pattern :union
  [[_ & unions] vars context]
  (let [parsed (mapv (fn [clause] (parse-where-clause clause vars context))
                     unions)]
    [(where/->pattern :union parsed)]))

(defmethod parse-pattern :optional
  [[_ & optionals] vars context]
  (into []
        (comp (map (fn [clause] (parse-where-clause clause vars context)))
              (map (partial where/->pattern :optional)))
        optionals))

(defmethod parse-pattern :bind
  [[_ & binds] _vars context]
  (let [parsed (parse-bind-map binds context)]
    [(where/->pattern :bind parsed)]))

(defmethod parse-pattern :values
  [[_ values] _vars context]
  (let [[_vars solutions] (parse-values values context)]
    [(where/->pattern :values solutions)]))

(defmethod parse-pattern :exists
  [[_ patterns] vars context]
  [(where/->pattern :exists (parse-where-clause patterns vars context))])

(defmethod parse-pattern :not-exists
  [[_ patterns] vars context]
  [(where/->pattern :not-exists (parse-where-clause patterns vars context))])

(defmethod parse-pattern :minus
  [[_ patterns] vars context]
  [(where/->pattern :minus (parse-where-clause patterns vars context))])

;; TODO: This function is only necessary because ledger aliases might not be
;; valid IRIs but virtual graph aliases are. We should require that all ledger
;; aliases/graph names be IRIs.
(defn parse-graph-string
  [graph context]
  (when (string? graph)
    (let [expanded (json-ld/expand-iri graph context)]
      (if (where/virtual-graph? expanded)
        expanded
        graph))))

(defmethod parse-pattern :graph
  [[_ graph where] vars context]
  (let [graph* (or (parse-variable graph)
                   (parse-graph-string graph context))
        where* (parse-where-clause where vars context)]
    [(where/->pattern :graph [graph* where*])]))

(defn parse-where
  [where vars context]
  (when where
    (-> where
        syntax/coerce-where
        (parse-where-clause vars context))))

(defn unwrap-tuple-patterns
  "Construct accepts ::v/node-map patterns, which can produce :tuple patterns, :class
  patterns, or :id patterns. We only need the pattern components as a template for
  construct, the :id and :class patterns are for optimized query execution, so this
  function unwraps :id and :class patterns and only returns the underlying components."
  [patterns]
  (mapv (fn [[pattern-type component :as pattern]]
          (case pattern-type
            :class component
            :id    [component]
            pattern))
        patterns))

(defn parse-construct
  [q context]
  (when-let [construct (:construct q)]
    (-> construct
        syntax/coerce-where
        (parse-where-clause nil context)
        unwrap-tuple-patterns
        select/construct-selector)))

(defn parse-select-as-fn
  [f context output]
  (let [parsed-fn  (parse-code f)
        fn-name    (some-> parsed-fn second first)
        bind-var   (last parsed-fn)
        aggregate? (when fn-name (eval/allowed-aggregate-fns fn-name))]
    (-> parsed-fn
        (eval/compile context)
        (select/as-selector output bind-var aggregate?))))

(defn parse-select-aggregate
  [f context]
  (-> f parse-code (eval/compile context) select/aggregate-selector))

(defn reverse?
  [context k]
  (-> context
      (get-in [k :reverse])
      boolean))

(defn expand-selection
  [selection depth context]
  (reduce
   (fn [acc select-item]
     (cond
       (map? select-item)
       (let [[k v]  (first select-item)
             iri    (json-ld/expand-iri k context)
             spec   {:iri iri}
             depth* (if (zero? depth)
                      0
                      (dec depth))
             spec*  (-> spec
                        (assoc :spec (expand-selection v depth* context)
                               :as k))]
         (if (reverse? context k)
           (assoc-in acc [:reverse iri] spec*)
           (assoc acc iri spec*)))

       (#{"*" :* '*} select-item)
       (assoc acc :wildcard? true)

       :else
       (let [iri  (json-ld/expand-iri select-item context)
             spec {:iri iri, :as select-item}]
         (if (reverse? context select-item)
           (assoc-in acc [:reverse iri] spec)
           (assoc acc iri spec)))))
   {:depth depth} selection))

(defn parse-select-map
  [sm depth context output]
  (log/trace "parse-select-map:" sm)
  (if (= output :fql)
    (let [[subj selection] (first sm)
          spec             (expand-selection selection depth context)]
      (if (v/variable? subj)
        (let [var (parse-var-name subj)]
          (select/subgraph-selector var selection depth spec))
        (let [iri (json-ld/expand-iri subj context false)]
          (select/subgraph-selector iri selection depth spec))))
    (throw (ex-info "Can only use subgraph selector with JSON-LD Query output formatting."
                    {:status 400 :error :db/invalid-select}))))

(defn parse-selector
  [context depth output s]
  (if (syntax/wildcard? s)
    (select/wildcard-selector output)
    (let [[selector-type selector-val] (syntax/parse-selector s)]
      (case selector-type
        :var (-> selector-val symbol (select/variable-selector output))
        :aggregate (case (first selector-val)
                     :string-fn (if (re-find #"^\(as " s)
                                  (parse-select-as-fn s context output)
                                  (parse-select-aggregate s context))
                     :list-fn (if (= 'as (first s))
                                (parse-select-as-fn s context output)
                                (parse-select-aggregate s context))
                     :vector-fn (if (= "as" (first s))
                                  (parse-select-as-fn s context output)
                                  (parse-select-aggregate s context)))
        :select-map (parse-select-map s depth context output)))))

(defn parse-select-clause
  [clause context output depth]
  (cond
    ;; singular function call
    (list? clause)
    (parse-selector context depth output clause)

    ;; collection of selectors
    (sequential? clause)
    (mapv (partial parse-selector context depth output)
          clause)

    ;; singular selector
    :else
    (parse-selector context depth output clause)))

(defn parse-select
  [q context]
  (if-let [select-key (some (fn [k] (when (contains? q k) k))
                            [:select :select-one :select-distinct])]
    (let [depth  (or (:depth q) 0)
          output (or (-> q :opts :output) :fql)
          select (-> q
                     (get select-key)
                     (parse-select-clause context output depth))]
      (assoc q select-key select))
    q))

(defn ensure-vector
  [x]
  (if (vector? x)
    x
    [x]))

(defn parse-grouping
  [q]
  (some->> (:group-by q)
           ensure-vector
           (mapv parse-var-name)))

(defn parse-ordering
  [q]
  (some->> (:order-by q)
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
  [q context]
  (if-let [code (some-> q :having parse-code)]
    (assoc q :having (eval/compile code context))
    q))

(defn parse-fuel
  [{:keys [opts] :as q}]
  (if-let [max-fuel (:max-fuel opts)]
    (assoc q :fuel max-fuel)
    q))

(defn get-named
  "Get the value from the map `m` associated with the key with name `nme`. This
  key could be a string, keyword, or symbol."
  [m nme]
  (or (get m nme)
      (get m (keyword nme))
      (get m (symbol nme))))

(defn parse-query*
  ([q] (parse-query* q nil))
  ([q parent-context]
   (let [orig-context  (:context q)
         context       (cond->> (json-ld/parse-context orig-context)
                         parent-context (merge parent-context))
         [vars values] (parse-values (:values q) context)
         where         (parse-where (:where q) vars context)
         construct     (parse-construct q context)
         grouping      (parse-grouping q)
         ordering      (parse-ordering q)]
     (-> q
         (assoc :context context
                :where where)
         (cond-> (seq values) (assoc :values values)
                 orig-context (assoc :orig-context orig-context)
                 grouping  (assoc :group-by grouping)
                 ordering  (assoc :order-by ordering)
                 construct (assoc :construct construct))
         (parse-having context)
         (parse-select context)
         parse-fuel))))

(defmethod parse-pattern :query
  [[_ sub-query] _vars context]
  (let [sub-query* (-> sub-query
                       syntax/coerce-subquery
                       (parse-query* context))]
    [(where/->pattern :query sub-query*)]))

(defn parse-query
  [q]
  (log/trace "parse-query" q)
  (-> q syntax/coerce-query parse-query*))

(declare parse-subj-cmp)

(defn parse-object-value
  [v datatype context metadata]
  (let [datatype* (iri/normalize datatype)]
    (if (= datatype* const/iri-id)
      (where/match-iri (json-ld/expand-iri v context))
      (-> (where/anonymous-value v datatype*)
          (where/match-meta metadata)))))

(defn parse-obj-cmp
  [allowed-vars context subj-cmp pred-cmp m triples
   {:keys [list id value type language] :as v-map}]
  (cond list
        (reduce (fn [triples [i list-item]]
                  (parse-obj-cmp allowed-vars context subj-cmp pred-cmp {:i i} triples list-item))
                triples
                (map vector (range) list))

    ;; literal object
        (some? value)
        (let [m*      (cond-> m
                        language (assoc :lang language))
              obj-cmp (if (v/variable? value)
                        (parse-variable-if-allowed allowed-vars value)
                        (parse-object-value value type context m*))]
          (conj triples [subj-cmp pred-cmp obj-cmp]))

    ;; ref object
        :else
        (let [ref-obj (if (v/variable? id)
                        (parse-variable-if-allowed allowed-vars id)
                        (where/match-iri
                         (if (nil? id)
                           (iri/new-blank-node-id)
                           id)))
              ref-cmp (if m
                        (where/match-meta ref-obj m)
                        ref-obj)
              v-map*  (if (nil? id)
                    ;; project newly created bnode-id into v-map
                        (assoc v-map :id (where/get-iri ref-cmp))
                        v-map)]
          (conj (parse-subj-cmp allowed-vars context triples v-map*)
                [subj-cmp pred-cmp ref-cmp]))))

(defn parse-pred-cmp
  [allowed-vars context subj-cmp triples [pred values]]
  (cond
    (v/variable? pred)
    (let [pred-cmp (parse-variable-if-allowed allowed-vars pred)]
      (reduce (partial parse-obj-cmp allowed-vars context subj-cmp pred-cmp nil)
              triples
              values))

    (= pred const/iri-rdf-type)
    (throw (ex-info (str (pr-str const/iri-rdf-type) " is not a valid predicate IRI."
                         " Please use the JSON-LD \"@type\" keyword instead.")
                    {:status 400 :error :db/invalid-predicate}))

    (= :type pred)
    (let [values*  (map (fn [typ] {:id typ})
                        values)
          pred-cmp (where/match-iri const/iri-rdf-type)]
      (reduce (partial parse-obj-cmp allowed-vars context subj-cmp pred-cmp nil)
              triples
              values*))

    :else
    (let [pred-cmp (where/match-iri pred)]
      (reduce (partial parse-obj-cmp allowed-vars context subj-cmp pred-cmp nil)
              triples
              values))))

(defn parse-subj-cmp
  [allowed-vars context triples {:keys [id] :as node}]
  (let [subj-cmp (cond (v/variable? id) (parse-variable-if-allowed allowed-vars id)
                       (nil? id)        (where/match-iri (iri/new-blank-node-id))
                       :else            (where/match-iri id))]
    (reduce (partial parse-pred-cmp allowed-vars context subj-cmp)
            triples
            (->> (dissoc node :id :idx)
                 ;; deterministic patterns for each pred
                 (sort-by (comp str first))))))

(defn parse-triples
  "Flattens and parses expanded json-ld into update triples."
  [expanded allowed-vars context]
  (try*
    (reduce (partial parse-subj-cmp allowed-vars context)
            []
            expanded)
    (catch* e
      (throw (ex-info (str "Parsing failure due to: " (ex-message e)
                           ". Query: " expanded)
                      (or (ex-data e) {})
                      e)))))

(defn parse-txn-opts
  [txn-opts override-opts txn-context]
  (let [{:keys [did] :as opts} (merge (syntax/coerce-txn-opts txn-opts)
                                      (syntax/coerce-txn-opts override-opts))]
    (-> opts
        (assoc :context txn-context)
        (update :identity #(or % did))
        (dissoc :did))))

(defn jld->parsed-triples
  "Parses a JSON-LD document into a sequence of update triples. The document
   will be expanded using the context inside the txn merged with the
   provided parsed-context, if not nil.

   If bound-vars is non-nil, it will replace any variables in the document
   assuming it is a valid variable placement, otherwise it will throw."
  [jld bound-vars parsed-context]
  (-> jld
      (json-ld/expand parsed-context)
      util/get-graph
      util/sequential
      (parse-triples bound-vars parsed-context)))

(defn parse-update-txn
  ([txn]
   (parse-update-txn txn {}))
  ([txn override-opts]
   (let [context       (or (ctx-util/txn-context txn)
                           (:context override-opts))
         [vars values] (-> (get-named txn "values")
                           (parse-values context))
         where         (-> (get-named txn "where")
                           (parse-where vars context))
         bound-vars    (-> where where/bound-variables (into vars))
         delete        (when-let [dlt (get-named txn "delete")]
                         (jld->parsed-triples dlt bound-vars context))
         insert        (when-let [ins (get-named txn "insert")]
                         (jld->parsed-triples ins bound-vars context))
         annotation    (util/get-first-value txn const/iri-annotation)
         opts          (-> (get-named txn "opts")
                           (parse-txn-opts override-opts context))
         ledger-id     (get-named txn "ledger")]
     (when (and (empty? insert) (empty? delete))
       (throw (ex-info "Invalid transaction, insert or delete clause must contain nodes with objects."
                       {:status 400 :error :db/invalid-transaction})))
     (cond-> {:opts opts}
       ledger-id    (assoc :ledger-id ledger-id)
       context      (assoc :context context)
       where        (assoc :where where)
       annotation   (assoc :annotation annotation)
       (seq values) (assoc :values values)
       (seq delete) (assoc :delete delete)
       (seq insert) (assoc :insert insert)))))

(defn blank-node-subject?
  [parsed-triple]
  (-> parsed-triple
      first
      where/get-iri
      iri/blank-node-id?))

(defn upsert-where-del
  "For an upsert transaction.

   Takes a parsed transaction and for each triple, replaces the object position
   with a variable and returns a map with :where and :delete keys.

   Skips blank nodes as they cannot be deleted."
  [parsed-txn]
  (loop [[next-triple & r] parsed-txn
         i      0
         where  []
         delete []]
    (if next-triple
      (if (blank-node-subject? next-triple)
        (recur r (inc i) where delete) ;; can't delete blank node properties
        (let [new-var    (str "?f" i)
              delete-smt (assoc next-triple 2 (parse-variable new-var))
              where-smt  (where/->pattern :optional [delete-smt])] ;; use optional so other matched triples still delete if no match
          (recur r (inc i) (conj where where-smt) (conj delete delete-smt))))
      {:where  where
       :delete delete})))

(defn parse-upsert-txn
  [txn {:keys [context format] :as opts}]
  (let [turtle?    (= :turtle format)
        context    (when-not turtle?
                     (or (ctx-util/txn-context txn)
                         context))
        opts       (parse-txn-opts nil opts context)
        parsed-txn (if turtle?
                     (turtle/parse txn)
                     (jld->parsed-triples txn nil context))
        {:keys [where delete]} (upsert-where-del parsed-txn)]
    {:opts    opts
     :context context
     :where   where
     :delete  delete
     :insert  parsed-txn}))

(defn parse-insert-txn
  [txn {:keys [format context] :as opts}]
  {:insert (if (= :turtle format)
             (turtle/parse txn)
             (jld->parsed-triples txn nil context))
   :context context
   :opts opts})

(defn parse-sparql
  [txn opts]
  (if (sparql/sparql-format? opts)
    (sparql/->fql txn)
    txn))

(defn ensure-ledger
  [txn]
  (if (get-named txn "ledger")
    txn
    (throw (ex-info "Invalid transaction, missing required key: ledger."
                    {:status 400, :error :db/invalid-transaction}))))
