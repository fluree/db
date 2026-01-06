(ns fluree.db.query.fql.parse
  (:require #?(:cljs [cljs.reader :refer [read-string]])
            [clojure.set :as set]
            [clojure.string :as str]
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

(def ^:dynamic *object-var-parsing*
  "When true (default), bare object values like \"?x\" are parsed as variables.
  When false, only an explicit object value map with '@variable' is parsed as
  a variable. Keys and @id variable parsing are unaffected."
  true)

(defn var-parsing-config
  "If present, bound-vars is a set containing all vars that are bound earlier in query
  execution. In the `where` clause it is just the vars from the `values` clause. In
  `insert` and `delete` clauses it also contains vars from the `where` clause.

  If `:parse-object-vars` is false then we will not attempt to parse string literals in
  the object position as variables, only specifically tagged @variable objects will be
  parsed as variables."
  [bound-vars opts]
  {:bound-vars  (set bound-vars)
   :parse-object-vars? (get opts :object-var-parsing true)})

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
  [var-config x]
  (if (contains? (:bound-vars var-config) (symbol x))
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
      (if (number? v)
        (where/untyped-value v)
        (where/anonymous-value v)))))

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
  original query's 'values' vars, however there should be exactly one 'fresh' var in the
  filter fn that isn't in that set - which should be the var that will receive
  flake/o."
  [params {:keys [bound-vars] :as _var-config}]
  (let [non-assigned-vars (set/difference params bound-vars)]
    (case (count non-assigned-vars)
      1 (first non-assigned-vars)
      0 (throw (ex-info (str "Variable filter function has no variable assigned to it, all parameters "
                             "exist in the 'values' clause. Filter function params: " params ". "
                             "Values assigned in query: " bound-vars ".")
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
  [fltr fltr-var var-config ctx]
  (let [code      (parse-code fltr)
        code-vars (or (not-empty (variables code))
                      (throw (ex-info (str "Filter function must contain a valid variable. Provided: " code)
                                      {:status 400 :error :db/invalid-query})))
        var-name  (find-filtered-var code-vars var-config)]
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
  (fn [pattern _var-config _context]
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
  [clause var-config context]
  ;; a single higher-order where pattern is already sequential, so we need to check if it needs wrapping
  (let [clause* (if (higher-order-pattern? clause)
                  [clause]
                  (util/sequential clause))]
    (->> clause*
         (mapcat (fn [pattern]
                   (parse-pattern pattern var-config context)))
         where/->where-clause)))

(defn parse-variable-attributes
  [var attrs var-config context]
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
                               (parse-filter-function var var-config context))
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
  [s-mch p-mch o {:keys [parse-object-vars?] :as var-config} context]
  (let [o* (expand-keys o context)
        explicit-var (get o const/iri-variable)]
    (cond
      explicit-var
      (let [attrs (-> o (dissoc const/iri-variable) (expand-keys context))
            var   (parse-var-name explicit-var)
            o-mch (parse-variable-attributes var attrs var-config context)]
        [(flip-reverse-pattern [s-mch p-mch o-mch])])

      (contains? o* const/iri-value)
      (let [v     (get o* const/iri-value)
            attrs (dissoc o* const/iri-value)
            o-mch (if (and parse-object-vars? (parse-var-name v))
                    (parse-variable-attributes (parse-var-name v) attrs var-config context)
                    (parse-value-attributes v attrs context))]
        [(flip-reverse-pattern [s-mch p-mch o-mch])])

      :else ;;ref
      (let [id-map  (with-id o context) ; not o*, we can't use expanded or we'll lose @reverse
            o-mch   (-> id-map
                        (get const/iri-id)
                        (parse-subject context))
            o-attrs (dissoc id-map const/iri-id)]
        ;; return a thunk wrapping the recursive call to preserve stack
        ;; space by delaying execution
        #(into [(flip-reverse-pattern [s-mch p-mch o-mch])]
               (parse-statements o-mch o-attrs var-config context))))))

(defn parse-statement*
  [s-mch p-mch o var-config context]
  (cond
    (v/query-variable? o var-config)
    (let [o-mch (parse-variable o)]
      [(flip-reverse-pattern [s-mch p-mch o-mch])])

    (map? o)
    (parse-object-map s-mch p-mch o var-config context)

    (sequential? o)
    #(mapcat (fn [o*]
               (parse-statement s-mch p-mch o* var-config context))
             o)

    (type-pred-match? p-mch)
    (let [class-ref (parse-class o context)]
      [(where/->pattern :class (flip-reverse-pattern [s-mch p-mch class-ref]))])

    :else
    (let [o-mch (if (number? o)
                  (where/untyped-value o)
                  (where/anonymous-value o))]
      [(flip-reverse-pattern [s-mch p-mch o-mch])])))

(defn parse-statement
  [s-mch p-mch o var-config context]
  (trampoline parse-statement* s-mch p-mch o var-config context))

(defn parse-statements*
  [s-mch attrs var-config context]
  #(mapcat (fn [[p o]]
             (let [p-mch (parse-predicate p context)]
               (parse-statement s-mch p-mch o var-config context)))
           attrs))

(defn parse-statements
  [s-mch attrs var-config context]
  (trampoline parse-statements* s-mch attrs var-config context))

(defn specified-properties?
  [attrs]
  (every? v/specified-value? (keys attrs)))

(defn nested?
  [attrs]
  (boolean (some (fn [[_k v]]
                   (map? v))
                 attrs)))

(defn simple-property-join?
  [id attrs]
  (and (>= (count attrs) 2)
       (v/query-variable? id)
       (specified-properties? attrs)
       (not (nested? attrs))))

(defn parse-id-map-pattern
  [m var-config context]
  (let [id    (get m const/iri-id)
        s-mch (parse-subject id context)
        attrs (dissoc m const/iri-id)]
    (if (empty? attrs)
      [(where/->pattern :id s-mch)]
      (let [statements (parse-statements s-mch attrs var-config context)]
        (if (simple-property-join? id attrs)
          [(where/->pattern :property-join statements)]
          (sort optimize/compare-triples statements))))))

(defn parse-node-map
  [m var-config context]
  (-> m
      (with-id context)
      (parse-id-map-pattern var-config context)))

(defmethod parse-pattern :node
  [m var-config context]
  (parse-node-map m var-config context))

(defn compile-filter-fn
  [context parsed-codes]
  (->> parsed-codes
       (map (fn [code]
              (comp (fn [typed-value]
                      (:value typed-value))
                    (eval/compile code context))))
       (apply every-pred)))

(defmethod parse-pattern :filter
  [[_ & codes] _var-config context]
  (let [parsed-codes (map parse-code codes)
        vars         (apply set/union (map variables parsed-codes))
        f            (compile-filter-fn context parsed-codes)]
    [(where/->pattern :filter (with-meta f {:forms parsed-codes, :vars vars}))]))

(defmethod parse-pattern :union
  [[_ & unions] var-config context]
  (let [parsed (mapv (fn [clause] (parse-where-clause clause var-config context))
                     unions)]
    [(where/->pattern :union parsed)]))

(defmethod parse-pattern :optional
  [[_ & optionals] var-config context]
  (into []
        (comp (map (fn [clause] (parse-where-clause clause var-config context)))
              (map (partial where/->pattern :optional)))
        optionals))

(defmethod parse-pattern :bind
  [[_ & binds] _var-config context]
  (let [parsed (parse-bind-map binds context)]
    [(where/->pattern :bind parsed)]))

(defmethod parse-pattern :values
  [[_ values] _var-config context]
  (let [[_vars solutions] (parse-values values context)]
    [(where/->pattern :values solutions)]))

(defmethod parse-pattern :exists
  [[_ patterns] var-config context]
  [(where/->pattern :exists (parse-where-clause patterns var-config context))])

(defmethod parse-pattern :not-exists
  [[_ patterns] var-config context]
  [(where/->pattern :not-exists (parse-where-clause patterns var-config context))])

(defmethod parse-pattern :minus
  [[_ patterns] var-config context]
  [(where/->pattern :minus (parse-where-clause patterns var-config context))])

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
  [[_ graph where] var-config context]
  (let [graph* (or (parse-variable graph)
                   (parse-graph-string graph context))
        where* (parse-where-clause where var-config context)]
    [(where/->pattern :graph [graph* where*])]))

(defn parse-where
  [where var-config context]
  (when where
    (-> where
        syntax/coerce-where
        (parse-where-clause var-config context))))

(defn unwrap-tuple-patterns
  "Construct accepts node-map patterns, which can produce :tuple patterns, :class
  patterns, or :id patterns. We only need the pattern components as a template
  for construct, the :id and :class patterns are for optimized query execution,
  so this function unwraps :id and :class patterns and only returns the
  underlying components."
  [patterns]
  (->> patterns
       (mapcat (fn [[pattern-type component :as pattern]]
                 (case pattern-type
                   :class         [component]
                   :property-join component
                   :id            [[component]]
                   [pattern])))
       vec))

(defn parse-construct
  [q context]
  (when-let [construct (:construct q)]
    (-> construct
        syntax/coerce-where
        (parse-where-clause (var-parsing-config nil (:opts q)) context)
        unwrap-tuple-patterns
        select/construct-selector)))

(defn parse-select-as-fn
  [f context output]
  (let [parsed-fn  (parse-code f)
        fn-name    (some-> parsed-fn second first)
        bind-var   (last parsed-fn)
        aggregate? (when fn-name (eval/allowed-aggregate-fns fn-name))
        agg-vars   (variables parsed-fn)
        agg-info   (when aggregate?
                     {:fn-name fn-name
                      :vars    agg-vars})]
    (-> parsed-fn
        (eval/compile context)
        (select/as-selector output bind-var aggregate? agg-info))))

(defn parse-select-aggregate
  [f context]
  (let [parsed   (parse-code f)
        fn-name  (when (seq? parsed) (first parsed))
        agg-vars (variables parsed)
        agg-info {:fn-name fn-name
                  :vars    agg-vars}]
    (-> parsed
        (eval/compile context)
        (select/aggregate-selector agg-info))))

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
  (let [order-by (:order-by q)
        ;; Disambiguate between:
        ;; - a single direction tuple like ["desc" "?x"] or '(desc ?x)
        ;; - a collection of orderings like ["?x" ["desc" "?y"]] etc
        orderings (cond
                    (nil? order-by) nil

                    ;; Single direction tuple encoded as a vector (often from query coercion).
                    (and (vector? order-by)
                         (= 2 (count order-by))
                         (or (syntax/asc? (nth order-by 0))
                             (syntax/desc? (nth order-by 0)))
                         (parse-var-name (nth order-by 1)))
                    [order-by]

                    ;; Single direction tuple encoded as a list/seq.
                    (and (sequential? order-by)
                         (= 2 (count order-by))
                         (or (syntax/asc? (first order-by))
                             (syntax/desc? (first order-by)))
                         (parse-var-name (second order-by)))
                    [order-by]

                    ;; Collection of orderings (vector form).
                    (vector? order-by)
                    order-by

                    ;; Single ordering (var, list tuple, etc).
                    :else
                    [order-by])]
    (some->> orderings
             (mapv (fn [ord]
                     (if-let [v (parse-var-name ord)]
                       [v :asc]
                       (let [[dir dim] ord
                             v (parse-var-name dim)]
                         (if (syntax/asc? dir)
                           [v :asc]
                           [v :desc]))))))))

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
         var-config    (var-parsing-config vars (:opts q))
         where         (parse-where (:where q) var-config context)
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
  [[_ sub-query] var-config context]
  (let [sub-query* (-> sub-query
                       syntax/coerce-subquery
                       (update :opts merge {:object-var-parsing (:parse-object-vars? var-config)})
                       (parse-query* context))]
    [(where/->pattern :query sub-query*)]))

(defmethod parse-pattern :service
  [[_ {:keys [clause] :as data}] _var-config context]
  (let [sparql (str/join " " (into (sparql/context->prefixes context)
                                   ["SELECT *"
                                    (str "WHERE " clause)]))]
    [(where/->pattern :service (assoc data :sparql-q sparql))]))

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
  [var-config context subj-cmp pred-cmp m triples v-map]
  (let [id     (util/get-id v-map)
        v-list (util/get-list v-map)
        value  (util/get-value v-map)
        type   (util/get-types v-map)
        lang   (util/get-lang v-map)
        explicit-var (get v-map const/iri-variable)]
    (cond v-list
          (reduce (fn [triples [i list-item]]
                    (parse-obj-cmp var-config context subj-cmp pred-cmp {:i i} triples list-item))
                  triples
                  (map vector (range) v-list))

          explicit-var
          (let [var-val    (cond
                             (map? explicit-var) (util/get-value explicit-var)
                             (sequential? explicit-var) (-> explicit-var first util/get-value)
                             :else explicit-var)
                var-mch    (parse-variable-if-allowed var-config var-val)
                dt         (if (sequential? type) (first type) type)
                dt-matcher (some-> dt (where/datatype-matcher context))
                lang-mch   (some-> lang where/lang-matcher)
                f          (combine-filters lang-mch dt-matcher)
                obj-cmp    (cond-> var-mch f (where/with-filter f))]
            (conj triples [subj-cmp pred-cmp obj-cmp]))

          ;; literal object
          (some? value)
          (let [m*      (cond-> m
                          lang (assoc :lang lang))
                obj-cmp (if (v/variable? value var-config)
                          (parse-variable-if-allowed var-config value)
                          (parse-object-value value type context m*))]
            (conj triples [subj-cmp pred-cmp obj-cmp]))

          ;; ref object
          :else
          (let [ref-obj (if (v/variable? id var-config)
                          (parse-variable-if-allowed var-config id)
                          (where/match-iri
                           (if (nil? id)
                             (iri/new-blank-node-id)
                             id)))
                ref-cmp (if m
                          (where/match-meta ref-obj m)
                          ref-obj)
                v-map*  (if (nil? id)
                          ;; project newly created bnode-id into v-map
                          (assoc v-map const/iri-id (where/get-iri ref-cmp))
                          v-map)]
            (conj (parse-subj-cmp var-config context triples v-map*)
                  [subj-cmp pred-cmp ref-cmp])))))

(defn parse-pred-cmp
  [var-config context subj-cmp triples [pred values]]
  (cond
    (v/variable? pred)
    (let [pred-cmp (parse-variable-if-allowed var-config pred)]
      (reduce (partial parse-obj-cmp var-config context subj-cmp pred-cmp nil)
              triples
              values))

    (= pred const/iri-rdf-type)
    (throw (ex-info (str (pr-str const/iri-rdf-type) " is not a valid predicate IRI."
                         " Please use the JSON-LD \"@type\" keyword instead.")
                    {:status 400 :error :db/invalid-predicate}))

    (= const/iri-type pred)
    (let [values*  (map (fn [typ] {const/iri-id typ})
                        values)
          pred-cmp (where/match-iri const/iri-rdf-type)]
      (reduce (partial parse-obj-cmp var-config context subj-cmp pred-cmp nil)
              triples
              values*))

    :else
    (let [pred-cmp (where/match-iri pred)]
      (reduce (partial parse-obj-cmp var-config context subj-cmp pred-cmp nil)
              triples
              values))))

(defn parse-subj-cmp
  [var-config context triples node]
  (let [id       (util/get-id node)
        subj-cmp (cond (v/variable? id) (parse-variable-if-allowed var-config id)
                       (nil? id)        (where/match-iri (iri/new-blank-node-id))
                       :else            (where/match-iri id))]
    (reduce (partial parse-pred-cmp var-config context subj-cmp)
            triples
            (->> (dissoc node const/iri-id)
                 ;; deterministic patterns for each pred
                 (sort-by (comp str first))))))

(defn parse-triples
  "Flattens and parses expanded json-ld into update triples."
  [expanded var-config context]
  (try*
    (reduce (partial parse-subj-cmp var-config context)
            [] expanded)
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

   Variable parsing is constrained by the var config allows (see `var-parsing-config`)."
  [jld var-config parsed-context]
  (-> jld
      (json-ld/expand parsed-context)
      util/get-graph
      util/sequential
      (parse-triples var-config parsed-context)))

(defn parse-update-txn
  ([txn]
   (parse-update-txn txn {}))
  ([txn override-opts]
   (let [context       (or (ctx-util/txn-context txn)
                           (:context override-opts))
         [vars values] (-> (get-named txn "values")
                           (parse-values context))
         var-config    (var-parsing-config vars override-opts)
         where         (-> (get-named txn "where")
                           (parse-where var-config context))
         var-config*   (cond-> (update var-config :bound-vars into (where/clause-variables where))
                         ;; don't attempt variable parsing if there are no unified vars
                         (not (or where vars))  (assoc :parse-object-vars? false))
         delete        (when-let [dlt (get-named txn "delete")]
                         (jld->parsed-triples dlt var-config* context))
         insert        (when-let [ins (get-named txn "insert")]
                         (jld->parsed-triples ins var-config* context))
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
  "Takes parsed transaction data and for each triple pattern, replaces the object position
  with a variable corresponding to the subject and predicate. Returns a map with :where
  and :delete keys.

   Skips blank nodes as they are new subjects with no existing flakes to retract."
  [parsed-txn]
  (loop [[next-triple & r] parsed-txn
         vars              {}
         where             []
         delete            []]
    (if next-triple
      (if (blank-node-subject? next-triple)
        ;; no need to find/delete a blank node subject, it's guaranteed to be new
        (recur r vars where delete)
        (let [s-iri (-> next-triple (get 0) where/get-iri)
              p-iri (-> next-triple (get 1) where/get-iri)]

          (if (get vars [s-iri p-iri])
            ;; we've already generated a pattern for this data
            (recur r vars where delete)
            ;; generate where and delete pattern
            (let [o-var      (str "?f" (count vars))
                  delete-smt (assoc next-triple 2 (parse-variable o-var))
                  ;; optional so we don't have to match every var in order to delete
                  where-smt  (where/->pattern :optional [delete-smt])]
              (recur r
                     (assoc vars [s-iri p-iri] o-var)
                     (conj where where-smt)
                     (conj delete delete-smt))))))
      {:where where
       :delete delete})))

(defn parse-upsert-txn
  [txn {:keys [context format] :as opts}]
  (let [turtle?    (= :turtle format)
        context    (when-not turtle?
                     (or (ctx-util/txn-context txn)
                         context))
        opts       (-> (parse-txn-opts nil opts context)
                       (assoc :object-var-parsing false))
        var-config (var-parsing-config nil opts)
        parsed-txn (if turtle?
                     (turtle/parse txn)
                     (jld->parsed-triples txn var-config context))
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
             (jld->parsed-triples txn (var-parsing-config nil (assoc opts :object-var-parsing false)) context))
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
