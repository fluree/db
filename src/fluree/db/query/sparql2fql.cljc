(ns fluree.db.query.sparql2fql
  (:require #?(:clj [clojure.java.io :as io])
            #?(:clj [fluree.db.util.docs :as docs]
               [instaparse.core :as insta]
               :cljs [instaparse.core :as insta :refer-macros [defparser]])
            #?(:cljs [fluree.db.util.cljs-shim :refer-macros [inline-resource]])
            [clojure.string :as str]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.util.core :as util]
            [clojure.set :as set]
            #?(:cljs [cljs.tools.reader :refer [read-string]])))

#?(:clj (set! *warn-on-reflection* true))

(defn handle-var
  [var-clause]
  (->> var-clause
       str/join
       (str "?")))

(defn valid-modifiers?
  "[:Modifiers [:PrettyPrint]]"
  [modifiers]
  (if (and (= (first modifiers) :Modifiers)
           (vector? (second modifiers)))
    true
    (throw (ex-info (str "Improperly formatted SPARQL query. Note: FlureeDB does not support all SPARQL features. Trouble parsing query modifiers: " modifiers)
                    {:status 400
                     :error  :db/invalid-query}))))

(defn handle-iri-ref
  [ref]
  (subs ref 1 (-> ref count dec)))

(defn handle-prefix-dec1
  "BNF -- PNAME_NS IRIREF"
  [prefix-dec]
  (let [name   (->> prefix-dec (drop-last 2) str/join keyword)
        iriref (-> prefix-dec last second handle-iri-ref)]
    {name iriref}))

(defn handle-base-dec1
  "BNF -- IRIREF"
  [base-dec]
  (let [iriref (-> base-dec second handle-iri-ref)]
    {"@base" iriref}))

(defn handle-prefixed-name
  [prefixed-name]
  (let [prefixed-name-str (str/join prefixed-name)]
    (log/trace "handling prefixed name:" prefixed-name-str)
    prefixed-name-str))

(defn handle-iri
  "Returns a predicate.
  BNF -- IRIREF | PrefixedName

  IRIREF not currently supported."
  [iri]
  (case (first iri)
    :PrefixedName (handle-prefixed-name (rest iri))

    :IRIREF
    (throw (ex-info (str "IRIREF not currently supported as SPARQL predicate. Provided: " iri)
                    {:status 400
                     :error  :db/invalid-query}))))

(defn handle-rdf-literal
  "BNF -- String ( LANGTAG | ( '^^' iri ) )?"
  [rdf-literal]
  (log/trace "handle-rdf-literal:" rdf-literal)
  (str/join rdf-literal))

(defn handle-numeric-literal
  [num-literal]
  (read-string num-literal))

(defn handle-boolean-literal
  [bool-lit]
  (read-string bool-lit))


(defn handle-data-block-value-or-graph-term
  [data-block-value]
  (case (first data-block-value)
    :NumericLiteral
    (-> data-block-value second handle-boolean-literal)

    :BooleanLiteral
    (-> data-block-value second read-string)

    "UNDEF"
    nil

    :iri
    (handle-iri (second data-block-value))

    :RDFLiteral
    (handle-rdf-literal (rest data-block-value))))

(defn handle-inline-data-one-var
  [var-parts]
  (let [variable-key (if (= (-> var-parts first first) :Var)
                       (handle-var (-> var-parts first rest))
                       (throw (ex-info (str "Improperly formatted SPARQL query. Note: FlureeDB does not support all SPARQL features. Trouble parsing VALUES: " var-parts)
                                       {:status 400
                                        :error  :db/invalid-query})))
        variable-val (if (= (-> var-parts second first) :DataBlockValue)
                       (handle-data-block-value-or-graph-term (-> var-parts second second))
                       (throw (ex-info (str "Improperly formatted SPARQL query. Note: FlureeDB does not support all SPARQL features. Trouble parsing VALUES: " var-parts)
                                       {:status 400
                                        :error  :db/invalid-query})))]
    {variable-key variable-val}))

(defn handle-values
  [values]
  (case (first values)
    :InlineDataOneVar
    (handle-inline-data-one-var (rest values))))

(defn handle-modifiers
  [query modifiers]
  (reduce (fn [q modifier]
            (case (first modifier)
              :PrettyPrint
              (assoc q :prettyPrint true)

              :ValuesClause
              (update q :vars merge (handle-values (second modifier)))

              (throw (ex-info (str "Unknown modifier. Note: FlureeDB does not support all SPARQL features. Trouble parsing query modifiers: " modifier)
                              {:status 400
                               :error  :db/invalid-query}))))
          query modifiers))

(defn handle-object
  "BNF -- VarOrTerm | TriplesNode"
  [object]
  (case (first object)
    :Var (handle-var (rest object))

    :GraphTerm (let [res (handle-data-block-value-or-graph-term (second object))] (if (vector? res) (second res) res))))

(defn handle-object-in-property-list-path
  "Given a subject, predicate, and either an ObjectPath or Object List, returns an array of where clauses."
  ([subject predicate object]
   (handle-object-in-property-list-path subject predicate object nil))
  ([subject predicate object source]
   (case (first object)
     ;; Single clause in [ ]
     :ObjectPath (if source
                   [[source subject predicate (handle-object (second object))]]
                   [[subject predicate (handle-object (second object))]])

     ;; Multiple clauses
     :ObjectList (if source
                   (map #(vector source subject predicate (handle-object (second %)))
                        (rest object))
                   (map #(vector subject predicate (handle-object (second %)))
                        (rest object))))))

(defn handle-path-primary
  "Returns a predicate.
  BNF -- iri | 'a' | '!'
  a becomes rdf:type, and ! is not currently supported. "
  [path-primary]
  (cond (and (coll? path-primary) (= :iri (first path-primary)))
        (handle-iri (second path-primary))

        (= path-primary "a")
        "type"

        (= path-primary "!")
        (throw (ex-info (str "! not currently supported as SPARQL predicate.")
                        {:status 400
                         :error  :db/invalid-query}))))

(def supported-path-mod #{"+" "*"})

(defn handle-path-mod
  [mod]
  (let [mod-type (or (supported-path-mod (first mod))
                     (throw (ex-info (str "The path modification: " (first mod) " is not currently supported. ")
                                     {:status 400
                                      :error  :db/invalid-query})))]
    (if (= 2 (count mod))
      (str mod-type (nth mod 1))
      mod-type)))

(defn handle-path-sequence
  "Returns a predicate name.
  BNF -- PathPrimary PathMod?
  PathMod being - ?, *, +, the only one which we currently support is +
  "
  [path-sequence]
  (let [predicate (handle-path-primary (-> path-sequence first second))
        predicate (if-let [mod (second path-sequence)]
                    (str predicate (handle-path-mod (rest mod)))
                    predicate)]
    predicate))

(defn handle-property-list-path-not-empty
  "Returns an array of where clauses, i.e. [[?s ?p ?o] [?s ?p1 ?o1]]
  BNF -- ( Path | Var ) ObjectPath ( ( ( Path | Simple ) ObjectList )? )* "
  [subject prop-path]
  (loop [[path-item & r] prop-path
         most-recent-pred   nil
         clauses            []]
    (if path-item
      (case (first path-item)
        :Var (let [predicate   (handle-var (rest path-item))
                   ;; Immediately after a Var, is either an ObjectPath or ObjectList
                   object      (first r)
                   new-r       (rest r)
                   new-clauses (handle-object-in-property-list-path subject predicate object)]
               (recur new-r predicate (concat clauses new-clauses)))

        :PathSequence (let [predicate   (handle-path-sequence (rest path-item))
                            object      (first r)
                            new-r       (rest r)
                            new-clauses (handle-object-in-property-list-path subject predicate object)]
                        (recur new-r predicate (concat clauses new-clauses)))

        :ObjectPath
        (recur r most-recent-pred
               (concat clauses (handle-object-in-property-list-path subject most-recent-pred path-item))))
      clauses)))

(defn handle-triples-same-subject-path
  "Returns array of clauses.
  BNF -- VarOrTerm PropertyListPathNotEmpty | TriplesNodePath PropertyListPath."
  [same-subject-path]
  (let [subject (handle-var (-> same-subject-path first rest))]
    (reduce (fn [where-arr where-item]
              (case (first where-item)
                :PropertyListPathNotEmpty
                (concat where-arr (handle-property-list-path-not-empty subject (rest where-item)))))
            [] (drop 1 same-subject-path))))

(defn handle-triples-block
  "TriplesSameSubjectPath ( <'.'> TriplesBlock? )?"
  [triples-block]
  (->> (map (fn [triple-item]
              (case (first triple-item)
                :TriplesBlock
                (handle-triples-block (rest triple-item))

                :TriplesSameSubjectPath
                (handle-triples-same-subject-path (rest triple-item)))) triples-block)
       (apply concat)))

(declare handle-arg-list)
(declare handle-expression)

(defn handle-iri-or-function
  "BNF -- iri ArgList?"
  [iri-or-function]
  (map #(case (first %)
          :iri (handle-iri (rest %))
          :ArgList (handle-arg-list (rest %)))
       iri-or-function))

;; Not part of analytical queries, but part of SPARQL spec: GROUP_CONCAT
(def supported-aggregates #{"COUNT" "SUM" "MIN" "MAX" "AVG" "SAMPLE"})

(defn handle-aggregate
  [aggregate]
  (let [function    (supported-aggregates (first aggregate))
        distinct?   (and (string? (second aggregate)) (= "DISTINCT" (second aggregate)))
        function    (cond (and distinct? (= function "COUNT"))
                          "count-distinct"

                          ;; TODO
                          distinct?
                          (throw (ex-info (str "Distinct option is currently not supported in functions other than count. Provided function: " function)
                                          {:status 400
                                           :error  :db/invalid-query}))

                          :else function)
        expressions (if distinct?
                      (drop 2 aggregate)
                      (drop 1 aggregate))
        expressions (map #(-> (handle-expression (rest %)) first) expressions)]
    (str "(" (str/lower-case function) " " (str/join " " expressions) ")")))

;; Listed here so we can easily add functions we need to support to get to SPARQL 1.1 spec
(def all-functions #{"STR" "LANG" "LANGMATCHES" "DATATYPE" "BOUND"
                     "IRI" "URI" "BNODE" "RAND" "ABS" "CEIL" "FLOOR" "ROUND"
                     "CONCAT" "STRLEN" "UCASE" "LCASE" "ENCODE_FOR_URI" "CONTAINS"
                     "STRSTARTS" "STRENDS" "STRBEFORE" "STRAFTER" "YEAR" "MONTH"
                     "DAY" "HOURS" "MINUTES" "SECONDS" "TIMEZONE" "TZ" "NOW"
                     "UUID" "STRUUID" "MD5" "SHA1" "SHA256" "SHA384" "SHA512"
                     "COALESCE" "IF" "STRLANG" "STRDT" "sameTerm" "isIRI" "isURI"
                     "isBLANK" "isLITERAL" "isNUMERIC"})

(def supported-functions {"COALESCE"  "coalesce"
                          "STR"       "str"
                          "RAND"      "rand"
                          "ABS"       "abs"
                          "CEIL"      "ceil"
                          "FLOOR"     "floor"
                          "CONCAT"    "concat"
                          "STRLEN"    "count"
                          "STRSTARTS" "strStarts"
                          "STRENDS"   "strEnds"
                          "IF"        "if"
                          "SHA256"    "sha256"
                          "SHA512"    "sha512"})

(defn handle-built-in-call
  "BNF is Aggregate or {FUN}( Expression ). Where FUN could be one of 50+ functions.
  There's some other variation possible here, including  functions take a var instead of an expression and other functions can take more than one expression."
  [built-in]
  (log/trace "handle-built-in-call:" built-in)
  (let [fn-name (first built-in)]
    (cond (string? fn-name)
          (let [function (get supported-functions fn-name)
                _        (when-not function
                           (throw (ex-info (str "The function " fn-name
                                                " is not yet implemented in SPARQL")
                                           {:status 400
                                            :error  :db/invalid-query})))
                args     (-> built-in second handle-arg-list flatten)]
            (str "(" function " " (str/join " " args) ")"))

          (= (-> built-in first first) :Aggregate)
          (handle-aggregate (-> built-in first rest)))))

(defn handle-multiplicative-expression
  "BNF -- UnaryExpression ( '*' UnaryExpression | '/' UnaryExpression )*"
  [mult-exp]
  (log/trace "handle-multiplicative-expression:" mult-exp)
  (case (first mult-exp)
    :BrackettedExpresion (handle-expression (rest mult-exp))

    :BuiltInCall (handle-built-in-call (rest mult-exp))

    :iriOrFunction (handle-iri-or-function (rest mult-exp))

    ;; TODO: Wrapping this in double quotes works for simple literals.
    ;;       But we should also support @lang & ^^datatype-iri.
    :RDFLiteral (str \" (handle-rdf-literal (rest mult-exp)) \")

    :NumericLiteral (handle-numeric-literal (second mult-exp))

    :BooleanLiteral (handle-boolean-literal (second mult-exp))

    :Var (handle-var (rest mult-exp))))

(def arithmetic-ops #{"+" "-" "*" "/" ""})

(defn handle-numeric-expression
  "BNF -- MultiplicativeExpression ( '+' MultiplicativeExpression | '-' MultiplicativeExpression | ( NumericLiteralPositive | NumericLiteralPositive ) ( ( '*' UnaryExpression ) | ( '/' UnaryExpression ) )* )"
  [num-exp]
  (log/trace "handle-numeric-expression:" num-exp)
  (loop [exp-group (take 3 num-exp)
         r         (drop 3 num-exp)
         acc       []]
    (log/trace "handle-numeric-expression exp-group:" exp-group)
    ;; Could be :MultiplicativeExpression, :NumericLiteralPositive,
    ;; :NumericLiteralPositive, :UnaryExpression, :UnaryExpression
    (case (count exp-group)
      1 (handle-multiplicative-expression (-> exp-group first second))

      2 (let [operator (let [op (first exp-group)]
                         (or (arithmetic-ops op)
                             (throw (ex-info (str "Unrecognized or unsupported opertator. Provided: " op)
                                             {:status 400
                                              :error  :db/invalid-query}))))
              mult-exp (handle-multiplicative-expression (-> exp-group second rest))]
          (recur (take 2 r) (drop 2 r) (concat acc [[operator mult-exp]])))

      3 (let [mult-exp   (handle-multiplicative-expression (-> exp-group first rest))
              operator   (let [op (second exp-group)]
                           (or (arithmetic-ops op)
                               (throw (ex-info (str "Unrecognized or unsupported opertator. Provided: " op)
                                               {:status 400
                                                :error  :db/invalid-query}))))
              mult-exp-2 (handle-multiplicative-expression (-> exp-group (nth 2) rest))]

          (recur (take 2 r) (drop 2 r) (concat acc [[mult-exp operator mult-exp-2]]))))))

;; Not supported IN, NOT IN
(def comparators #{"=" "!=" "<" ">" "<=" ">="})

(defn handle-relational-expression
  "Returns expression as string.

  BNF -- NumericExpression ( '=' NumericExpression | '!=' NumericExpression | '<' NumericExpression | '>' NumericExpression | '<=' NumericExpression | '>=' NumericExpression | 'IN' ExpressionList | 'NOT' 'IN' ExpressionList )?"
  [rel-exp]
  (log/trace "handling relational expression:" rel-exp)
  (let [first-exp  (handle-numeric-expression (-> rel-exp first rest))
        _          (log/trace "first-exp:" first-exp)
        operator   (when-let [op (second rel-exp)]
                     (if (and op (comparators op))
                       op
                       (throw (ex-info (str "Unrecognized or unsupported opertator. Provided: " op)
                                       {:status 400
                                        :error  :db/invalid-query}))))
        _          (log/trace "operator:" operator)
        second-exp (when-let [second-exp (and (> (count rel-exp) 1) (nth rel-exp 2))]
                     (handle-numeric-expression (rest second-exp)))]
    (log/trace "second-exp:" second-exp)
    (if (or operator second-exp)
      (str "(" operator " " first-exp " " second-exp ")")
      first-exp)))

(defn handle-expression
  "BNF -- RelationalExpression*"
  [exp]
  (log/trace "handle-expresion:" exp)
  (map (fn [exp']
         (log/trace "handle-expression exp':" exp')
         (case (first exp')
           :RelationalExpression
           (handle-relational-expression (rest exp'))))
       exp))

(defn handle-bind
  "Returns bind statement inside [ ], i.e. [{\"bind\": {\"?handle\": \"dsanchez\"}}]"
  [bind]
  (log/trace "handle-bind:" bind)
  (let [var       (handle-var (-> bind second rest))
        bindValue (-> (handle-expression (-> bind first rest)) first)
        bindValue (if (str/starts-with? bindValue "(") (str "#" bindValue)
                                                       bindValue)]
    {:bind {var bindValue}}))

(defn handle-arg-list
  "BNF -- NIL | 'DISTINCT'? Expression ( Expression )* "
  [arg-list]
  (log/trace "handle-arg-list:" arg-list)
  (let [arg-list' (case (first arg-list)
                    :ExpressionList (rest arg-list)
                    :Expression [arg-list])]
    (map (fn [arg]
           (cond (= "NIL" arg)
                 nil

                 (= "DISTINCT" arg)
                 "DISTINCT"

                 (= :Expression (first arg))
                 (handle-expression (rest arg))))
         arg-list')))

(declare handle-graph-pattern-not-triples)

(defn handle-group-graph-pattern-sub
  "TriplesBlock? ( GraphPatternNotTriples <'.'?> TriplesBlock? )* "
  [where-val]
  (->> (mapv (fn [where-item]
               (case (first where-item)
                 :TriplesBlock
                 (handle-triples-block (rest where-item))

                 :GraphPatternNotTriples
                 [(handle-graph-pattern-not-triples (second where-item))])) where-val)
       (apply concat)
       vec))

(defn handle-where-clause
  "( SubSelect | GroupGraphPatternSub )"
  [where-clause]
  (case (first where-clause)
    :GroupGraphPatternSub
    (handle-group-graph-pattern-sub (rest where-clause))

    :SubSelect
    (throw (ex-info (str "SubSelect queries not currently supported. Provided: " (rest where-clause))
                    {:status 400
                     :error  :db/invalid-query}))))

(defn handle-constraint
  "BNF- BrackettedExpression | BuiltInCall | FunctionCall"
  [filter-exp]
  (case (first filter-exp)
    :BrackettedExpression (-> (handle-expression (-> filter-exp second second vector)) vec)

    :BuiltInCall (handle-built-in-call (rest filter-exp))

    :FunctionCall (throw (ex-info (str "This feature is not yet implemented in SPARQL. Provided: " filter-exp) {:status 400 :error :db/invalid-query}))))

(defn handle-group-or-union
  "BNF -- GroupGraphPattern ( <'UNION'> GroupGraphPattern )*
  {\"union\": [ [[s p o][s1 p1 p1]] [[s2 p2 o2]] ]   "
  [group-or-union]
  {:union (mapv handle-where-clause group-or-union)})

(defn handle-optional-graph-pattern
  [optional]
  {:optional (first (mapv handle-where-clause optional))})


(defn handle-graph-pattern-not-triples
  "BNF -- GroupOrUnionGraphPattern | OptionalGraphPattern | MinusGraphPattern | GraphGraphPattern | ServiceGraphPattern | Filter | Bind | InlineData"
  [not-triples]
  (case (first not-triples)
    :GroupOrUnionGraphPattern
    (handle-group-or-union (rest not-triples))

    :OptionalGraphPattern
    (handle-optional-graph-pattern (rest not-triples))

    :MinusGraphPattern
    (throw (ex-info (str "This feature is not yet implemented in SPARQL. Provided: " not-triples) {:status 400 :error :db/invalid-query}))

    :GraphGraphPattern
    (throw (ex-info (str "This feature is not yet implemented in SPARQL. Provided: " not-triples) {:status 400 :error :db/invalid-query}))

    :ServiceGraphPattern
    (throw (ex-info (str "This feature is not yet implemented in SPARQL. Provided: " not-triples) {:status 400 :error :db/invalid-query}))

    :Filter
    {:filter (handle-constraint (-> not-triples second second))}

    :Bind
    (handle-bind (rest not-triples))

    :InlineData
    {:bind (handle-values (second not-triples))}))

(defn handle-group-condition
  "BNF -- BuiltInCall | FunctionCall | Expression ( 'AS' Var )? | Var"
  [group-condition]
  (case (first group-condition)
    :Var (handle-var (rest group-condition))
    :BuiltInCall (throw
                  (ex-info (str "This format of GroupBy is not currently supported. Provided: "
                                group-condition)
                           {:status 400 :error :db/invalid-query}))

    :FunctionCall (throw
                   (ex-info (str "This format of GroupBy is not currently supported. Provided: "
                                 group-condition)
                            {:status 400 :error :db/invalid-query}))

    :Expression (throw
                 (ex-info (str "This format of GroupBy is not currently supported. Provided: "
                               group-condition)
                          {:status 400 :error :db/invalid-query}))))

(defn handle-order-condition
  "BNF -- ( ( 'ASC' | 'DESC' ) BrackettedExpression ) | ( Constraint | Var )"
  [order-condition]
  (cond (#{"ASC" "DESC"} (first order-condition))
        (let [exp (-> order-condition second second)]
          [(first order-condition) (-> order-condition second second rest handle-expression first)])

        (= :Var (-> order-condition first first))
        (handle-var (-> order-condition first rest))

        (= :Constraint (-> order-condition first first))
        (throw (ex-info (str "Ordering by a constraint not currently supported. Provided: " order-condition)
                        {:status 400
                         :error  :db/invalid-query}))))


(defn handle-having-condition
  [having-condition]
  (let [expressions (-> having-condition
                        second ;; skip :Constraint
                        second ;; skip :BrackettedExpression
                        rest) ;; get all :Expression (s)
        _           (when (> (count expressions) 1)
                      (throw (ex-info (str "Multiple 'HAVING' expressions in SPARQL not currently supported, please let us know you'd like this supported!")
                                      {:status 400 :error :db/invalid-query})))
        parsed      (handle-expression expressions)]
    (first parsed)))

(defn handle-solution-modifier
  [solution-modifier]
  (reduce (fn [acc modifier]
            (case (first modifier)
              :LimitClause (assoc acc :limit (-> modifier second read-string))
              :OffsetClause (assoc acc :offset (-> modifier second read-string))
              :GroupClause (let [group-conditions (-> modifier rest)
                                 groupBy          (if (= 1 (count group-conditions))
                                                    (handle-group-condition (-> group-conditions first second))
                                                    (mapv #(handle-group-condition (second %)) group-conditions))]
                             (assoc acc :groupBy groupBy))
              :OrderClause (assoc acc :orderBy (handle-order-condition (-> modifier rest)))
              :HavingClause (let [having-condition (second (some #(when (and (vector? %) (= :HavingCondition (first %))) %) modifier))
                                  having           (handle-having-condition having-condition)]
                              (assoc acc :having having))))
          {} solution-modifier))

(def supported-select-options #{"DISTINCT" "REDUCED"})

(defn handle-dataset-clause
  [dataset-clause]
  (log/trace "handling dataset clause:" dataset-clause)
  (case (first dataset-clause)
    :DefaultGraphClause
    (-> dataset-clause rest str/join)

    :NamedGraphClause
    (throw (ex-info (str "SPARQL named graphs are not yet supported in Fluree. "
                         "See here for additional details: "
                         docs/error-codes-page "#query-sparql-no-named-graphs")
                    {:status 400
                     :error  :db/invalid-query}))))

(defn handle-select
  [query select]
  (loop [query query
         [item & r] select]
    (if-not item
      (let [q (update query :select vec)]
        (if-let [select-key (:selectKey q)]
          (-> (set/rename-keys q {:select select-key})
              (dissoc :selectKey))
          q))
      (let [[q r] (if (string? item)
                    [(assoc query :selectKey (keyword (str "select" (str/capitalize item)))) r]

                    (case (first item)
                      :Var
                      [(update query :select concat [(handle-var (rest item))]) r]

                      :Expression
                      (let [_        (log/trace "handle-select expression:" item)
                            exp      (-> item rest handle-expression first)
                            _        (log/trace "handle-select exp:" exp)
                            next-as? (= "AS" (first r))
                            _        (log/trace "handle-select r:" r)
                            [exp r] (if next-as?
                                      [(str "(as " exp " " (handle-var (-> r second rest)) ")") (drop 2 r)]
                                      [exp r])]
                        [(update query :select concat [exp]) r])

                      :WhereClause
                      [(assoc query :where (vec (handle-where-clause (second item)))) r]

                      :SolutionModifier
                      [(merge query (handle-solution-modifier (rest item))) r]

                      :DatasetClause
                      [(assoc query :from (handle-dataset-clause (rest item))) r]))]
        (recur q r)))))

(defn handle-prologue
  "BNF -- ( BaseDec1 | PrefixDec1 )*"
  [prologue]
  (reduce (fn [acc pro]
            (case (first pro)
              :BaseDec1
              (merge acc (handle-base-dec1 (rest pro)))
              #_(throw (ex-info (str "Base URIs not currently supported in SPARQL implementation. Provided: " (rest pro))
                                {:status 400 :error :db/invalid-query}))

              :PrefixDec1
              (merge acc (handle-prefix-dec1 (rest pro)))))
          {} prologue))

(defn assoc-if
  [pred m & kvs]
  (if pred
    (apply assoc m kvs)
    m))

(defn parsed->fql
  [parsed]
  (reduce (fn [query top-level]
            (case (first top-level)
              :Prologue
              (let [prologue (rest top-level)]
                (assoc-if (seq prologue)
                  query :context (handle-prologue prologue)))

              :Modifiers
              (when valid-modifiers?
                (handle-modifiers query (rest top-level)))

              :SelectQuery
              (handle-select query (rest top-level))

              (throw (ex-info (str "Improperly formatted SPARQL query. Note: FlureeDB does not support all SPARQL features. Trouble parsing: " (first top-level))
                              {:status 400
                               :error  :db/invalid-query}))))
          {} parsed))
