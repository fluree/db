(ns fluree.db.query.sparql-parser
  (:require #?(:clj [clojure.java.io :as io])
            #?(:clj  [instaparse.core :as insta]
               :cljs [instaparse.core :as insta :refer-macros [defparser]])
            #?(:cljs [fluree.db.util.cljs-shim :refer-macros [inline-resource]])
            [clojure.string :as str]
            [fluree.db.util.log :as log]
            [fluree.db.util.core :as util]
            [clojure.set :as set]
            #?(:cljs [cljs.tools.reader :refer [read-string]])))

#?(:cljs (def inline-content (inline-resource "sparql-js.bnf")))

#?(:clj  (def sparql (insta/parser (io/resource "sparql.bnf")))
   :cljs (defparser sparql inline-content))


(def wikidata-prefixes
  ["<http://www.wikidata.org/entity/>"
   "<http://www.w3.org/2002/07/owl#>"
   "<http://www.w3.org/ns/prov#>"
   "<http://www.bigdata.com/queryHints#>"
   "<http://www.wikidata.org/prop/novalue/>"
   "<http://www.wikidata.org/prop/statement/value/>"
   "<http://www.wikidata.org/prop/qualifier/value/>"
   "<http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
   "<http://www.wikidata.org/prop/>"
   "<http://www.wikidata.org/reference/>"
   "<http://www.wikidata.org/prop/statement/>"
   "<http://www.bigdata.com/rdf/search#>"
   "<http://schema.org/>"
   "<http://www.wikidata.org/prop/direct/>"
   "<http://www.wikidata.org/value/>"
   "<http://www.wikidata.org/wiki/Special:EntityData/>"
   "<http://www.bigdata.com/rdf/gas#>"
   "<http://www.wikidata.org/entity/statement/>"
   "<http://www.bigdata.com/rdf#>"
   "<http://www.wikidata.org/prop/qualifier/value-normalized/>"
   "<http://wikiba.se/ontology#>"
   "<http://www.wikidata.org/prop/reference/value-normalized/>"
   "<http://www.w3.org/2000/01/rdf-schema#>"
   "<http://www.wikidata.org/prop/qualifier/>"
   "<http://www.wikidata.org/prop/statement/value-normalized/>"
   "<http://www.wikidata.org/prop/reference/value/>"
   "<http://www.w3.org/2004/02/skos/core#>"
   "<http://www.wikidata.org/prop/reference/>"
   "<http://www.w3.org/2001/XMLSchema#>"])

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

(defn handle-prefixed-name
  "Returns a source and predicate"
  [prefixed-name]
  (let [name   (str/join prefixed-name)
        [source predicate] (str/split name #":")
        ;; This is in order to accomodate ISO-8601 formatted clock times in SPARQL.
        source (str/replace source ";" ":")]
    (cond (or (= source "fdb") (= source "fd"))
          ["$fdb" predicate]

          (str/starts-with? source "wd")
          (let [predicate (if (str/starts-with? predicate "?")
                            predicate
                            (str source ":" predicate))]
            ["$wd" predicate])

          (= source "fullText")
          ["$fdb" (str source ":" predicate)]

          (str/starts-with? source "fdb")
          [(str "$" source) predicate]

          (str/starts-with? source "fd")
          [(str "$fdb" (subs source 2)) predicate]

          :else
          [source predicate])))

(defn handle-iri
  "Returns a source and predicate.
  BNF -- IRIREF | PrefixedName

  IRIREF not currently supported."
  [iri]
  (condp = (first iri)
    :PrefixedName (handle-prefixed-name (rest iri))

    :IRIREF
    (throw (ex-info (str "IRIREF not currently supported as SPARQL predicate. Provided: " iri)
                    {:status 400
                     :error  :db/invalid-query}))))

(defn handle-rdf-literal
  "BNF -- String ( LANGTAG | ( '^^' iri ) )?"
  [rdf-literal]
  (str/join rdf-literal))

(defn handle-numeric-literal
  [num-literal]
  (read-string num-literal))

(defn handle-boolean-literal
  [bool-lit]
  (read-string bool-lit))


(defn handle-data-block-value-or-graph-term
  [data-block-value]
  (condp = (first data-block-value)
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
  (condp = (first values)
    :InlineDataOneVar
    (handle-inline-data-one-var (rest values))

    :else
    ;; TODO
    ))


(defn handle-modifiers
  [query modifiers]
  (reduce (fn [q modifier]
            (condp = (first modifier)

              :PrettyPrint
              (assoc q :prettyPrint true)

              :ValuesClause
              (update q :vars merge (handle-values (second modifier)))

              :else
              (throw (ex-info (str "Unknown modifier. Note: FlureeDB does not support all SPARQL features. Trouble parsing query modifiers: " modifier)
                              {:status 400
                               :error  :db/invalid-query}))))
          query modifiers))

(defn handle-object
  "BNF -- VarOrTerm | TriplesNode"
  [object]
  (condp = (first object)
    :Var (handle-var (rest object))

    :GraphTerm (let [res (handle-data-block-value-or-graph-term (second object))] (if (vector? res) (second res) res))))

(defn handle-object-in-property-list-path
  "Given a subject, predicate, and either an ObjectPath or Object List, returns an array of where clauses."
  ([subject predicate object]
   (handle-object-in-property-list-path subject predicate object nil))
  ([subject predicate object source]
   (condp = (first object)
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
  "Return source and predicate.
  BNF -- iri | 'a' | '!'
  a becomes rdf:type, and ! is not currently supported. "
  [path-primary]
  (cond (and (coll? path-primary) (= :iri (first path-primary)))
        (handle-iri (second path-primary))

        (= path-primary "a")
        ["$fdb" "rdf:type"]

        (= path-primary "!")
        (throw (ex-info (str "! not currently supported as SPARQL predicate.")
                        {:status 400
                         :error  :db/invalid-query}))))

(def supported-path-mod #{"+"})

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
  "Returns a predicate name and source.
  BNF -- PathPrimary PathMod?
  PathMod being - ?, *, +, the only one which we currently support is +
  "
  [path-sequence]
  (let [[source predicate] (handle-path-primary (-> path-sequence first second))
        predicate (if-let [mod (second path-sequence)]

                    (str predicate (handle-path-mod (rest mod)))
                    predicate)]
    [source predicate]))

(defn handle-property-list-path-not-empty
  "Returns an array of where clauses, i.e. [[?s ?p ?o] [?s ?p1 ?o1]]
  BNF -- ( Path | Var ) ObjectPath ( ( ( Path | Simple ) ObjectList )? )* "
  [subject prop-path]
  (loop [[path-item & r] prop-path
         most-recent-pred   nil
         most-recent-source nil
         clauses            []]
    (if path-item
      (condp = (first path-item)
        :Var (let [predicate   (handle-var (rest path-item))
                   ;; Immediately after a Var, is either an ObjectPath or ObjectList
                   object      (first r)
                   new-r       (rest r)
                   new-clauses (handle-object-in-property-list-path subject predicate object)]
               (recur new-r predicate most-recent-source (concat clauses new-clauses)))

        :PathSequence (let [[source predicate] (handle-path-sequence (rest path-item))
                            object      (first r)
                            new-r       (rest r)
                            new-clauses (handle-object-in-property-list-path subject predicate object source)] (recur new-r predicate source (concat clauses new-clauses)))

        :ObjectPath
        (recur r most-recent-pred most-recent-source
               (concat clauses (handle-object-in-property-list-path subject most-recent-pred path-item most-recent-source))))
      clauses)))

(defn handle-triples-same-subject-path
  "Returns array of clauses.
  BNF -- VarOrTerm PropertyListPathNotEmpty | TriplesNodePath PropertyListPath."
  [same-subject-path]
  (let [subject (handle-var (-> same-subject-path first rest))]
    (reduce (fn [where-arr where-item]
              (condp = (first where-item)
                :PropertyListPathNotEmpty
                (concat where-arr (handle-property-list-path-not-empty subject (rest where-item)))))
            [] (drop 1 same-subject-path))))

(defn handle-triples-block
  "TriplesSameSubjectPath ( <'.'> TriplesBlock? )?"
  [triples-block]
  (->> (map (fn [triple-item]
              (condp = (first triple-item)
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
  (map #(condp = (first %)
          :iri (handle-iri (rest %))
          :ArgList (handle-arg-list (rest %)))
       iri-or-function))

;; Not part of SPARQL spec, but to add: RAND, STDEV, VARIANCE
;; Not part of analytical queries, but part of SPARQL spec: GROUP_CONCAT
(def supported-aggregates #{"COUNT" "SUM" "MIN" "MAX" "AVG" "SAMPLE" "MEDIAN"})

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

;; Listed here so we can easily function we need to support to get to SPARQL 1.1 spec
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
                          "CONCAT"    "groupconcat"
                          "STRLEN"    "count"
                          "STRSTARTS" "strStarts"
                          "STRENDS"   "strEnds"
                          "IF"        "if"})

(defn handle-built-in-call
  "BNF is Aggregate or {FUN}( Expression ). Where FUN could be one of 50+ functions.
  There's some other variation possible here, including  functions take a var instead of an expression and other functions can take more than one expression."
  [built-in]
  (cond (string? (first built-in))
        (let [function (get supported-functions (first built-in))
              _        (when-not function
                         (throw (ex-info "This function is not yet implemented in SPARQL"
                                         {:status 400
                                          :error  :db/invalid-query})))
              args     (-> (handle-arg-list (-> built-in second rest)) flatten)]
          (str "(" function " " (str/join " " args) ")"))

        (= (-> built-in first first) :Aggregate)
        (handle-aggregate (-> built-in first rest))))

(defn handle-multiplicative-expression
  "BNF -- UnaryExpression ( '*' UnaryExpression | '/' UnaryExpression )*"
  [mult-exp]
  (condp = (first mult-exp)
    :BrackettedExpresion (handle-expression (rest mult-exp))

    :BuiltInCall (handle-built-in-call (rest mult-exp))

    :iriOrFunction (handle-iri-or-function (rest mult-exp))

    :RDFLiteral (handle-rdf-literal (rest mult-exp))

    :NumericLiteral (handle-numeric-literal (second mult-exp))

    :BooleanLiteral (handle-boolean-literal (second mult-exp))

    :Var (handle-var (rest mult-exp))))

(def arithmetic-ops #{"+" "-" "*" "/" ""})

(defn handle-numeric-expression
  "BNF -- MultiplicativeExpression ( '+' MultiplicativeExpression | '-' MultiplicativeExpression | ( NumericLiteralPositive | NumericLiteralPositive ) ( ( '*' UnaryExpression ) | ( '/' UnaryExpression ) )* )"
  [num-exp]
  (loop [exp-group (take 3 num-exp)
         r         (drop 3 num-exp)
         acc       []]
    ;; Could be :MultiplicativeExpression, :NumericLiteralPositive,
    ;; ;NumericLiteralPositive, :UnaryExpression, :UnaryExpression
    (condp = (count exp-group)
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
  (let [first-exp  (handle-numeric-expression (-> rel-exp first rest))
        operator   (when-let [op (second rel-exp)]
                     (if (and op (comparators op))
                       op
                       (throw (ex-info (str "Unrecognized or unsupported opertator. Provided: " op)
                                       {:status 400
                                        :error  :db/invalid-query}))))
        second-exp (when-let [second-exp (and (> (count rel-exp) 1) (nth rel-exp 2))]
                     (handle-numeric-expression (rest second-exp)))]
    (if (or operator second-exp)
      (str "(" operator " " first-exp " " second-exp ")")
      first-exp)))


(defn handle-expression
  "BNF -- RelationalExpression*"
  [exp]
  (map (fn [exp]
         (condp = (first exp)
           :RelationalExpression
           (handle-relational-expression (rest exp)))) exp))

(defn handle-bind
  "Returns bind statement inside [ ], i.e. [{\"bind\": {\"?handle\": \"dsanchez\"}}]"
  [bind]
  (let [var       (handle-var (-> bind second rest))
        bindValue (-> (handle-expression (-> bind first rest)) first)
        bindValue (if (str/starts-with? bindValue "(") (str "#" bindValue)
                                                       bindValue)]
    {:bind {var bindValue}}))

(defn handle-arg-list
  "BNF -- NIL | 'DISTINCT'? Expression ( Expression )* "
  [arg-list]
  (map (fn [arg]
         (cond (= "NIL" arg)
               nil

               (= "DISTINCT" arg)
               "DISTINCT"

               (= :Expression (first arg))
               (handle-expression (rest arg))))
       arg-list))

(declare handle-graph-pattern-not-triples)

(defn handle-group-graph-pattern-sub
  "TriplesBlock? ( GraphPatternNotTriples <'.'?> TriplesBlock? )* "
  [where-val]
  (->> (mapv (fn [where-item]
               (condp = (first where-item)
                 :TriplesBlock
                 (handle-triples-block (rest where-item))

                 :GraphPatternNotTriples
                 [(handle-graph-pattern-not-triples (second where-item))])) where-val)
       (apply concat)
       vec))

(defn handle-where-clause
  "( SubSelect | GroupGraphPatternSub )"
  [where-clause]
  (condp = (first where-clause)
    :GroupGraphPatternSub
    (handle-group-graph-pattern-sub (rest where-clause))

    :SubSelect
    (throw (ex-info (str "SubSelect queries not currently supported. Provided: " (rest where-clause))
                    {:status 400
                     :error  :db/invalid-query}))))

(defn handle-constraint
  "BNF- BrackettedExpression | BuiltInCall | FunctionCall"
  [filter-exp]
  (condp = (first filter-exp)
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
  (condp = (first not-triples)
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
  (condp = (first group-condition)
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

(defn handle-solution-modifier
  [solution-modifier]
  (reduce (fn [acc modifier]
            (condp = (first modifier)
              :LimitClause (assoc acc :limit (-> modifier second read-string))
              :OffsetClause (assoc acc :offset (-> modifier second read-string))
              :GroupClause (let [group-conditions (-> modifier rest)
                                 groupBy          (if (= 1 (count group-conditions))
                                                    (handle-group-condition (-> group-conditions first second))
                                                    (mapv #(handle-group-condition (second %)) group-conditions))]
                             (assoc acc :groupBy groupBy))
              :OrderClause (assoc acc :orderBy (handle-order-condition (-> modifier rest)))))
          {} solution-modifier))

(def supported-select-options #{"DISTINCT" "REDUCED"})

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
      (let [[q r] (if (and (string? item))
                    [(assoc query :selectKey (keyword (str "select" (str/capitalize item)))) r]

                    (condp = (first item)
                      :Var
                      [(update query :select concat [(handle-var (rest item))]) r]

                      :Expression
                      (let [exp      (-> (handle-expression (rest item)) first)
                            next-as? (= "AS" (first r))
                            [exp r] (if next-as?
                                      [(str "(as " exp " " (handle-var (-> r second rest)) ")") (drop 2 r)]
                                      [exp r])]
                        [(update query :select concat [exp]) r])

                      :WhereClause
                      [(assoc query :where (vec (handle-where-clause (second item)))) r]

                      :SolutionModifier
                      [(merge query (handle-solution-modifier (rest item))) r]))]
        (recur q r)))))

(defn handle-prologue
  "BNF -- ( BaseDec1 | PrefixDec1 )*"
  [prologue]
  (reduce (fn [acc pro]
            (condp = (first pro)
              :BaseDec1 (throw (ex-info (str "Base URIs not currently supported in SPARQL implementation. Provided: " (rest pro))
                                        {:status 400 :error :db/invalid-query}))

              :PrefixDec1 (merge acc (handle-prefix-dec1 (rest pro)))))
          {} prologue))

(defn sparql-parsed->analytical
  [parsed]
  (reduce (fn [query top-level]
            (condp = (first top-level)

              :Prologue (assoc query :prefixes (handle-prologue (rest top-level)))

              :Modifiers
              (when valid-modifiers?
                (handle-modifiers query (rest top-level)))

              :SelectQuery
              (handle-select query (rest top-level))

              :else
              (throw (ex-info (str "Improperly formatted SPARQL query. Note: FlureeDB does not support all SPARQL features. Trouble parsing: " (first top-level))
                              {:status 400
                               :error  :db/invalid-query})))) {} parsed))


(defn sparql-to-ad-hoc
  [sparql-query]
  (let [sparql-parsed (sparql sparql-query)
        _             (if (= instaparse.gll.Failure (type sparql-parsed))
                        (throw (ex-info (str "Improperly formatted SPARQL query. Note: FlureeDB does not support all SPARQL features. Provided: " sparql-query)
                                        {:status 400
                                         :error  :db/invalid-query})))]
    (sparql-parsed->analytical sparql-parsed)))

(comment


  #?(:clj  (def sparql (insta/parser (clojure.java.io/resource "sparql.bnf")))
     :cljs (defparser sparql inline-content))

  (sparql value-query)
  (sparql-to-ad-hoc value-query)


  ;; WORKS
  (def basic-query "SELECT ?person ?nums\nWHERE {\n    ?person     fd:person/handle    \"jdoe\";\n                fd:person/favNums    ?nums.\n}")

  (def basic-query-2 "SELECT ?collection \nWHERE { \n  ?collectionID fdb:_collection/name ?collection. \n  }")

  (def basic-query-3 "SELECT ?nums\nWHERE {\n    ?person     fd:person/handle    \"jdoe\";\n                fd:person/favNums    ?nums.\n    ?person2    fd:person/handle    \"zsmith\";\n                fd:person/favNums   ?nums.\n}")

  (def basic-query-4 "SELECT ?nums\nWHERE {\n    ?person     fdb4:person/handle   \"jdoe\";\n                fdb4:person/favNums  ?nums;\n                fdb5:person/favNums  ?nums.\n}")

  (def wd-query "SELECT ?movie ?title\nWHERE {\n  ?user     fdb:person/favMovies    ?movie.\n  ?movie    fdb:movie/title       ?title.\n     ?wdMovie  wd:?label             ?title;\n            wdt:P840               ?narrative_location;\n            wdt:P31               wd:Q11424.\n  ?user     fdb:person/handle       ?handle.\n  \n}\nLIMIT 100")

  (def wd-query-2 "SELECT ?name ?artist ?artwork ?artworkLabel\nWHERE {\n    ?person     fd:person/handle        \"jdoe\";\n                fd:person/favArtists    ?artist.\n    ?artist     fd:artist/name          ?name.\n    ?artwork    wdt:P170                ?creator.\n    ?creator    wd:?label                ?name.\n}")

  (def wd-query-3-select-distinct "SELECT DISTINCT ?name ?artist ?artwork ?artworkLabel\nWHERE {\n    ?person     fd:person/handle        \"jdoe\";\n                fd:person/favArtists    ?artist.\n    ?artist     fd:artist/name          ?name.\n    ?artwork    wdt:P170                ?creator.\n    ?creator    wd:?label                ?name.\n}\nLIMIT 5")

  (def full-text-query "SELECT ?person\nWHERE {\n  ?person fullText:person/handle \"jdoe\".\n}")

  (def full-text-query-2 "SELECT ?person\nWHERE {\n  ?person fullText:person \"jdoe\".\n}")

  (def full-text-query-3 "SELECT ?person ?nums ?age\nWHERE {\n  ?person fullText:person/handle \"jdoe\".\n  ?person fdb:person/favNums ?nums.\n  ?person fdb:person/age ?age.\n}")

  (def union-query "SELECT ?person ?age\nWHERE {\n  {   ?person fdb:person/age 70.\n    ?person fdb:person/handle \"dsanchez\". } \n  UNION \n  {   ?person fdb:person/handle \"anguyen\". } \n  ?person fdb:person/age ?age.\n}")

  (def recur "SELECT ?followHandle\nWHERE {\n  ?person fdb:person/handle \"anguyen\".\n  ?person fdb:person/follows+ ?follows.\n  ?follows fdb:person/handle ?followHandle.\n}")

  (def bind-query-2 "SELECT ?person ?handle\nWHERE {\n  BIND (\"dsanchez\" AS ?handle)\n  ?person fdb:person/handle ?handle.\n}")

  (def optional-query "SELECT ?person ?name ?handle ?favNums \nWHERE {\n  ?person fdb:person/fullName ?name. \n  OPTIONAL {  ?person fdb:person/handle ?handle. \n              ?person fdb:person/favNums ?favNums. }\n}")

  (def optional-query-2 "SELECT ?handle ?num\nWHERE {\n  ?person fdb:person/handle ?handle.\n  OPTIONAL { ?person fdb:person/favNums ?num. }\n}")

  (def agg-query-sum "SELECT (SUM(?nums) AS ?sum)\nWHERE {\n    ?person     fd:person/handle    \"zsmith\";\n                fd:person/favNums    ?nums.\n}")

  (def bind-query "SELECT ?hash\nWHERE {\n  ?s fdb:_block/number ?bNum.\n  BIND (MAX(?bNum) AS ?maxBlock)\n  ?s fdb:_block/number ?maxBlock.\n  ?s fdb:_block/hash ?hash.\n}")


  (def agg-query-sample "SELECT (SAMPLE(10 ?nums) AS ?sample)\nWHERE {\n    ?person     fd:person/handle    \"zsmith\";\n                fd:person/favNums    ?nums.\n}")

  (def filter-query "SELECT ?handle ?num\nWHERE {\n  ?person fdb:person/handle ?handle.\n  ?person fdb:person/favNums ?num.\n  FILTER ( ?num > 10 ).\n}")

  (def optional-filter "SELECT ?handle ?num\nWHERE {\n  ?person fdb:person/handle ?handle.\n  OPTIONAL { ?person fdb:person/favNums ?num. \n            FILTER( ?num > 10 )\n    }\n}")

  (def optional-filter-coalesce "SELECT ?favNums ?age\nWHERE {\n  ?person fdb:person/favNums ?favNums.\n  OPTIONAL {\n    ?person fdb:person/age ?age.\n     FILTER( ?favNums > COALESCE(?age, 3))\n  }\n \n}")

  (def group-by-q "SELECT ?handle\nWHERE {\n  ?person fdb:person/handle ?handle.\n}\nGROUP BY ?person")

  (def group-by-two "SELECT ?handle\nWHERE {\n  ?person fdb:person/handle ?handle.\n}\nGROUP BY ?person ?handle")

  (def cross-time "SELECT ?nums\nWHERE {\n    ?person     fd4:person/handle   \"zsmith\";\n                fd4:person/favNums  ?nums;\n                fd5:person/favNums  ?nums.\n}")

  (def with-prefixes "PREFIX ftest: <fluree/test>\nSELECT ?nums\nWHERE {\n   ?person     fd4:person/handle   \"zsmith\";\n                fd4:person/favNums  ?nums.\n    ?personTest ftest:person/handle \"zsmith\";\n                ftest:person/favNums  ?nums.\n}")

  (def cross-db-cross-time "PREFIX ftest: <fluree/test> \nSELECT ?nums\nWHERE {\n   ?person     fd4:person/handle   \"zsmith\";\n                fd4:person/favNums  ?nums.\n    ?personTest ftest5:person/handle \"zsmith\";\n                ftest5:person/favNums  ?nums.\n}")

  (def order-by-q "SELECT ?handle\nWHERE {\n  ?person fdb:person/handle ?handle.\n}\nORDER BY ?person")

  (def order-by-asc "SELECT ?handle\nWHERE {\n  ?person fdb:person/handle ?handle.\n}\nORDER BY ASC(?person)")

  (def recur+depth "SELECT ?followHandle\nWHERE {\n  ?person fdb:person/handle \"anguyen\".\n  ?person fdb:person/follows+3 ?follows.\n  ?follows fdb:person/handle ?followHandle.\n}")

  (def value-query "SELECT ?handle\nWHERE {\n  VALUES ?handle { \"dsanchez\" }\n  ?person fdb:person/handle ?handle.\n}")

  )



