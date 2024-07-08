(ns fluree.db.query.sparql.translator
  (:require [fluree.db.constants :as const]
            [clojure.string :as str]
            #?(:cljs [cljs.tools.reader :refer [read-string]])
            #?(:clj [clojure.java.io :as io])
            #?(:clj  [instaparse.core :as insta :refer [defparser]]
               :cljs [instaparse.core :as insta :refer-macros [defparser]])
            #?(:cljs [fluree.db.util.cljs-shim :refer-macros [inline-resource]])))

(defn rule?
  [x]
  (and (sequential? x)
       (keyword? (first x))))

;; take a term rule and return a value
(defmulti parse-term (fn [[tag & _]] tag))
;; take a rule return a sequence of entry tuples [[k v]...]
(defmulti parse-rule (fn [[tag & _]] tag))

(defmethod parse-term :Var
  ;; Var ::= VAR1 WS | VAR2 WS
  ;; [:Var "n" "u" "m" "s"]
  [[_ & var]]
  (str "?" (str/join var)))

(def supported-aggregate-functions
  {"MAX"       "max"
   "MIN"       "min"
   "SAMPLE"    "sample1"
   "COUNT"     "count"
   "SUM"       "sum"
   "AVG"       "avg"})

(defmethod parse-term :Aggregate
  ;; Aggregate ::= 'COUNT' WS <'('> WS 'DISTINCT'? WS ( '*' | Expression ) WS <')'> WS
  ;; | 'SUM' WS <'('> WS 'DISTINCT'? Expression <')'>
  ;; | 'MIN' <'('>  WS 'DISTINCT'? Expression <')'>
  ;; | 'MAX' <'('>  WS 'DISTINCT'? Expression <')'>
  ;; | 'AVG' <'('>  WS 'DISTINCT'? Expression <')'>
  ;; | 'SAMPLE' <'('>  WS 'DISTINCT'? Expression? Expression <')'>
  ;; | 'GROUP_CONCAT' <'('> WS 'DISTINCT'? Expression ( <';'> WS 'SEPARATOR' WS <'='> WS String WS )? <')'>
  [[_ & [func & body]]]
  (if-let [f (get supported-aggregate-functions func)]
    (let [distinct? (= "DISTINCT" (first body))
          f (cond (and distinct? (= f "count"))
                  "count-distinct"

                  distinct?
                  (throw (ex-info "DISTINCT only supported with COUNT"
                                  {:status 400 :error :db/invalid-query}))

                  :else
                  f)
          body (if distinct? (second body) (first body))]
      (str "(" f " " (parse-term body) ")"))
    (throw (ex-info (str "Unsupported aggregate function: " func)
                    {:status 400 :error :db/invalid-query}))))

(def supported-scalar-functions
  {"COALESCE"  "coalesce"
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

(defmethod parse-term :ExpressionList
  ;; ExpressionList ::= NIL | <'('> Expression ( <','> Expression )* <')'>
  [[_ & expressions]]
  (mapv parse-term expressions))

(defmethod parse-term :Func
  [[_ func & args]]
  (if-let [f (get supported-scalar-functions func)]
    (str "(" f " " (str/join " "
                             (->> (mapv parse-term args)
                                  (flatten)
                                  (map (fn [arg]
                                         (if (and (string? arg)
                                                  (not (str/starts-with? arg "?")))
                                           (str \" arg \")
                                           arg))))
                             #_(reduce
                               (fn [parsed-args expr]
                                 (let [parsed (parse-term expr)]
                                   (cond
                                     ;; We need to quote literals to embed in the func string.
                                     ;; (contains? (set (flatten expr)) :RDFLiteral)
                                     ;; (conj parsed-args (str \" parsed \"))
                                     ;; handle the nesting from :ExpressionList
                                     (vector? parsed)    (into parsed-args parsed)
                                     ;; no special handling necessary
                                     :else               (conj parsed-args parsed ))))
                               []
                               args)) ")")
    (throw (ex-info (str "Unsupported function: " func)
                    {:status 400 :error :db/invalid-query}))))

(defmethod parse-term :NumericLiteral
  [[_ num-str]]
  (read-string num-str))

(defmethod parse-term :MultiplicativeExpression
  ;; MultiplicativeExpression ::= UnaryExpression ( '*' UnaryExpression | '/' UnaryExpression )*
  ;; <UnaryExpression> ::= '!' PrimaryExpression
  ;; | '+' PrimaryExpression
  ;; | '-' PrimaryExpression
  ;; | PrimaryExpression
  ;; <PrimaryExpression> ::= BrackettedExpression | BuiltInCall | iriOrFunction | RDFLiteral | NumericLiteral | BooleanLiteral | Var
  [[_ & expression]]
  (str/join " " (mapv parse-term expression)))

(defmethod parse-term :NumericExpression
  ;; NumericExpression ::= WS AdditiveExpression WS
  ;; <AdditiveExpression> ::= MultiplicativeExpression ( '+' MultiplicativeExpression | '-' MultiplicativeExpression | ( NumericLiteralPositive | NumericLiteralNegative ) ( ( '*' UnaryExpression ) | ( '/' UnaryExpression ) )* )*
  [[_ & [m-exp op & right-exps]]]
  (let [expr (parse-term m-exp)]
    (cond
      (= "+" op) (str "(" op " " expr " " (str/join " " (mapv parse-term right-exps)) ")")
      (= "-" op) (str "(" op " " expr " " (str/join " " (mapv parse-term right-exps)) ")")
      (= "*" op) (str "(" op " " expr " " (str/join " " (mapv parse-term right-exps)) ")")
      (= "/" op) (str "(" op " " expr " " (str/join " " (mapv parse-term right-exps)) ")")
      :else
      expr)))

(defmethod parse-term :RelationalExpression
  ;; RelationalExpression ::= NumericExpression WS ( '=' NumericExpression | '!=' NumericExpression | '<' NumericExpression | '>' NumericExpression | '<=' NumericExpression | '>=' NumericExpression | 'IN' ExpressionList | 'NOT' 'IN' ExpressionList )?
  [[_ & [n-exp op op-or-exp expr-list]]]
  (let [expr (parse-term n-exp)]
    (cond
      (= "IN" op)
      (throw (ex-info (str "Unsupported operator: " op)
                      {:status 400 :error :db/invalid-query}))
      (and (= "NOT" op)
           (= "IN" op-or-exp))
      (throw (ex-info (str "Unsupported operator: " op)
                      {:status 400 :error :db/invalid-query}))

      (nil? op)
      expr

      (= "!=" op)
      (str "(not= " expr " " (parse-term op-or-exp) ")")

      :else
      ;; op: =, <, >, <=, >=
      (str "(" op " " expr " " (parse-term op-or-exp) ")"))))

(defmethod parse-term :Expression
  ;; Expression ::= WS ConditionalOrExpression WS
  ;; <ConditionalOrExpression> ::= ConditionalAndExpression ( <'||'> ConditionalAndExpression )*
  ;; <ConditionalAndExpression> ::= ValueLogical ( <'&&'> ValueLogical )*
  ;; <ValueLogical> ::= RelationalExpression
  [[_ & expression]]
  (str/join " " (mapv parse-term expression)))

(defmethod parse-term :IRIREF
  ;; #"<[^<>\"{}|^`\x00-\x20]*>" WS
  ;; [:IRIREF <ex:example>]
  [[_ iri]]
  (subs iri 1 (-> iri count dec)))

(defmethod parse-rule :PrefixDecl
  ;; PrefixDecl ::= <'PREFIX'> WS PNAME_NS WS IRIREF
  ;; [:PrefixDecl "e" "x" ":" [:IRIREF "<http://example.com/>"]]
  [[_ & prefix-decl]]
  (let [prefix (->> (drop-last 2 prefix-decl) vec str/join)
        iri    (->> prefix-decl
                    (drop-while (comp not sequential?))
                    first
                    parse-term)]
    [[prefix iri]]))

(defmethod parse-rule :BaseDecl
  ;; BaseDecl ::= <'BASE'> WS IRIREF
  ;; [:BaseDecl [:IRIREF "<http://example.org/book/>"]]
  [[_ base-decl]]
  (let [iri (parse-term base-decl)]
    [[const/iri-base iri]
     [const/iri-vocab iri]]))

(defmethod parse-rule :Prologue
  ;; Prologue ::= ( BaseDecl | PrefixDecl )*
  [[_ & prologue]]
  (->> prologue
       (reduce (fn [context decl] (into context (parse-rule decl))) {})
       (conj [:context])
       (conj [])))

(defmethod parse-rule :SelectClause
  ;; SelectClause ::= WS <'SELECT'> WS ( 'DISTINCT' | 'REDUCED')? ( ( WS ( Var | ( <'('> Expression WS 'AS' WS Var <')'> ) ) )+ | ( WS '*' ) )
  [[_ & select-clause]]
  (let [[modifier & rules] select-clause
        select-key (condp = modifier
                     "DISTINCT" :selectDistinct
                     "REDUCED"  (throw (ex-info "SELECT REDUCED is not a supported SPARQL clause"
                                                {:status 400 :error :db/invalid-query}))
                     :select)]
    (loop [[term & r] (if (#{"DISTINCT" "REDUCED"} modifier)
                        rules
                        select-clause)
           result     []]
      (if term
        (cond (rule? term)
              (let [[tag & body] term]
                (if (= tag :Var)
                  (recur r (conj result (parse-term term)))
                  ;; :Expression
                  (let [[next next-next & r*] r
                        expr (parse-term term)
                        as?  (= "AS" next)
                        expr (if as?
                               (str "(as " expr " " (parse-term next-next) ")")
                               expr)]
                    (recur (if as? r* r) (conj result expr)))))

              (= "*" term)
              (recur r (conj result "*")))
        [[select-key result]]))))

(defmethod parse-term :DefaultGraphClause
  ;; DefaultGraphClause ::= SourceSelector
  ;; <SourceSelector> ::= iri
  [[_ source]]
  (parse-term source))

(defmethod parse-term :NamedGraphClause
  ;; NamedGraphClause ::= <'NAMED'> SourceSelector
  ;; <SourceSelector> ::= iri
  [[_ source]]

(defmethod parse-term :RDFLiteral
  ;; RDFLiteral ::= String WS ( LANGTAG | ( '^^' iri ) )? WS
  ;; LANGTAG    ::=   #"@[a-zA-Z]+-[a-zA-Z0-9]*" WS
  [[_ & literal]]
  (loop [[char & r] literal
         result     ""]
    (if char
      (cond
        ;; datatype :iri
        (= "^^" char)             (recur nil {const/iri-value result const/iri-type (parse-term (first r))})
        ;; LANGTAG
        (= :LANGTAG (first char)) (recur nil {const/iri-value result const/iri-language (parse-term char)})
        ;; String
        :else
        (recur r (str result char)))
      result)))

(defmethod parse-term :Bind
  ;; Bind ::= <'BIND' WS '(' WS>  Expression <WS 'AS' WS> Var <WS ')' WS>
=======
  (throw (ex-info "FROM NAMED is not a supported SPARQL clause"
                  {:status 400 :error :db/invalid-query}))
  (parse-term source))

(defmethod parse-rule :DatasetClause
  ;; DatasetClause ::= <'FROM'> WS ( DefaultGraphClause | NamedGraphClause )
  ;; DefaultGraphClause ::= SourceSelector
  ;; NamedGraphClause ::= <'NAMED'> SourceSelector
  ;; <SourceSelector> ::= iri
  [[_ source]]
  [[:from (parse-term source)]])

(defmethod parse-term :RDFLiteral
  [[_ & literal]]
  (apply str literal))

(defmethod parse-term :Bind
>>>>>>> 28aaa46d (rewrite sparql translator)
  [[_ & bindings]]
  ;; bindings come in as val, var; need to be reversed to var, val.
  (into [:bind] (->> bindings
                     (mapv parse-term)
                     (partition-all 2)
                     (mapcat reverse))))

(defmethod parse-term :BrackettedExpression
  ;; BrackettedExpression ::= <'('> WS Expression WS <')'>
  [[_ bracketted-expr]]
  (parse-term bracketted-expr))

(defmethod parse-term :Constraint
  ;; Constraint ::= BrackettedExpression | BuiltInCall | FunctionCall
  [[_ constraint]]
  (parse-term constraint))

(defmethod parse-term :Filter
  ;; Filter ::= <'FILTER'> WS Constraint
  [[_ constraint]]  [:filter [(parse-term constraint)]])

(defmethod parse-term :OptionalGraphPattern
  ;; OptionalGraphPattern ::= <'OPTIONAL'> GroupGraphPattern
  [[_ & optional]]
  (into [:optional] (mapv parse-term optional)))

(defmethod parse-term :DataBlockValue
  ;; DataBlockValue ::= iri | RDFLiteral | NumericLiteral | BooleanLiteral | 'UNDEF' WS
  [[_ value]]
  (if (= value "UNDEF")
    nil
    (parse-term value)))

(defmethod parse-term :InlineDataFull
  ;; InlineDataFull ::= ( NIL | '(' Var* ')' ) '{' ( '(' DataBlockValue* ')' | NIL )* '}'
  [_ data]
  (throw (ex-info "Multiple inline data values not supported"
                  {:status 400 :error :db/invalid-query})))

(defmethod parse-term :InlineDataOneVar
  ;; InlineDataOneVar ::= Var <'{'> WS DataBlockValue* <'}'>
  [[_ var & data-block-values]]
  (if (> (count data-block-values) 1)
    (throw (ex-info "Multiple inline data values not supported"
                    {:status 400 :error :db/invalid-query}))
    [:bind (parse-term var) (parse-term (first data-block-values))]))

(defmethod parse-term :InlineData
  ;; InlineData ::= <'VALUES'> WS DataBlock
  ;; <DataBlock> ::= InlineDataOneVar | InlineDataFull
  [[_ inline-data]]
  (parse-term inline-data))

(defmethod parse-term :GraphPatternNotTriples
  ;; GraphPatternNotTriples ::= GroupOrUnionGraphPattern | OptionalGraphPattern | MinusGraphPattern | GraphGraphPattern | ServiceGraphPattern | Filter | Bind | InlineData
  [[_ & non-triples]]
  (into [] (mapv parse-term non-triples)))

(defmethod parse-term :PrefixedName
  [[_ & name]]
  (apply str name))

(defmethod parse-term :iri
  ;; iri ::= ( IRIREF | PrefixedName ) WS
  [[_ iri]]
  (parse-term iri))

(defmethod parse-term :PathPrimary
  ;; PathPrimary ::= iri | 'a' | '!' PathNegatedPropertySet | '(' Path ')'
  ;; PathNegatedPropertySet ::= PathOneInPropertySet | '(' ( PathOneInPropertySet ( '|' PathOneInPropertySet )* )? ')'
  ;; PathOneInPropertySet ::= iri | 'a' | '^' ( iri | 'a' )
  [[_ el]]
  (cond (rule? el) (parse-term el)
        (= el "a") const/iri-type
        :else
        (throw (ex-info (str "Non-predicate paths are not supported: " el)
                        {:status 400 :error :db/invalid-query}))))

(defmethod parse-term :PathMod
  ;; PathMod ::= '?' | '*' | ('+' INTEGER?) WS
  [[_ mod degree]]
  (str mod degree))

(defmethod parse-term :PathSequence
  ;; PathSequence ::= PathEltOrInverse ( <'/'> PathEltOrInverse )*
  ;; TODO: it may be a mistake to hide the '^'
  ;; <PathEltOrInverse> ::= PathElt | <'^'> PathElt
  ;; <PathElt> ::= PathPrimary PathMod?
  [[_ & elements]]
  (apply str (mapv parse-term elements)))

(defmethod parse-term :Object
  [[_ obj]]
  (parse-term obj))

(defmethod parse-term :ObjectList
  ;; ObjectList ::= Object ( <','> WS Object )*
  ;; Object ::= GraphNode
  ;; <GraphNode> ::= VarOrTerm | TriplesNode
  ;; <VarOrTerm> ::= Var | GraphTerm WS
  ;; TriplesNode ::= Collection | BlankNodePropertyList
  ;; Collection ::=  '(' GraphNode+ ')'
  ;; BlankNodePropertyList ::= '[' PropertyListNotEmpty ']'
  [[_ path :as obj-path]]
  (parse-term path))

(defmethod parse-term :ObjectPath
  ;; ObjectPath ::= GraphNodePath
  ;; <GraphNodePath> ::= VarOrTerm | TriplesNodePath
  ;; TriplesNodePath  ::=  CollectionPath WS | BlankNodePropertyListPath WS
  ;; CollectionPath ::= '(' GraphNodePath+ ')'
  ;; BlankNodePropertyListPath ::= '[' PropertyListPathNotEmpty ']'
  [[_ & objs]]
  (mapv parse-term objs))

(defmethod parse-term :PropertyListPathNotEmpty
  ;; PropertyListPathNotEmpty ::= ( VerbPath | VerbSimple ) ObjectListPath ( <';'> WS ( ( VerbPath | VerbSimple ) ObjectList )? )* WS
  ;; <VerbPath> ::= Path
  ;; <Path> ::= PathAlternative
  ;; <PathAlternative> ::= PathSequence ( <'|'> PathSequence )*
  ;; <VerbSimple> ::= Var
  ;; <ObjectListPath> ::= ObjectPath WS ( <',' WS> ObjectPath )*
  [[_ & expressions]]
  (->> (mapv parse-term expressions)
       (reduce (fn [plist expr]
                 (if (string? expr)
                   (conj plist expr)
                   (into plist expr)))
               [])))

(defmethod parse-term :TriplesSameSubjectPath
  ;; TriplesSameSubjectPath ::= VarOrTerm PropertyListPathNotEmpty | TriplesNodePath PropertyListPath WS
  ;; <VarOrTerm> ::= Var | GraphTerm WS
  ;; <GraphTerm> ::= iri | RDFLiteral | NumericLiteral | BooleanLiteral | BlankNode | NIL
  [[_ subject properties]]
  (let [s (parse-term subject)]
    (->> (partition-all 2 (parse-term properties))
         (mapv (fn [[p o]] {"@id" s p o})))))

(defmethod parse-term :TriplesBlock
  ;; TriplesBlock ::= WS TriplesSameSubjectPath WS ( <'.'> TriplesBlock? WS )?
  [[_ subject-path triples-block :as r]]
  (cond-> (parse-term subject-path)
    triples-block (concat (parse-term triples-block))))

(defmethod parse-term :GroupOrUnionGraphPattern
  ;; GroupOrUnionGraphPattern ::= GroupGraphPattern ( <'UNION'> GroupGraphPattern )*
  [[_ & union-patterns]]
  (into [:union] (mapcat parse-term union-patterns)))

(defmethod parse-term :GroupGraphPatternSub
  ;; GroupGraphPatternSub ::= WS TriplesBlock? ( GraphPatternNotTriples WS <'.'?> TriplesBlock? WS )* WS
  [[_ & patterns]]
  (mapcat parse-term patterns))

(defmethod parse-term :SubSelect
  [r]
  (throw (ex-info "SubSelect patterns are not supported"
                  {:status 400 :error :db/invalid-query})))

(defmethod parse-rule :WhereClause
  ;; WhereClause ::= <'WHERE'?> WS GroupGraphPattern WS
  ;; <GroupGraphPattern> ::= WS <'{'> WS ( SubSelect | GroupGraphPatternSub ) WS <'}'> WS
  [[_ & patterns]]
  [[:where (into [] (mapcat parse-term patterns))]])

(defmethod parse-rule :OffsetClause
  ;; OffsetClause ::= <'OFFSET'> WS INTEGER
  [[_ offset]]
  [[:offset (read-string offset)]])

(defmethod parse-rule :LimitClause
  ;; LimitClause ::= <'LIMIT'> WS INTEGER
  [[_ limit]]
  [[:limit (read-string limit)]])

(defmethod parse-term :ExplicitOrderCondition
  ;; ExplicitOrderCondition ::= ( 'ASC' | 'DESC' ) WS BrackettedExpression
  [[_ order expr]]
  [(str/lower-case order) (parse-term expr)])

(defmethod parse-rule :OrderClause
  ;; OrderClause ::= <'ORDER' WS 'BY'> WS OrderCondition+ WS
  ;; <OrderCondition> ::= ExplicitOrderCondition | Constraint | Var
  [[_ & conditions]]
  (if (> (count conditions) 1)
    (throw (ex-info "Multiple ORDER BY conditions are not supported"
                    {:status 400 :error :db/invalid-query}))
    [[:orderBy (first
                 (mapv (fn [condition]
                         (cond (= "ASC" (first condition)) ["asc" (parse-term (second condition))]
                               (= "DESC" (first condition)) ["desc" (parse-term (second condition))]
                               :else
                               (parse-term condition)))
                       conditions))]]))

(defmethod parse-term :GroupCondition
  ;; GroupCondition ::= BuiltInCall | FunctionCall | <'('> Expression ( WS 'AS' WS Var )? <')'> | Var
  [[_ expr as var :as condition]]
  (if (= as "AS")
    (str "(as " (parse-term expr) " " (parse-term var) ")")
    (parse-term expr)))

(defmethod parse-rule :GroupClause
  ;; GroupClause ::= <'GROUP' WS 'BY' WS> GroupCondition+
  [[_ & conditions]]
  [[:groupBy (mapv parse-term conditions)]])

(defmethod parse-term :HavingCondition
  ;; HavingCondition ::= Constraint
  [[_ & conditions]]
  (mapv parse-term conditions))

(defmethod parse-rule :HavingClause
  ;; HavingClause ::= <'HAVING'> HavingCondition+
  [[_ & conditions]]
  [(into [:having] (mapcat parse-term conditions))])

(defmethod parse-rule :SolutionModifier
  ;; SolutionModifier ::= GroupClause? HavingClause? OrderClause? LimitOffsetClauses?
  ;; <LimitOffsetClauses>    ::=   LimitClause OffsetClause? | OffsetClause LimitClause?
  [[_ & modifiers]]
  (mapcat parse-rule modifiers))

(defmethod parse-rule :SelectQuery
  ;; SelectQuery ::= WS SelectClause WS DatasetClause* WS WhereClause WS SolutionModifier WS
  [[_ & select-query]]
  (reduce (fn [entries rule] (into entries (parse-rule rule)))
          []
          select-query))

(defmethod parse-rule :PrettyPrint
  [_]
  [[:prettyPrint true]])

(defmethod parse-rule :Modifiers
  [[_ & modifiers]]
  (mapcat parse-rule modifiers))

(defn parse-stage-2
  [parsed]
  (def parsed parsed)
  (reduce (fn [fql rule]
            (let [entries (parse-rule rule)]
              (println "DEP parsed" (pr-str entries))
              (into fql entries)))
          {}
          parsed))

(def grammar #?(:clj  (io/resource "sparql2.bnf")
                :cljs (inline-resource "sparql2.bnf")))

(defparser parser grammar)

(defn parse-stage-1
  [sparql]
  (let [parsed (parser sparql)]
    (if (insta/failure? parsed)
      (throw (ex-info (str "Improperly formatted SPARQL query: " sparql)
                      {:status   400 :error    :db/invalid-query}))
      parsed)))

(comment
  (def parsed
    (parse-stage-1
      "PREFIX person: <http://example.org/Person#>
                          SELECT (CONCAT(?handle, '-', ?fullName) AS ?hfn)
                          WHERE {?person person:handle ?handle.
                                 ?person person:fullName ?fullName.}"))

  (-> (parse-stage-1 "PREFIX person: <http://example.org/Person#>
                          SELECT ?handle
                          WHERE {?person person:handle ?handle.}
                          ORDER BY DESC(?handle)")

      (parse-stage-2))

  parsed


  ,)
