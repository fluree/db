(ns fluree.db.query.sparql.translator
  (:require [fluree.db.constants :as const]
            [clojure.string :as str]
            #?(:cljs [cljs.tools.reader :refer [read-string]])))

(defn rule?
  [x]
  (and (sequential? x)
       (keyword? (first x))))

(defn literal-quote
  "Quote a non-variable string literal for use in an expression."
  [x]
  (if (and (string? x)
           (not (str/starts-with? x "?")))
    (str "\"" x "\"")
    x))

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

(defmethod parse-term :ExistsFunc
  [r]
  (throw (ex-info "EXISTS is not a supported SPARQL clause"
                  {:status 400 :error :db/invalid-query})))

(defmethod parse-term :NotExistsFunc
  [r]
  (throw (ex-info "NOT EXISTS is not a supported SPARQL clause"
                  {:status 400 :error :db/invalid-query})))

(defmethod parse-term :RegexExpression
  ;; RegexExpression ::= <'REGEX'> <'('> Expression <','> Expression ( <','> Expression )? <')'>
  [[_ text pattern flags]]
  (str "(regex " (literal-quote (parse-term text)) " " (literal-quote (parse-term pattern))
       (when flags (str " " (literal-quote (parse-term flags)))) ")"))

(defmethod parse-term :SubstringExpression
  ;; SubstringExpression ::= <'SUBSTR'> <'('> Expression <','> Expression ( <','> Expression )? <')'>
  [[_ source starting-loc length]]
  (str "(subStr " (literal-quote (parse-term source)) " " (parse-term starting-loc)
       (when length (str " " (parse-term length))) ")"))

(defmethod parse-term :StrReplaceExpression
  ;; StrReplaceExpression ::= <'REPLACE'> <'('> Expression <','> Expression <','> Expression ( <','> Expression )? <')'>
  [[_ arg pattern replacement flags]]
  (str "(replace " (literal-quote (parse-term arg)) " " (literal-quote (parse-term pattern))
       " " (literal-quote (parse-term replacement))
       (when flags (str " " (literal-quote (parse-term flags)))) ")"))

(defmethod parse-term :ExpressionList
  ;; ExpressionList ::= NIL | <'('> Expression ( <','> Expression )* <')'>
  [[_ & expressions]]
  (mapv parse-term expressions))

(def supported-scalar-functions
  {"ABS"            "abs"
   "BNODE"          "bnode"
   "BOUND"          "bound"
   "CEIL"           "ceil"
   "COALESCE"       "coalesce"
   "CONCAT"         "concat"
   "CONTAINS"       "contains"
   "DATATYPE"       "datatype"
   "DAY"            "day"
   "ENCODE_FOR_URI" "encodeForUri"
   "FLOOR"          "floor"
   "HOURS"          "hours"
   "IF"             "if"
   "IRI"            "iri"
   "LANG"           "lang"
   "LANGMATCHES"    "langMatches"
   "LCASE"          "lcase"
   "MD5"            "md5"
   "MINUTES"        "minutes"
   "MONTH"          "month"
   "NOW"            "now"
   "RAND"           "rand"
   "ROUND"          "round"
   "SECONDS"        "seconds"
   "SHA1"           "sha1"
   "SHA256"         "sha256"
   "SHA512"         "sha512"
   "STR"            "str"
   "STRENDS"        "strEnds"
   "STRLEN"         "count"
   "STRSTARTS"      "strStarts"})

(defmethod parse-term :Func
  [[_ func & args]]
  (let [f (get supported-scalar-functions func)]
    (case f
      "abs"          (str "(" f " " (str/join " " (mapv (comp literal-quote parse-term) args)) ")")
      "bnode"        (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "bound"        (str "(" f " " (parse-term (first args)) ")")
      "ceil"         (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "coalesce"     (str "(" f " " (str/join " " (->> (parse-term (first args)) (mapv literal-quote))) ")")
      "concat"       (str "(" f " " (str/join " " (->> (parse-term (first args)) (mapv literal-quote))) ")")
      "contains"     (str "(" f " " (literal-quote (parse-term (first args))) " "
                          (literal-quote (parse-term (first (next args)))) ")")
      "datatype"     (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "day"          (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "encodeForUri" (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "floor"        (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "hours"        (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "if"           (str "(" f " " (literal-quote (parse-term (first args))) " "
                          (literal-quote (parse-term (first (next args)))) " "
                          (literal-quote (parse-term (first (nnext args)))) ")")
      "iri"          (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "lang"         (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "langMatches"  (str "(" f " " (literal-quote (parse-term (first args))) " "
                          (literal-quote (parse-term (first (next args)))) ")")
      "lcase"        (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "md5"          (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "minutes"      (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "month"        (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "now"          (str "(" f ")")
      "rand"         (str "(" f ")")
      "round"        (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "seconds"      (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "sha1"         (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "sha256"       (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "sha512"       (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "str"          (str "(" f " " (literal-quote (parse-term (first args))) ")")
      (throw (ex-info (str "Unsupported function: " func)
                      {:status 400 :error :db/invalid-query})))))

(defmethod parse-term :NumericLiteral
  ;; NumericLiteral   ::=   NumericLiteralUnsigned WS | NumericLiteralPositive WS | NumericLiteralNegative WS
  ;; <NumericLiteralUnsigned>   ::=   INTEGER | DECIMAL | DOUBLE
  ;; <NumericLiteralPositive>   ::=   INTEGER_POSITIVE | DECIMAL_POSITIVE | DOUBLE_POSITIVE
  ;; <NumericLiteralNegative>   ::=   INTEGER_NEGATIVE | DECIMAL_NEGATIVE | DOUBLE_NEGATIVE
  ;; <INTEGER_POSITIVE>   ::=   '+' INTEGER
  ;; <DECIMAL_POSITIVE>   ::=   '+' DECIMAL
  ;; <DOUBLE_POSITIVE>    ::=   '+' DOUBLE
  ;; <INTEGER_NEGATIVE>   ::=   '-' INTEGER
  ;; <DECIMAL_NEGATIVE>   ::=   '-' DECIMAL
  ;; <DOUBLE_NEGATIVE>    ::=   '-' DOUBLE
  ;; <INTEGER>    ::=   #"[0-9]+"
  ;; <DECIMAL>    ::=  #"[0-9]*\.[0-9]*"
  ;; <DOUBLE>   ::=   #"[0-9]+\.[0-9]*|(\.[0-9]+)|([0-9]+)" EXPONENT
  ;; EXPONENT   ::=   #"[eE][+-]?[0-9]+"
  [[_ sign num-str]]
  (read-string (str sign num-str)))

(defmethod parse-term :MultiplicativeExpression
  ;; MultiplicativeExpression ::= UnaryExpression ( '*' UnaryExpression | '/' UnaryExpression )*
  ;; <UnaryExpression> ::= '!' PrimaryExpression
  ;; | '+' PrimaryExpression
  ;; | '-' PrimaryExpression
  ;; | PrimaryExpression
  ;; <PrimaryExpression> ::= BrackettedExpression | BuiltInCall | iriOrFunction | RDFLiteral | NumericLiteral | BooleanLiteral | Var
  [[_ & [expr0 & [op1 expr1 & exprs]]]]
  (if op1
    ;; we need to recursively compose expressions
    (loop [[op expr & r] exprs
           result  (str "(" op1 " " (parse-term expr0) " " (parse-term expr1) ")")]
      (if (and op expr)
        (recur r (str "(" op " " result " " (parse-term expr) ")"))
        result))
    ;; A single UnaryExpression doesn't need to be composed with anything else
    (parse-term expr0)))

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
  [[_ constraint]]
  [:filter [(parse-term constraint)]])

(defmethod parse-term :OptionalGraphPattern
  ;; OptionalGraphPattern ::= <'OPTIONAL'> GroupGraphPattern
  [[_ & optional]]
  (into [:optional] (mapv parse-term optional)))

(defmethod parse-term :DataBlockValue
  ;; DataBlockValue ::= iri | RDFLiteral | NumericLiteral | BooleanLiteral | 'UNDEF' WS
  [[_ [tag :as value]]]
  (cond
    ;; iri values need to be wrapped in a value-map
    (= tag :iri) {const/iri-type const/iri-anyURI const/iri-value (parse-term value)}
    (= value "UNDEF")      nil
    :else                  (parse-term value)))

(defmethod parse-term :VarList
  ;; VarList ::= ( <'('> Var* <')'> )
  [[_ & vars]]
  (mapv parse-term vars))

(defmethod parse-term :ValueList
  ;; ValueList ::= ( <'('> WS DataBlockValue* <')'> )
  [[_ & values]]
  (mapv parse-term values))

(defmethod parse-term :InlineDataFull
  ;; InlineDataFull ::= ( NIL | VarList ) WS <'{'> WS ( ValueList WS | NIL )* <'}'>
  [[_ vars & data]]
  [:values [(parse-term vars)] (mapv parse-term data)])

(defmethod parse-term :InlineDataOneVar
  ;; InlineDataOneVar ::= Var <'{'> WS DataBlockValue* <'}'>
  [[_ var & data-block-values]]
  [:values [(parse-term var) (mapv parse-term data-block-values)]])

(defmethod parse-term :InlineData
  ;; InlineData ::= <'VALUES'> WS DataBlock
  ;; <DataBlock> ::= InlineDataOneVar | InlineDataFull
  [[_ inline-data]]
  (parse-term inline-data))

(defmethod parse-term :GraphPatternNotTriples
  ;; GraphPatternNotTriples ::= GroupOrUnionGraphPattern | OptionalGraphPattern | MinusGraphPattern | GraphGraphPattern | ServiceGraphPattern | Filter | Bind | InlineData
  [[_ & non-triples]]
  (mapv parse-term non-triples))

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
  ;; TODO: this does nothing in FQL
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

(defmethod parse-rule :ValuesClause
  ;; ValuesClause ::= ( <'VALUES'> WS DataBlock )? WS
  [[_ datablock]]
  [(parse-term datablock)])

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
  (list (str/lower-case order) (parse-term expr)))

(defmethod parse-rule :OrderClause
  ;; OrderClause ::= <'ORDER' WS 'BY'> WS OrderCondition+ WS
  ;; <OrderCondition> ::= ExplicitOrderCondition | Constraint | Var
  [[_ & conditions]]
  [[:orderBy (mapv (fn [condition]
                     (cond (= "ASC" (first condition)) (list "asc" (parse-term (second condition)))
                           (= "DESC" (first condition)) (list "desc" (parse-term (second condition)))
                           :else
                           (parse-term condition)))
                   conditions)]])

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

(defn translate
  [parsed]
  (->> parsed
       (reduce (fn [fql rule] (into fql (parse-rule rule))) {})))
