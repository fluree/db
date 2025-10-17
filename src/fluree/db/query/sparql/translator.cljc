(ns fluree.db.query.sparql.translator
  (:require #?(:cljs [cljs.tools.reader :refer [read-string]])
            [clojure.string :as str]
            [fluree.db.constants :as const]))

(def bnode-counter (atom 0))

(defn new-bnode!
  []
  (str "_:b" (swap! bnode-counter inc)))

(defn rule?
  [x]
  (and (sequential? x)
       (keyword? (first x))))

(defn literal-quote
  "Quote a non-variable, non-expression string literal for use in an expression."
  [x]
  (if (and (string? x)
           (not (re-matches #"^\(.+\)$" x))
           (not (str/starts-with? x "?")))
    (str "\"" x "\"")
    x))

(defmulti parse-term
  "Accepts a term rule and returns a value."
  (fn [[tag & _]] tag))

(defmulti parse-rule
  "Accepts a rule and returns a sequence of entry tuples [[k v]...]."
  (fn [[tag & _]] tag))

(defmethod parse-term :BooleanLiteral
  [[_ bool]]
  (case bool
    "false" false
    "true" true))

(defmethod parse-term :ANON
  [_]
  (new-bnode!))

(defmethod parse-term :Var
  ;; Var ::= VAR1 WS | VAR2 WS
  ;; [:Var "n" "u" "m" "s"]
  [[_ & var]]
  (str "?" (str/join var)))

(defmethod parse-term :Separator
  [[_ & separator-chars]]
  (apply str separator-chars))

(def supported-aggregate-functions
  {"MAX"       "max"
   "MIN"       "min"
   "SAMPLE"    "sample1"
   "COUNT"     "count"
   "SUM"       "sum"
   "AVG"       "avg"
   "GROUP_CONCAT" "groupconcat"})

(defmethod parse-term :Wildcard [_] "*")

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
          [mset scalarvals] (if distinct? (rest body) body)]
      (str "(" f " " (parse-term mset) (when scalarvals (str " " (literal-quote (parse-term scalarvals)))) ")"))
    (throw (ex-info (str "Unsupported aggregate function: " func)
                    {:status 400 :error :db/invalid-query}))))

(defmethod parse-term :ExistsFunc
  [[_ & patterns]]
  ["exists" (into [] (mapcat parse-term patterns))])

(defmethod parse-term :NotExistsFunc
  [[_ & patterns]]
  ["not-exists" (into [] (mapcat parse-term patterns))])

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
  {"abs"            "abs"
   "bnode"          "bnode"
   "bound"          "bound"
   "ceil"           "ceil"
   "coalesce"       "coalesce"
   "concat"         "concat"
   "contains"       "contains"
   "datatype"       "datatype"
   "day"            "day"
   "encode_for_uri" "encodeForUri"
   "floor"          "floor"
   "hours"          "hours"
   "if"             "if"
   "iri"            "iri"
   "lang"           "lang"
   "langmatches"    "langMatches"
   "lcase"          "lcase"
   "md5"            "md5"
   "minutes"        "minutes"
   "month"          "month"
   "now"            "now"
   "rand"           "rand"
   "round"          "round"
   "seconds"        "seconds"
   "sha1"           "sha1"
   "sha256"         "sha256"
   "sha512"         "sha512"
   "str"            "str"
   "strafter"       "strAfter"
   "strbefore"      "strBefore"
   "strdt"          "strDt"
   "strends"        "strEnds"
   "strlang"        "strLang"
   "strlen"         "strLen"
   "strstarts"      "strStarts"
   "struuid"        "struuid"
   "timezone"       "timezone"
   "tz"             "tz"
   "ucase"          "ucase"
   "uri"            "uri"
   "uuid"           "uuid"
   "year"           "year"
   "isblank"        "isBlank"
   "isiri"          "isIri"
   "isliteral"      "isLiteral"
   "isnumeric"      "isNumeric"
   "isuri"          "isUri"
   "sameterm"       "sameTerm"})

(defmethod parse-term :Func
  [[_ func & args]]
  (let [f (get supported-scalar-functions (str/lower-case func))]
    (case f
      "abs"          (str "(" f " " (parse-term (first args)) ")")
      "bnode"        (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "bound"        (str "(" f " " (parse-term (first args)) ")")
      "ceil"         (str "(" f " " (parse-term (first args)) ")")
      "coalesce"     (str "(" f " " (str/join " " (->> (parse-term (first args)) (mapv literal-quote))) ")")
      "concat"       (str "(" f " " (str/join " " (->> (parse-term (first args)) (mapv literal-quote))) ")")
      "contains"     (str "(" f " " (literal-quote (parse-term (first args))) " "
                          (literal-quote (parse-term (first (next args)))) ")")
      "datatype"     (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "day"          (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "encodeForUri" (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "floor"        (str "(" f " " (parse-term (first args)) ")")
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
      "round"        (str "(" f " " (parse-term (first args)) ")")
      "seconds"      (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "sha1"         (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "sha256"       (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "sha512"       (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "str"          (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "strAfter"     (str "(" f " " (literal-quote (parse-term (first args))) " "
                          (literal-quote (parse-term (first (next args)))) ")")
      "strBefore"    (str "(" f " " (literal-quote (parse-term (first args))) " "
                          (literal-quote (parse-term (first (next args)))) ")")
      "strDt"        (str "(" f " " (literal-quote (parse-term (first args))) " "
                          (literal-quote (parse-term (first (next args)))) ")")
      "strEnds"      (str "(" f " " (literal-quote (parse-term (first args))) " "
                          (literal-quote (parse-term (first (next args)))) ")")
      "strLang"      (str "(" f " " (literal-quote (parse-term (first args))) " "
                          (literal-quote (parse-term (first (next args)))) ")")
      "strLen"       (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "strStarts"    (str "(" f " " (literal-quote (parse-term (first args))) " "
                          (literal-quote (parse-term (first (next args)))) ")")
      "struuid"      (str "(" f ")")
      "timezone"     (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "tz"           (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "ucase"        (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "uri"          (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "uuid"         (str "(" f ")")
      "year"         (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "isBlank"      (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "isIri"        (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "isLiteral"    (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "isNumeric"    (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "isUri"        (str "(" f " " (literal-quote (parse-term (first args))) ")")
      "sameTerm"     (str "(" f " " (literal-quote (parse-term (first args))) " "
                          (literal-quote (parse-term (first (next args)))) ")")
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

(defmethod parse-term :iriOrFunction
  ;; iriOrFunction ::= iri ArgList?
  [[_ iri arglist]]
  (when arglist
    (throw (ex-info "Unsupported syntax."
                    {:status 400 :error :db/invalid-query :term arglist})))
  {const/iri-id (parse-term iri)})

(defmethod parse-term :UnaryExpression
  [[_ op-or-expr expr]]
  (condp = op-or-expr
    "!" (str "(not " (parse-term expr) ")")
    "+" (str "+" (parse-term expr) ")")
    "-" (str "-" (parse-term expr) ")")
    (parse-term op-or-expr)))

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
      (str "(in " (literal-quote expr) " " (literal-quote (parse-term op-or-exp)) ")")

      (and (= "NOT" op)
           (= "IN" op-or-exp))
      (str "(not (in " (literal-quote expr) " " (literal-quote (parse-term expr-list)) "))")

      (nil? op)
      expr

      (= "!=" op)
      (str "(not= " (literal-quote expr) " " (literal-quote (parse-term op-or-exp)) ")")

      :else
      ;; op: =, <, >, <=, >=
      (str "(" op " " (literal-quote expr) " " (literal-quote (parse-term op-or-exp)) ")"))))

(defmethod parse-term :ConditionalOrExpression
  ;; ConditionalOrExpression ::= ConditionalAndExpression ( <'||'> ConditionalAndExpression )*
  [[_ expr & exprs]]
  (if (seq exprs)
    (str "(or " (parse-term expr) " " (str/join " " (mapv parse-term exprs)) ")")
    (parse-term expr)))

(defmethod parse-term :ConditionalAndExpression
  ;; ConditionalAndExpression ::= ValueLogical ( <'&&'> ValueLogical )*
  ;; <ValueLogical> ::= RelationalExpression
  [[_ expr & exprs]]
  (if (seq exprs)
    (str "(and " (parse-term expr) " " (str/join " " (mapv parse-term exprs)) ")")
    (parse-term expr)))

(defmethod parse-term :Expression
  ;; Expression ::= WS ConditionalOrExpression WS
  ;; <ConditionalOrExpression> ::= ConditionalAndExpression ( <'||'> ConditionalAndExpression )*
  ;; <ConditionalAndExpression> ::= ValueLogical ( <'&&'> ValueLogical )*
  ;; <ValueLogical> ::= RelationalExpression
  [[_ & expression]]
  (let [expressions (mapv parse-term expression)]
    (if (= 1 (count expressions))
      (first expressions)
      expressions)))

(defmethod parse-term :IRIREF
  ;; #"<[^<>\"{}|^`\x00-\x20]*>" WS
  ;; [:IRIREF <ex:example>]
  [[_ iri]]
  (subs iri 1 (-> iri count dec)))

(defmethod parse-term :BLANK_NODE_LABEL
  [[_ & bnode-chars]]
  (str/join bnode-chars))

(defmethod parse-rule :PrefixDecl
  ;; PrefixDecl ::= <'PREFIX'> WS PNAME_NS WS IRIREF
  ;; [:PrefixDecl "e" "x" ":" [:IRIREF "<http://example.com/>"]]
  [[_ & prefix-decl]]
  (let [prefix  (->> (drop-last 2 prefix-decl) vec str/join not-empty)
        ;; sometimes no prefix is specified: PREFIX : <my:IRI> and
        ;; all IRIs are just prefixed with a colon
        prefix* (or prefix ":")
        iri     (->> prefix-decl
                     (drop-while (comp not sequential?))
                     first
                     parse-term)]
    [[prefix* iri]]))

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
        ;; either multiple terms or a single wildcard
        (cond (rule? term)
              (let [[tag & _body] term]
                (if (= tag :Var)
                  (recur r (conj result (parse-term term)))
                  ;; :Expression
                  (let [[next next-next & r*] r
                        expr (parse-term term)
                        as?  (= [:As] next)
                        expr (if as?
                               (str "(as " expr " " (parse-term next-next) ")")
                               expr)]
                    (recur (if as? r* r) (conj result expr)))))

              (= "*" term)
              (recur nil term))
        [[select-key result]]))))

(defmethod parse-term :PropertyObjectList
  ;; PropertyObjectList ::= Verb ObjectList
  ;; <Verb> ::= VarOrIri | Type
  [[_ verb objects]]
  (let [p (parse-term verb)]
    (mapv #(vector p (if (and (= const/iri-type p)
                              (not= \? (first %)))
                       (get % const/iri-id)
                       %))
          (parse-term objects))))

(defmethod parse-term :PropertyListNotEmpty
  ;; PropertyListNotEmpty ::= PropertyObjectList ( <';'>  WS ( PropertyObjectList )? )*
  [[_ & properties]]
  (mapcat parse-term properties))

(defmethod parse-term :PropertyList
  ;; PropertyList ::= PropertyListNotEmpty?
  [[_ plist]]
  (if plist
    (parse-term plist)
    []))

(defmethod parse-term :TriplesSameSubject1
  ;; TriplesSameSubject1 ::= VarOrTerm PropertyListNotEmpty
  [[_ subject properties]]
  (let [s (parse-term subject)]
    (mapv (fn [[p o]] {"@id" s p o}) (parse-term properties))))

(defmethod parse-term :TriplesSameSubject2
  ;; TriplesSameSubject1 ::= TriplesNode PropertyList
  [[_ node plist]]
  [(into (parse-term node) (parse-term plist))])

(defmethod parse-rule :ConstructTemplate
  ;; ConstructTemplate   ::=   <'{'> WS ConstructTriples? WS <'}'> WS
  ;; <ConstructTriples>    ::=   TriplesSameSubject ( WS <'.'> WS ConstructTriples? )?
  [[_ & construct-triples]]
  [[:construct (vec (mapcat parse-term construct-triples))]])

(defmethod parse-rule :ConstructWhereTemplate
  ;; ConstructWhereTemplate ::= <'{'> WS TriplesTemplate? <'}'>
  [[_ & construct-triples]]
  (let [triples (vec (mapcat parse-term construct-triples))]
    [[:construct triples] [:where triples]]))

(defmethod parse-term :DefaultGraphClause
  ;; DefaultGraphClause ::= SourceSelector
  ;; <SourceSelector> ::= iri
  [[_ source]]
  (parse-term source))

(defmethod parse-term :NamedGraphClause
  ;; NamedGraphClause ::= <'NAMED'> SourceSelector
  ;; <SourceSelector> ::= iri
  [[_ source]]
  (parse-term source))

(defmethod parse-term :GraphGraphPattern
  ;; GraphGraphPattern ::= <'GRAPH'> VarOrIri GroupGraphPattern
  [[_ & [var-or-iri group-graph-pattern]]]
  [:graph (parse-term var-or-iri) (into [] (parse-term group-graph-pattern))])

(defmethod parse-term :VarOrIri
  ;; <VarOrIri> ::= Var | iri WS
  [[_ var-or-iri]]
  (parse-term var-or-iri))

(defmethod parse-rule :DatasetClause
  ;; DatasetClause ::= FromClause*
  ;; <FromClause>  ::= <'FROM'> WS ( DefaultGraphClause | NamedGraphClause )
  [[_ & clauses]]
  (let [{from  :DefaultGraphClause
         named :NamedGraphClause}
        (group-by first clauses)]
    (cond-> []
      (seq from)  (conj [:from (mapv parse-term from)])
      (seq named) (conj [:from-named (mapv parse-term named)]))))

(defmethod parse-term :LANGTAG
  ;; LANGTAG ::= #"@[a-zA-Z]+(-[a-zA-Z0-9]+)*" WS
  [[_ langstr]]
  ;; just drop the @ prefix
  (subs langstr 1))

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
  (let [parsed-constraint (parse-term constraint)]
    (if (contains? #{"exists" "not-exists"} (first parsed-constraint))
      parsed-constraint
      [:filter parsed-constraint])))

(defmethod parse-term :OptionalGraphPattern
  ;; OptionalGraphPattern ::= <'OPTIONAL'> GroupGraphPattern
  [[_ & optional]]
  (into [:optional] (mapcat parse-term optional)))

(defmethod parse-term :MinusGraphPattern
  [[_ & patterns]]
  (into [:minus] (mapv parse-term patterns)))

(defmethod parse-term :ServiceClause
  [[_ & chars]]
  (apply str chars))

(defn service-pattern
  [silent? [service clause]]
  [:service {:silent? silent? :service (parse-term service) :clause (parse-term clause)}])

(defmethod parse-term :ServiceGraphPattern
  ;; ServiceGraphPattern ::= <'SERVICE'> WS 'SILENT'? WS VarOrIri GroupGraphPattern
  [[_ & terms]]
  (if (= "SILENT" (first terms))
    (service-pattern true (rest terms))
    (service-pattern false terms)))

(defmethod parse-term :DataBlockValue
  ;; DataBlockValue ::= iri | RDFLiteral | NumericLiteral | BooleanLiteral | 'UNDEF' WS
  [[_ [tag :as value]]]
  (cond
    ;; iri values need to be wrapped in a value-map
    (= tag :iri) {const/iri-type const/iri-id const/iri-value (parse-term value)}
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
  [:values [(parse-term vars) (mapv parse-term data)]])

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
  (reduce (fn [results non-triple]
            (if (= :GroupOrUnionGraphPattern (first non-triple))
              (into results (parse-term non-triple))
              (conj results (parse-term non-triple))))
          []
          non-triples))

(defmethod parse-term :PrefixedName
  [[_ & name]]
  (apply str name))

(defmethod parse-term :iri
  ;; iri ::= ( IRIREF | PrefixedName ) WS
  [[_ iri]]
  (parse-term iri))

(defmethod parse-term :Type
  [_]
  const/iri-type)

(defmethod parse-term :PathPrimary
  ;; PathPrimary    ::=   iri | Type | '!' PathNegatedPropertySet | '(' Path ')'
  ;; PathNegatedPropertySet   ::=   PathOneInPropertySet | '(' ( PathOneInPropertySet ( '|' PathOneInPropertySet )* )? ')'
  ;; PathOneInPropertySet   ::=   iri | Type | '^' ( iri | Type )
  ;; Type ::= (WS <'a'> WS)
  [[_ el]]
  (cond (rule? el) (parse-term el)
        (= el "a") const/iri-type
        :else
        (throw (ex-info (str "Non-predicate paths are not supported: " el)
                        {:status 400 :error :db/invalid-query}))))

(defmethod parse-term :PathMod
  ;; PathMod ::= '?' WS | '*' WS | ('+' INTEGER?) WS
  [[_ mod degree]]
  (if degree
    (throw (ex-info "Depth modifiers on transitive path elements are not supported."
                    {:status 400 :error :db/invalid-query}))
    (str mod degree)))

(defmethod parse-term :PathElt
  [[_ primary mod]]
  (if mod
    (let [term  (parse-term primary)
          term* (if ((set (flatten primary)) :IRIREF)
                  ;; expanded IRIs need to be wrapped in angle brackets in a transitive path
                  (str "<" term ">")
                  term)]
      (str "<" term* (parse-term mod) ">"))
    (parse-term primary)))

(defmethod parse-term :PathSequence
  ;; PathSequence ::= PathEltOrInverse ( <'/'> PathEltOrInverse )*
  ;; <PathEltOrInverse> ::= PathElt | '^' PathElt
  ;; PathElt ::= PathPrimary PathMod?
  [[_ & elements]]
  (apply str (mapv parse-term elements)))

(defmethod parse-term :Object
  ;; Object   ::=   GraphNode
  ;; <GraphNode>    ::=   VarOrTerm | TriplesNode
  [[_ [tag :as obj]]]
  (if (= tag :iri)
    {const/iri-id (parse-term obj)}
    (parse-term obj)))

(defmethod parse-term :ObjectList
  ;; ObjectList ::= Object ( <','> WS Object )*
  ;; Object ::= GraphNode
  ;; <GraphNode> ::= VarOrTerm | TriplesNode
  ;; <VarOrTerm> ::= Var | GraphTerm WS
  ;; <TriplesNode> ::= Collection | BlankNodePropertyList
  ;; Collection ::=  '(' GraphNode+ ')'
  [[_ & path]]
  (mapv parse-term path))

(defmethod parse-term :BlankNodePropertyList
  ;; BlankNodePropertyList ::= <'['> PropertyListNotEmpty <']'>
  [[_ plist]]
  (into {const/iri-id (new-bnode!)} (parse-term plist)))

(defmethod parse-term :BlankNodePropertyListPath
  ;; BlankNodePropertyListPath ::= <'['> PropertyListPathNotEmpty <']'>
  [[_ plist]]
  (->> (partition-all 2 (parse-term plist))
       (mapv (fn [[p o]] (if (= p const/iri-type) [p (get o const/iri-id)] [p o])))
       (into {const/iri-id (new-bnode!)})))

(defmethod parse-term :ObjectPath
  ;; ObjectPath ::= GraphNodePath
  ;; <GraphNodePath> ::= VarOrTerm | TriplesNodePath
  ;; <TriplesNodePath>  ::=  CollectionPath WS | BlankNodePropertyListPath WS
  ;; CollectionPath ::= '(' GraphNodePath+ ')'
  [[_ & objs]]
  (mapv (fn [[tag :as obj]]
          (if (= tag :iri)
            {const/iri-id (parse-term obj)}
            (parse-term obj)))
        objs))

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
         (reduce (fn [triples [p o]]
                   (into triples (if (and (get o const/iri-id) (> (count o) 1))
                                   ;; unpack nested object into a separate triple with ref
                                   [{"@id" s p (select-keys o [const/iri-id])} o]
                                   [{"@id" s p o}]))) []))))

(defmethod parse-term :TriplesBlock
  ;; TriplesBlock ::= WS TriplesSameSubjectPath WS ( <'.'> TriplesBlock? WS )?
  [[_ subject-path triples-block]]
  (cond-> (parse-term subject-path)
    triples-block (concat (parse-term triples-block))))

(defmethod parse-term :GroupOrUnionGraphPattern
  ;; GroupOrUnionGraphPattern ::= GroupGraphPattern ( <'UNION'> GroupGraphPattern )*
  [[_ group-pattern & union-patterns]]
  (if union-patterns
    (let [all-patterns (cons (parse-term group-pattern) (map parse-term union-patterns))]
      (reduce (fn [a g]
                (if (= (first a) :union)
                  [:union [a] (vec g)]
                  [:union (vec a) (vec g)]))
              (first all-patterns)
              (rest all-patterns)))
    (parse-term group-pattern)))

(defmethod parse-term :GroupGraphPatternSub
  ;; GroupGraphPatternSub ::= WS TriplesBlock? ( GraphPatternNotTriples WS <'.'?> TriplesBlock? WS )* WS
  [[_ & patterns]]
  (mapcat (fn [pattern]
            (let [pattern (parse-term pattern)]
              ;; don't flatten union patterns
              (if (= :union (first pattern))
                [pattern]
                pattern)))
          patterns))

(declare translate)
(defmethod parse-term :SubSelect
  ;; SubSelect ::= SelectClause WS WhereClause WS SolutionModifier WS ValuesClause
  [[_ & subquery-clauses]]
  ;; SubSelect is always nested under GroupOrUnionGraphPattern, which returns a sequence of results
  ;; so we need to wrap it in an extra vector
  [[:query (translate subquery-clauses)]])

(defmethod parse-rule :WhereClause
  ;; WhereClause ::= <'WHERE'?> WS GroupGraphPattern WS
  ;; <GroupGraphPattern> ::= WS <'{'> WS ( SubSelect | GroupGraphPatternSub ) WS <'}'> WS
  [[_ & patterns]]
  (let [result (into [] (mapcat parse-term patterns))
        ;; If first element of result is a keyword, wrap result in a vector
        ;; Otherwise, return result
        result* (if (keyword? (first result)) [result] result)]
    [[:where result*]]))

(defmethod parse-rule :ValuesClause
  ;; ValuesClause ::= ( <'VALUES'> WS DataBlock )? WS
  [[_ datablock]]
  (if datablock
    [(parse-term datablock)]
    []))

(defmethod parse-rule :OffsetClause
  ;; OffsetClause ::= <'OFFSET'> WS INTEGER
  [[_ offset]]
  [[:offset (read-string offset)]])

(defmethod parse-rule :LimitClause
  ;; LimitClause ::= <'LIMIT'> WS INTEGER
  [[_ limit]]
  [[:limit (read-string limit)]])

(defmethod parse-term :ExplicitOrderCondition
  ;; ExplicitOrderCondition ::= ( 'ASC' | 'DESC' | 'asc' | 'desc' ) WS BrackettedExpression
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
  [[_ expr as var]]
  (if (= as [:As])
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

(defmethod parse-rule :ConstructWhereQuery
  ;; ConstructWhereQuery ::= DatasetClause <'WHERE'> WS ConstructWhereTemplate SolutionModifier
  [[_ & construct-query]]
  (reduce (fn [entries rule] (into entries (parse-rule rule)))
          []
          construct-query))

(defmethod parse-rule :ConstructQuery
  ;; ConstructQuery   ::= WS <'CONSTRUCT'> WS ( ConstructTemplate DatasetClause WhereClause SolutionModifier | ConstructWhereQuery )
  [[_ & construct-query]]
  (reduce (fn [entries rule] (into entries (parse-rule rule)))
          []
          construct-query))

(defmethod parse-rule :ModifyWhere
  ;; ModifyWhere ::= <'WHERE'> GroupGraphPattern
  [[_ group-pattern]]
  [[:where (vec (parse-term group-pattern))]])

(defmethod parse-term :QuadsNotTriples
  ;; QuadsNotTriples ::= <'GRAPH'> WS VarOrIri <'{'> WS TriplesTemplate? <'}'> WS
  [[_ graph-iri & triples]]
  [(conj [:graph (parse-term graph-iri)] (vec (mapcat parse-term triples)))])

(defmethod parse-term :Quads
  ;; <Quads> ::= TriplesTemplate? ( QuadsNotTriples '.'? TriplesTemplate? )*
  [[_ & quads]]
  (vec (mapcat parse-term quads)))

(defmethod parse-rule :DeleteWhere
  ;; DeleteWhere ::= <'DELETE WHERE'> WS QuadPattern
  [[_ quad-pattern]]
  (let [pattern (parse-term quad-pattern)]
    [[:where pattern]
     [:delete pattern]]))

(defmethod parse-rule :DeleteData
  ;; DeleteClause ::= <'DELETE DATA'> WS QuadData
  ;; <QuadData> ::= <'{'> WS Quads <'}'> WS
  [[_ quad-pattern]]
  (let [{graph-patterns true
         default-patterns false}
        (->> (parse-term quad-pattern)
             (group-by (comp (partial = :graph) first)))]
    (cond (>= (count graph-patterns) 2)
          (throw (ex-info "Multiple GRAPH declarations not supported in DELETE DATA."
                          {:status 400 :error :db/invalid-update}))
          (= (count graph-patterns) 1)
          (let [[[_ graph-iri data]] graph-patterns]
            [[:ledger graph-iri] [:delete data]])
          :else
          [[:delete default-patterns]])))

(defmethod parse-rule :DeleteClause
  ;; DeleteClause ::= <'DELETE'> WS QuadPattern
  ;; <QuadPattern> ::= <'{'> WS Quads <'}'> WS
  [[_ quad-pattern]]
  (let [quad-data (parse-term quad-pattern)]
    (when (not-empty (filter (comp (partial = :graph) first) quad-data))
      (throw (ex-info "GRAPH not supported in DELETE. Use WITH or USING instead."
                      {:status 400 :error :db/invalid-update})))
    [[:delete quad-data]]))

(defmethod parse-rule :InsertData
  ;; InsertClause ::= <'INSERT DATA'> WS QuadData
  ;; <QuadData> ::= <'{'> WS Quads <'}'> WS
  [[_ quad-pattern]]
  (let [{graph-patterns true
         default-patterns false}
        (->> (parse-term quad-pattern)
             (group-by (comp (partial = :graph) first)))]
    (cond (>= (count graph-patterns) 2)
          (throw (ex-info "Multiple GRAPH declarations not supported in INSERT DATA."
                          {:status 400 :error :db/invalid-update}))
          (= (count graph-patterns) 1)
          (let [[[_ graph-iri data]] graph-patterns]
            [[:ledger graph-iri] [:insert data]])
          :else
          [[:insert default-patterns]])))

(defmethod parse-rule :InsertClause
  ;; InsertClause ::= <'INSERT'> WS QuadPattern
  ;; <QuadPattern> ::= <'{'> WS Quads <'}'> WS
  [[_ quad-pattern]]
  (let [quad-data (parse-term quad-pattern)]
    (when (not-empty (filter (comp (partial = :graph) first) quad-data))
      (throw (ex-info "GRAPH not supported in INSERT. Use WITH or USING instead."
                      {:status 400 :error :db/invalid-update})))
    [[:insert quad-data]]))

(defmethod parse-rule :ModifyClause
  ;; ModifyClause ::= ( DeleteClause InsertClause? | InsertClause )
  [[_ & clauses]]
  (mapcat parse-rule clauses))

(defmethod parse-rule :UsingNamed
  ;; UsingNamed ::= <'USING NAMED'> WS iri
  [[_ _iri]]
  (throw (ex-info "USING NAMED is not supported in SPARQL Update."
                  {:status 400 :error :db/invalid-update})))

(defmethod parse-rule :UsingDefault
  ;; UsingDefault ::= <'USING'> WS iri
  [[_ iri]]
  [[:ledger (parse-term iri)]])

(defmethod parse-rule :UsingClause
  ;; UsingClause ::= (UsingDefault | UsingNamed)*
  [[_ & using-clauses]]
  (cond (zero? (count using-clauses))
        []

        (= 1 (count using-clauses))
        (parse-rule (first using-clauses))

        :else
        (throw (ex-info "More than one USING clause is not supported in SPARQL Update."
                        {:status 400 :error :db/invalid-update}))))

(defmethod parse-rule :ModifyWith
  ;; ModifyWith ::= <'WITH'> WS iri ModifyClause UsingClause* ModifyWhere
  [[_ iri & clauses]]
  (into [[:ledger (parse-term iri)]]
        (mapcat parse-rule clauses)))

(defmethod parse-rule :Modify
  ;; Modify ::= ModifyClause UsingClause* ModifyWhere
  [[_ & clauses]]
  (mapcat parse-rule clauses))

(defmethod parse-rule :Update
  ;; Update ::= Prologue ( Update1 ( ';' Update )? )?
  [[_ & update-op]]
  (reduce (fn [entries rule] (into entries (parse-rule rule)))
          []
          update-op))

(defmethod parse-rule :PrettyPrint
  [_]
  [[:prettyPrint true]])

(defmethod parse-rule :Modifiers
  [[_ & modifiers]]
  (mapcat parse-rule modifiers))

(defn translate
  [parsed]
  (reset! bnode-counter 0)
  (reduce (fn [fql rule] (into fql (parse-rule rule)))
          {}
          parsed))
