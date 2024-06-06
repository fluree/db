(ns fluree.db.reasoner.owl-datalog
  (:require [fluree.db.json-ld.iri :as iri]
            [fluree.db.constants :as const]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log]))

;; conversions of owl statements to datalog

(defn only-named-ids
  "Returns all @id values that are not blank nodes
  from either an ordered list or a set of objects."
  [vals]
  (into []
        (comp
         (keep util/get-id)
         (remove iri/blank-node-id?))
        (util/unwrap-list vals)))

(defn get-named-ids
  "Gets all @id values from property 'k' in json-ld node.
  Filters all scalar values and blank nodes."
  [json-ld k]
  (remove
   iri/blank-node-id?
   (util/get-all-ids json-ld k)))

(defn equiv-class-type
  [equiv-class-statement]
  (let [statement-id (util/get-id equiv-class-statement)]
    (cond (util/of-type? equiv-class-statement const/iri-owl:Restriction)
          (cond
            (contains? equiv-class-statement const/iri-owl:hasValue)
            :has-value

            (contains? equiv-class-statement const/iri-owl:someValuesFrom)
            :some-values

            (contains? equiv-class-statement const/iri-owl:allValuesFrom)
            :all-values

            (contains? equiv-class-statement const/iri-owl:maxCardinality)
            :max-cardinality

            (contains? equiv-class-statement const/iri-owl:maxQualifiedCardinality)
            :max-qual-cardinality

            (contains? equiv-class-statement const/iri-owl:qualifiedCardinality)
            :qual-cardinality

            :else
            (do
              (log/warn "Unsupported owl:Restriction" equiv-class-statement)
              nil))

          (contains? equiv-class-statement const/iri-owl:oneOf)
          :one-of

          (contains? equiv-class-statement const/iri-owl:intersectionOf)
          :intersection-of

          (contains? equiv-class-statement const/iri-owl:unionOf)
          :union-of

          statement-id
          (if (iri/blank-node-id? statement-id)
            :blank-nodes
            :classes)

          :else nil)))

(defmulti to-datalog (fn [rule-type _inserts _owl-statement _all-rules]
                       rule-type))

(defmethod to-datalog ::eq-sym
  [_ inserts owl-statement all-rules]
  ;; note any owl:sameAs are just inserts into the current db
  ;; the owl:sameAs rule is a base rule for any existing owl:sameAs
  ;; that might already exist in the current db
  (let [id      (util/get-id owl-statement)
        sa-ids  (get-named-ids owl-statement const/iri-owl:sameAs)
        rule-id (str const/iri-owl:sameAs "(" id ")")
        triples (->> sa-ids
                     (mapcat (fn [sa-id]
                               ;; insert triples both ways
                               [[id const/iri-owl:sameAs {"@id" sa-id}]
                                [sa-id const/iri-owl:sameAs {"@id" id}]]))
                     set)]

    ;; insert sameAs triples into inserts atom
    (swap! inserts update rule-id (fn [et]
                                    (into triples et)))

    all-rules))

;; rdfs:domain
(defmethod to-datalog ::prp-dom
  [_ _ owl-statement all-rules]
  (let [property (util/get-id owl-statement)
        domain   (get-named-ids owl-statement const/iri-rdfs:domain)
        rule     {"where"  {"@id"    "?s"
                            property nil}
                  "insert" {"@id"   "?s"
                            "@type" domain}}]
    (if (and property (seq domain))
      ;; rule-id *is* the property
      (conj all-rules [(str property "(rdfs:domain)") rule])
      (do (log/warn "Ignoring rdfs:domain rule " owl-statement
                    " as property or domain definition not supported by owl2rl reasoning.")
          all-rules))))

;; rdfs:range
(defmethod to-datalog ::prp-rng
  [_ _ owl-statement all-rules]
  (let [range    (get-named-ids owl-statement const/iri-rdfs:range)
        property (util/get-id owl-statement)
        rule     {"where"  {"@id"    nil,
                            property "?ps"}
                  "insert" {"@id"   "?ps"
                            "@type" range}}]
    ;; rule-id *is* the property
    (if (and property (seq range))
      (conj all-rules [(str property "(rdfs:range)") rule])
      (do (log/warn "Ignoring rdfs:range rule " owl-statement
                    " as property or range definition not supported by owl2rl reasoning rule.")
          all-rules))))

;; owl:FunctionalProperty
(defmethod to-datalog ::prp-fp
  [_ _ owl-statement all-rules]
  (let [fp   (util/get-id owl-statement)
        rule {"where"  [{"@id" "?s"
                         fp    "?fp-vals"}
                        {"@id" "?s"
                         fp    "?fp-vals2"}
                        ["filter" "(not= ?fp-vals ?fp-vals2)"]]
              "insert" {"@id"                "?fp-vals"
                        const/iri-owl:sameAs "?fp-vals2"}}]
    (conj all-rules [(str const/iri-owl:FunctionalProperty "(" fp ")") rule])))

;; owl:InverseFunctionalProperty
(defmethod to-datalog ::prp-ifp
  [_ _ owl-statement all-rules]
  (let [ifp  (util/get-id owl-statement)
        rule {"where"  [{"@id" "?x1"
                         ifp   "?y"}
                        {"@id" "?x2"
                         ifp   "?y"}
                        ["filter" "(not= ?x1 ?x2)"]]
              "insert" {"@id"                "?x1"
                        const/iri-owl:sameAs "?x2"}}]
    (conj all-rules [(str const/iri-owl:InverseFunctionalProperty "(" ifp ")") rule])))

;;owl:SymetricProperty
(defmethod to-datalog ::prp-symp
  [_ _ owl-statement all-rules]
  (let [symp (util/get-id owl-statement)
        rule {"where"  [{"@id" "?x"
                         symp  "?y"}]
              "insert" {"@id" "?y"
                        symp  "?x"}}]
    (conj all-rules [(str symp "(owl:SymetricProperty)") rule])))

;; owl:TransitiveProperty
(defmethod to-datalog ::prp-trp
  [_ _ owl-statement all-rules]
  (let [trp  (util/get-id owl-statement)
        rule {"where"  [{"@id" "?x"
                         trp   "?y"}
                        {"@id" "?y"
                         trp   "?z"}]
              "insert" {"@id" "?x"
                        trp   "?z"}}]
    (conj all-rules [(str trp "(owl:TransitiveProperty)") rule])))

;; rdfs:subPropertyOf
(defmethod to-datalog ::prp-spo1
  [_ inserts owl-statement all-rules]
  (let [child-prop   (util/get-id owl-statement)
        parent-props (get-named-ids owl-statement const/iri-rdfs:subPropertyOf)
        triples      (mapv (fn [parent-prop]
                             [child-prop const/iri-rdfs:subPropertyOf {"@id" parent-prop}])
                           parent-props)
        rule-id      (str child-prop "(rdfs:subPropertyOf)")]

    ;; insert subPropertyOf triples directly into db, as there is native support
    ;; in Fluree for subPropertyOf already
    (when parent-props
      (swap! inserts update rule-id (fn [et]
                                      (into triples et))))

    ;; no new rules, just inserted triples
    all-rules))

;; turns list of properties into:
;; [{"@id" "?u0", "?p1" "?u1"}, {"@id" "?u2", "?p2" "?u3"} ... ]
(defmethod to-datalog ::prp-spo2
  [_ _ owl-statement all-rules]
  (try*
    (let [prop    (util/get-id owl-statement)
          p-chain (get-named-ids owl-statement const/iri-owl:propertyChainAxiom)
          where   (->> p-chain
                       (map-indexed (fn [idx p-n]
                                      (if p-n
                                        {"@id" (str "?u" idx)
                                         p-n   (str "?u" (inc idx))}
                                        (throw
                                          (ex-info
                                            (str "propertyChainAxiom for property: "
                                                 prop " - should only contain IRIs however "
                                                 "it appears to contain at least one scalar value "
                                                 "(e.g. a string or number)")
                                            {:owl-statement owl-statement})))))
                       (into []))
          rule    {"where"  where
                   "insert" {"@id" "?u0"
                             prop  (str "?u" (count p-chain))}}]
      (when (empty? p-chain)
        (throw (ex-info (str "propertyChainAxiom for property: " prop
                             " - is not property defined. Value should be of "
                             "type @list and it likely is defined as a set.")
                        {:owl-statement owl-statement})))
      (conj all-rules [(str prop "(owl:propertyChainAxiom)") rule]))
    (catch* e (log/warn (str "Ignoring OWL rule " (ex-message e)))
            all-rules)))

(defmethod to-datalog ::prp-inv
  [_ _ owl-statement all-rules]
  (let [prop     (util/get-first-id owl-statement const/iri-owl:inverseOf)
        inv-prop (util/get-id owl-statement)
        rule1    {"where"  [{"@id" "?x"
                             prop  "?y"}]
                  "insert" {"@id"    "?y"
                            inv-prop "?x"}}
        rule2    {"where"  [{"@id"    "?x"
                             inv-prop "?y"}]
                  "insert" {"@id" "?y"
                            prop  "?x"}}]
    (-> all-rules
        (conj [(str inv-prop "(owl:inverseOf-1)") rule1])
        (conj [(str inv-prop "(owl:inverseOf-2)") rule2]))))


(defn equiv-class-rules
  "Maps every class equivalence to every other class in the set"
  [rule-class class-statements]
  (let [c-set (->> class-statements
                   only-named-ids
                   (into #{rule-class}))]
    (map-indexed (fn [idx cn]
                   (let [rule-id (str rule-class "(owl:equivalentClass-" idx ")")
                         rule    {"where"  {"@id"   "?x"
                                            "@type" cn}
                                  "insert" {"@id"   "?x"
                                            "@type" (into [] (disj c-set cn))}}]
                     [rule-id rule]))
                 c-set)))

(defn equiv-max-qual-cardinality
  "Handles rule cls-maxqc3, cls-maxqc4"
  [rule-class restrictions]
  (reduce
    (fn [acc restriction]
      (let [property  (util/get-first-id restriction const/iri-owl:onProperty)
            max-q-val (util/get-first-value restriction const/iri-owl:maxQualifiedCardinality)
            on-class  (util/get-first-id restriction const/iri-owl:onClass)
            rule      (if (= const/iri-owl:Thing on-class)
                        ;; special case for rule cls-maxqc4 where onClass
                        ;; is owl:Thing, means every object of property is sameAs
                        {"where"  [{"@id"    "?u"
                                    "@type"  rule-class
                                    property "?y1"}
                                   {"@id"    "?u"
                                    property "?y2"}
                                   ["filter" "(not= ?y1 ?y2)"]]
                         "insert" {"@id"                "?y1"
                                   const/iri-owl:sameAs "?y2"}}
                        ;; standard case for rule cls-maxqc3
                        {"where"  [{"@id"    "?u"
                                    "@type"  rule-class
                                    property "?y1"}
                                   {"@id"   "?y1"
                                    "@type" on-class}
                                   {"@id"    "?u"
                                    property "?y2"}
                                   {"@id"   "?y2"
                                    "@type" on-class}
                                   ["filter" "(not= ?y1 ?y2)"]]
                         "insert" {"@id"                "?y1"
                                   const/iri-owl:sameAs "?y2"}})]
        (if (and property on-class (= 1 max-q-val))
          (conj acc [(str rule-class "(owl:maxQualifiedCardinality-" property ")") rule])
          (do (log/info "owl:Restriction for class" rule-class
                        "is being ignored. owl:maxQualifiedCardinality can only infer"
                        "new facts when owl:maxQualifiedCardinality=1. Property" property
                        "has owl:maxQualifiedCardinality equal to: " max-q-val
                        "with class restriction:" on-class)
              acc))))
    []
    restrictions))

(defn equiv-max-cardinality
  "Handles rule cls-maxc2"
  [rule-class restrictions]
  (reduce
    (fn [acc restriction]
      (let [property (util/get-first-id restriction const/iri-owl:onProperty)
            max-val  (util/get-first-value restriction const/iri-owl:maxCardinality)
            rule     {"where"  [{"@id"    "?x"
                                 "@type"  rule-class
                                 property "?y1"}
                                {"@id"    "?x"
                                 "@type"  rule-class
                                 property "?y2"}
                                ["filter" "(not= ?y1 ?y2)"]]
                      "insert" {"@id"                "?y1"
                                const/iri-owl:sameAs "?y2"}}]
        (if (and property (= 1 max-val))
          (conj acc [(str rule-class "(owl:maxCardinality-" property ")") rule])
          (do (log/info "owl:Restriction for class" rule-class
                        "is being ignored. owl:maxCardinality can only infer"
                        "new facts when owl:maxCardinality=1. Property" property
                        "has owl:maxCardinality equal to:" max-val)
              acc))))
    []
    restrictions))

(defn equiv-has-value
  "Handles rules cls-hv1, cls-hv2"
  [rule-class restrictions]
  (reduce
    (fn [acc restriction]
      (let [property (util/get-first-id restriction const/iri-owl:onProperty)
            has-val  (util/get-first restriction const/iri-owl:hasValue)
            has-val* (if (util/get-id has-val)
                       {"@id" has-val}
                       (util/get-value has-val))
            rule1    {"where"  {"@id"    "?x"
                                property has-val*}
                      "insert" {"@id"   "?x"
                                "@type" rule-class}}
            rule2    {"where"  {"@id"   "?x"
                                "@type" rule-class}
                      "insert" {"@id"    "?x"
                                property has-val*}}]
        (if (and property has-val*)
          (-> acc
              (conj [(str rule-class "(owl:Restriction-" property "-1)") rule1])
              (conj [(str rule-class "(owl:Restriction-" property "-2)") rule2]))
          (do (log/warn "owl:Restriction for class" rule-class
                        "is not properly defined. owl:onProperty is:" property
                        "and owl:hasValue is:" has-val
                        ". onProperty must exist and be an IRI (wrapped in {@id: ...})."
                        "hasValue must exist, but can be an IRI or literal value.")
              acc))))
    []
    restrictions))

(defn equiv-all-values
  "Handles rules cls-avf"
  [rule-class restrictions]
  (reduce
    (fn [acc restriction]
      (let [property (util/get-first-id restriction const/iri-owl:onProperty)
            all-val  (util/get-first-id restriction const/iri-owl:allValuesFrom)
            rule     {"where"  {"@id"    "?x"
                                property "?y"}
                      "insert" {"@id"   "?y"
                                "@type" all-val}}]
        (if (and property all-val)
          (conj acc [(str rule-class "(owl:allValuesFrom-" property ")") rule])
          (do (log/warn "owl:Restriction for class" rule-class
                        "is not properly defined. owl:onProperty is:" (get restriction const/iri-owl:onProperty)
                        "and owl:allValuesFrom is:" (util/get-first restriction const/iri-owl:allValuesFrom)
                        ". onProperty must exist and be an IRI (wrapped in {@id: ...})."
                        "allValuesFrom must exist and must be an IRI.")
              acc))))
    []
    restrictions))

(defn some-values-condition
  "Used to build where statement for owl:someValuesFrom restriction when the
  parent clause is *not* an owl:equivalentClass, and therefore is part of a more complex
  nested condition.

  binding-var should be the variables that is being bound in the parent where statement
  clause."
  [binding-var some-values-statements]
  (reduce
    (fn [acc some-values-statement]
      (let [property (util/get-first-id some-values-statement const/iri-owl:onProperty)
            {:keys [classes union-of]} (group-by equiv-class-type (get some-values-statement const/iri-owl:someValuesFrom))]
        (cond
          classes
          (let [target-type (-> classes first util/get-id)]
            (if (and property target-type)
              (-> acc
                  (conj {"@id"    binding-var
                         property "?_some-val-rel"}) ;; choosing a binding var name unlikely to collide
                  (conj {"@id"   "?_some-val-rel"
                         "@type" target-type}))
              (do (log/warn (str "Ignoring owl:someValuesFrom rule: " some-values-statement
                                 " as property or target @type not supported by owl2rl reasoning."))
                  acc)))

          union-of
          (let [union-classes   (-> union-of
                                    first ;; always sequential, but only can be one value so take first
                                    (get-named-ids const/iri-owl:unionOf))
                with-property-q {"@id"    binding-var
                                 property "?_some-val-rel"}
                of-classes-q    (reduce (fn [acc class]
                                          (conj acc {"@id"   "?_some-val-rel"
                                                     "@type" {"@id" class}}))
                                        ["union"]
                                        union-classes)]
            (conj acc with-property-q of-classes-q))

          :else
          (do (log/warn "Ignoring some rules from nested owl:someValuesFrom values."
                        "Currently only support explicit classes and owl:unionOf values."
                        "Please let us know if there is a rule you think should be supported.")
              acc))))
    []
    some-values-statements))

(defn has-value-condition
  "Used to build where statement for owl:hasValue restriction when the
  parent clause is *not* an owl:equivalentClass, and therefore is part of a more complex
  nested condition.

  binding-var should be the variables that is being bound in the parent where statement
  clause."
  [binding-var has-value-statements]
  (reduce
    (fn [acc has-value-statement]
      (let [property   (util/get-first-id has-value-statement const/iri-owl:onProperty)
            has-value  (util/get-first has-value-statement const/iri-owl:hasValue)
            has-value* (if-let [has-val-id (util/get-id has-value)]
                         {"@id" has-val-id}
                         (util/get-value has-value))]
        (conj acc {"@id"    binding-var
                   property has-value*})))
    []
    has-value-statements))

(defn one-of-condition
  "Used to build union clause for owl:oneOf class.

  Assume just a single one-of-statement is passed in, not a list."
  [binding-var property one-of-statement]
  (let [individuals (get-named-ids one-of-statement const/iri-owl:oneOf)]
    (reduce (fn [acc i]
              (conj acc {"@id"    binding-var
                         property {"@id" i}}))
            ["union"]
            individuals)))

(defn equiv-some-values
  "Handles rules cls-svf1, cls-svf2

  Value of owl:someValuesFrom must be a single class, but that class can be
  a owl:oneOf, etc which allows for multiple criteria to match."
  [rule-class restrictions]
  (reduce
    (fn [acc restriction]
      (let [property (util/get-first-id restriction const/iri-owl:onProperty)
            {:keys [classes one-of]} (group-by equiv-class-type (get restriction const/iri-owl:someValuesFrom))
            rule     (cond
                       ;; special case where someValuesFrom is owl:Thing, means
                       ;; everything with property should be in the class (cls-svf2)
                       (and classes (= const/iri-owl:Thing (-> classes first util/get-id)))
                       {"where"  {"@id"    "?x"
                                  property nil}
                        "insert" {"@id"   "?x"
                                  "@type" rule-class}}

                       ;; an explicit class is defined for someValuesFrom (cls-svf1)
                       classes
                       {"where"  [{"@id"    "?x"
                                   property "?y"}
                                  {"@id"   "?y"
                                   "@type" (-> classes first util/get-id)}]
                        "insert" {"@id"   "?x"
                                  "@type" rule-class}}

                       ;; one-of is defined for someValuesFrom (cls-svf1)
                       one-of
                       {"where"  [(one-of-condition "?x" property (first one-of))]
                        "insert" {"@id"   "?x"
                                  "@type" rule-class}})]
        (if rule
          (conj acc [(str rule-class "(owl:someValuesFrom-" property ")") rule])
          (do (log/warn "owl:Restriction for class" rule-class
                        "is not properly defined. owl:onProperty is:" (get restriction const/iri-owl:onProperty)
                        "and owl:someValuesFrom is:" (util/get-first restriction const/iri-owl:someValuesFrom)
                        ". onProperty must exist and be an IRI (wrapped in {@id: ...})."
                        "someValuesFrom must exist and must be a Class.")
              acc))))
    []
    restrictions))

(defn equiv-intersection-of
  "Handles owl:intersectionOf - rules cls-int1, cls-int2"
  [rule-class intersection-of-statements inserts]
  (reduce
    (fn [acc intersection-of-statement]
      (let [intersections (util/unwrap-list (get intersection-of-statement const/iri-owl:intersectionOf))
            {:keys [classes has-value some-values qual-cardinality]} (group-by equiv-class-type intersections)
            restrictions  (cond->> []
                                   has-value (into (has-value-condition "?y" has-value))
                                   some-values (into (some-values-condition "?y" some-values)))
            class-list    (only-named-ids classes)
            cls-int1      (when (or (seq class-list)
                                    (seq restrictions))
                            {"where"  (reduce
                                        (fn [acc c]
                                          (conj acc
                                                {"@id"   "?y"
                                                 "@type" c}))
                                        restrictions
                                        class-list)
                             "insert" {"@id"   "?y"
                                       "@type" rule-class}})
            cls-int2      (when (seq class-list)
                            {"where"  {"@id"   "?y"
                                       "@type" rule-class}
                             "insert" {"@id"   "?y"
                                       "@type" (into [] class-list)}})

            triples       (reduce
                            (fn [triples* c]
                              (conj triples* [rule-class const/iri-rdfs:subClassOf {"@id" c}]))
                            []
                            class-list)]

        (when qual-cardinality
          (log/warn (str "Ignoring owl:qualifiedCardinality rule(s), "
                         " not supported by owl2rl profile: " qual-cardinality)))

        ;; intersectionOf inserts subClassOf rules (scm-int)
        (when (seq triples)
          (swap! inserts assoc (str rule-class "(owl:intersectionOf-subclass)") triples))

        (cond-> acc
                cls-int1 (conj [(str rule-class "(owl:intersectionOf-1)#" (hash class-list)) cls-int1])
                cls-int2 (conj [(str rule-class "(owl:intersectionOf-2)#" (hash class-list)) cls-int2]))))
    []
    intersection-of-statements))

(defn equiv-union-of
  "Handles rules cls-uni, scm-uni"
  [rule-class union-of-statements inserts]
  (reduce
    (fn [acc union-of-statement]
      (let [unions            (util/unwrap-list (get union-of-statement const/iri-owl:unionOf))
            {:keys [classes has-value]} (group-by equiv-class-type unions)
            restrictions      (cond->> []
                                       has-value (into (has-value-condition "?y" has-value)))
            restriction-rules (map (fn [where]
                                     [(str rule-class "(owl:unionOf->owl:hasValue)#" (hash where))
                                      {"where"  where
                                       "insert" {"@id"   "?y"
                                                 "@type" rule-class}}])
                                   restrictions)
            class-list        (map util/get-id classes)
            ;; could do optional clauses instead of separate
            ;; opted for separate for now to keep it simple
            ;; and allow for possibly fewer rule triggers with
            ;; updating data - but not sure what is best
            rules             (map-indexed
                                (fn [idx c]
                                  (let [rule {"where"  {"@id"   "?y"
                                                        "@type" c}
                                              "insert" {"@id"   "?y"
                                                        "@type" rule-class}}]
                                    [(str rule-class "(owl:unionOf-" idx ")") rule]))
                                class-list)

            triples           (reduce
                                (fn [triples* c]
                                  (conj triples* [c const/iri-rdfs:subClassOf {"@id" rule-class}]))
                                []
                                class-list)]

        ;; unionOf inserts subClassOf rules (scm-uni)
        (swap! inserts assoc (str rule-class "(owl:unionOf-subclass)") triples)

        (concat acc rules restriction-rules)))
    []
    union-of-statements))

(defn equiv-one-of
  "Handles rule cls-oo"
  [rule-class one-of-statements inserts]
  (let [triples (reduce (fn [acc one-of-statement]
                          (let [individuals (get-named-ids one-of-statement const/iri-owl:oneOf)
                                triples     (map (fn [i]
                                                   [i "@type" rule-class])
                                                 individuals)]
                            (into acc triples)))
                        #{}
                        one-of-statements)]
    (swap! inserts assoc (str rule-class "(owl:oneOf)") triples)
    ;; return empty rules, as the triples are inserted directly
    []))

;; owl:equivalentClass
(defmethod to-datalog ::cax-eqc
  [_ inserts owl-statement all-rules]
  (let [c1 (util/get-id owl-statement) ;; the class which is the subject
        ;; combine with all other equivalent classes for a set of 2+ total classes
        {:keys [classes intersection-of union-of one-of
                has-value some-values all-values
                max-cardinality max-qual-cardinality]} (->> (get owl-statement const/iri-owl:equivalentClass)
                                                            util/unwrap-list
                                                            (group-by equiv-class-type))]
    (cond-> all-rules
            classes (into (equiv-class-rules c1 classes)) ;; cax-eqc1, cax-eqc2
            intersection-of (into (equiv-intersection-of c1 intersection-of inserts)) ;; cls-int1, cls-int2, scm-int
            union-of (into (equiv-union-of c1 union-of inserts)) ;; cls-uni, scm-uni
            one-of (into (equiv-one-of c1 one-of inserts)) ;; cls-oo
            has-value (into (equiv-has-value c1 has-value)) ;; cls-hv1, cls-hv1
            some-values (into (equiv-some-values c1 some-values)) ;; cls-svf1, cls-svf2
            all-values (into (equiv-all-values c1 all-values)) ;; cls-svf1, cls-svf2
            max-cardinality (into (equiv-max-cardinality c1 max-cardinality)) ;; cls-maxc2
            max-qual-cardinality (into (equiv-max-qual-cardinality c1 max-qual-cardinality))))) ;; cls-maxqc3, cls-maxqc4

;; rdfs:subClassOf
(defmethod to-datalog ::cax-sco
  [_ inserts owl-statement all-rules]
  (let [c1         (util/get-id owl-statement) ;; the class which is the subject
        class-list (get-named-ids owl-statement const/iri-rdfs:subClassOf)
        triples    (reduce
                     (fn [triples* c]
                       (conj triples* [c1 const/iri-rdfs:subClassOf {"@id" c}]))
                     []
                     class-list)]

    ;; fluree handles subClassOf at query-time, so just need
    ;; to insert the triples into the current db
    (swap! inserts assoc (str c1 "(rdfs:subClassOf)") triples)

    all-rules))

(defmethod to-datalog ::prp-key
  [_ _ owl-statement all-rules]
  (let [class     (util/get-id owl-statement)
        ;; support props in either @list form, or just as a set of values
        prop-list (get-named-ids owl-statement const/iri-owl:hasKey)
        where     (->> prop-list
                       (map-indexed (fn [idx prop]
                                      [prop (str "?z" idx)]))
                       (into {"@type" class}))
        rule      {"where"  [(assoc where "@id" "?x")
                             (assoc where "@id" "?y")
                             ["filter" "(not= ?x ?y)"]]
                   "insert" [{"@id"                "?x"
                              const/iri-owl:sameAs "?y"}
                             {"@id"                "?y"
                              const/iri-owl:sameAs "?x"}]}]
    (conj all-rules [(str class "(owl:hasKey)#" (hash prop-list)) rule])))

(defmethod to-datalog :default
  [_ _ owl-statement all-rules]
  (throw (ex-info "Unsupported OWL statement" {:owl-statement owl-statement})))

(def base-rules
  [
   ;; eq-sym, eq-trans, eq-rep-s covered by below rule
   [(str const/iri-owl:sameAs "(eq)")
    {"where"  [{"@id"                "?s"
                const/iri-owl:sameAs "?s'"}
               {"@id" "?s"
                "?p"  "?o"}
               {"@id" "?s'"
                "?p'" "?o'"}]
     "insert" [{"@id" "?s'"
                "?p"  "?o"}
               {"@id"                "?s'"
                const/iri-owl:sameAs {"@id" "?s"}}
               {"@id" "?s"
                "?p'" "?o'"}]}]
   ;; eq-rep-o covered by below rule
   [(str const/iri-owl:sameAs "(eq-rep-o)")
    {"where"  [{"@id"                "?o"
                const/iri-owl:sameAs "?o'"}
               {"@id" "?s"
                "?p"  "?o"}]
     "insert" [{"@id" "?s"
                "?p"  "?o'"}]}]])

(defn property-types
  [owl-statement]
  (let [types (util/get-types owl-statement)]
    (reduce (fn [acc type]
              (condp = type

                const/iri-owl:FunctionalProperty
                (assoc acc :functional-property? true)

                const/iri-owl:InverseFunctionalProperty
                (assoc acc :inverse-functional-property? true)

                const/iri-owl:SymetricProperty
                (assoc acc :symetric-property? true)

                const/iri-owl:TransitiveProperty
                (assoc acc :transitive-property? true)

                acc))
            {}
            types)))


(defn statement->datalog
  [inserts owl-statement]
  (try*
    (let [{:keys [functional-property? inverse-functional-property?
                  symetric-property? transitive-property?]} (property-types owl-statement)]
      (cond
        (contains? owl-statement const/iri-owl:sameAs) ;; TODO - can probably take this out of multi-fn
        (to-datalog ::eq-sym inserts owl-statement [])

        :else
        (cond->> []
                 (contains? owl-statement const/iri-rdfs:domain)
                 (to-datalog ::prp-dom inserts owl-statement)

                 (contains? owl-statement const/iri-rdfs:range)
                 (to-datalog ::prp-rng inserts owl-statement)

                 (contains? owl-statement const/iri-rdfs:subPropertyOf)
                 (to-datalog ::prp-spo1 inserts owl-statement)

                 (contains? owl-statement const/iri-owl:propertyChainAxiom)
                 (to-datalog ::prp-spo2 inserts owl-statement)

                 (contains? owl-statement const/iri-owl:inverseOf)
                 (to-datalog ::prp-inv inserts owl-statement)

                 (contains? owl-statement const/iri-owl:hasKey)
                 (to-datalog ::prp-key inserts owl-statement)

                 (contains? owl-statement const/iri-rdfs:subClassOf)
                 (to-datalog ::cax-sco inserts owl-statement)

                 (contains? owl-statement const/iri-owl:equivalentClass)
                 (to-datalog ::cax-eqc inserts owl-statement)

                 functional-property?
                 (to-datalog ::prp-fp inserts owl-statement)

                 inverse-functional-property?
                 (to-datalog ::prp-ifp inserts owl-statement)

                 symetric-property?
                 (to-datalog ::prp-symp inserts owl-statement)

                 transitive-property?
                 (to-datalog ::prp-trp inserts owl-statement))))
    (catch* e
            (log/error e (str "Error processing OWL statement: " owl-statement " - skipping!"))
            [])))

(defn owl->datalog
  [inserts owl-graph]
  (->> owl-graph
       (mapcat (partial statement->datalog inserts))
       (into base-rules)))

