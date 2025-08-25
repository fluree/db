(ns fluree.db.reasoner.owl-datalog
  (:require [fluree.db.constants :as const]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.util :as util :refer [try* catch*]]
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
    (cond
          ;; Check for Restriction types first, before checking if it's a blank node
      (util/of-type? equiv-class-statement const/iri-owl:Restriction)
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

          ;; Only check for blank nodes/classes if none of the above matched
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
(defn extract-chain-property
  "Extract property from chain element, handling inverse properties.
  Returns {:property prop-id :is-inverse? bool}"
  [chain-element]
  (cond
    ;; Direct property ID string
    (string? chain-element)
    {:property chain-element
     :is-inverse? false}

    ;; Map with inverseOf - could be single or double inverse
    (and (map? chain-element) (contains? chain-element const/iri-owl:inverseOf))
    (let [inverse-val (util/get-first chain-element const/iri-owl:inverseOf)]
      (cond
        ;; Check if inverse-val has an @id - use that with inverse flag
        ;; This handles cases where a property is defined elsewhere and referenced here
        (and inverse-val (util/get-id inverse-val))
        {:property (util/get-id inverse-val)
         :is-inverse? true}

        ;; Double inverse case - but still check if intermediate has @id
        (and inverse-val (contains? inverse-val const/iri-owl:inverseOf))
        ;; Even though it's a double inverse, if the intermediate has an @id, use it
        (if-let [intermediate-id (util/get-id inverse-val)]
          {:property intermediate-id
           :is-inverse? true}
          ;; Otherwise normalize to original
          (let [original-prop (util/get-first-id inverse-val const/iri-owl:inverseOf)]
            (if original-prop
              (do (log/debug "Found double inverse in property chain, normalizing to original:" original-prop)
                  {:property original-prop
                   :is-inverse? false}) ;; Double inverse normalizes to non-inverse
              (throw (ex-info "Invalid double inverse property - no target property specified"
                              {:chain-element chain-element})))))

        ;; Single inverse case - just extract the property being inverted
        :else
        (if-let [inverse-prop (util/get-first-id chain-element const/iri-owl:inverseOf)]
          {:property inverse-prop
           :is-inverse? true}
          (throw (ex-info "Invalid inverse property - no target property specified"
                          {:chain-element chain-element})))))

    ;; Map with @id - normal property reference
    (and (map? chain-element) (util/get-id chain-element))
    {:property (util/get-id chain-element)
     :is-inverse? false}

    :else
    (throw (ex-info "Invalid property chain element"
                    {:chain-element chain-element}))))

(defmethod to-datalog ::prp-spo2
  [_ _ owl-statement all-rules]
  (try*
    (let [prop    (util/get-id owl-statement)
          p-chain-raw (util/unwrap-list (get owl-statement const/iri-owl:propertyChainAxiom))
          p-chain (mapv extract-chain-property p-chain-raw)
          where   (->> p-chain
                       (map-indexed (fn [idx {:keys [property is-inverse?]}]
                                      (if property
                                        (if is-inverse?
                                          ;; For inverse: ?u(n+1) property ?u(n)
                                          {"@id" (str "?u" (inc idx))
                                           property (str "?u" idx)}
                                          ;; For normal: ?u(n) property ?u(n+1)
                                          {"@id" (str "?u" idx)
                                           property (str "?u" (inc idx))})
                                        (throw
                                         (ex-info
                                          (str "propertyChainAxiom for property: "
                                               prop " - contains invalid element")
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

(defn extract-property-with-inverse
  "Extracts a property from a restriction, handling inverse properties.
  Returns a map with :property and :is-inverse? keys.
  Handles double inverse normalization (inverse of inverse = original)."
  [restriction]
  (let [on-property (util/get-first restriction const/iri-owl:onProperty)]
    (cond
      ;; No on-property found
      (nil? on-property)
      {:property nil
       :is-inverse? false}

      ;; Check for double inverse (inverse of inverse)
      (and (contains? on-property const/iri-owl:inverseOf)
           (let [inverse-val (util/get-first on-property const/iri-owl:inverseOf)]
             (and inverse-val (contains? inverse-val const/iri-owl:inverseOf))))
      (let [double-inverse-prop (util/get-first on-property const/iri-owl:inverseOf)
            original-prop (util/get-first-id double-inverse-prop const/iri-owl:inverseOf)]
        (if original-prop
          (do (log/debug "Found double inverse property, normalizing to original:" original-prop)
              {:property original-prop
               :is-inverse? false}) ;; Double inverse normalizes to non-inverse
          {:property nil
           :is-inverse? false}))

      ;; Single inverse property
      (contains? on-property const/iri-owl:inverseOf)
      (let [inverse-prop (util/get-first-id on-property const/iri-owl:inverseOf)]
        (if inverse-prop
          (do (log/debug "Found inverse property" inverse-prop "in restriction for" (util/get-id restriction))
              {:property inverse-prop
               :is-inverse? true})
          {:property nil
           :is-inverse? false}))

      ;; Property chain as property (when the onProperty directly contains a chain definition)
      ;; This is for cases like: "owl:onProperty" {"owl:propertyChainAxiom" [...]}
      (contains? on-property const/iri-owl:propertyChainAxiom)
      (let [chain-val (get on-property const/iri-owl:propertyChainAxiom)
            ;; Ensure chain is always a sequence
            chain-seq (util/sequential chain-val)]
        {:property-chain chain-seq
         :is-chain? true
         :property nil
         :is-inverse? false})

      ;; Direct property reference (including properties that have chain axioms defined elsewhere)
      :else
      {:property (util/get-id on-property)
       :is-inverse? false})))

(defn equiv-has-value
  "Handles rules cls-hv1, cls-hv2"
  [rule-class restrictions]
  (reduce
   (fn [acc restriction]
     (let [{:keys [property is-inverse?]} (extract-property-with-inverse restriction)
           has-val  (util/get-first restriction const/iri-owl:hasValue)
           has-val* (cond
                      (util/get-id has-val)
                      {"@id" (util/get-id has-val)}

                      ;; For typed data values, preserve the full object for matching
                      (and (map? has-val) (contains? has-val "@value"))
                      has-val

                      ;; For simple values, use raw value
                      :else
                      (util/get-value has-val))
           rule1    (if is-inverse?
                     ;; For inverse: if has-val has property x, then x is rule-class
                      {"where"  (if (map? has-val*)
                                  {"@id"    (get has-val* "@id")
                                   property "?x"}
                                ;; For scalar values, can't have inverse
                                  (throw (ex-info "Cannot have inverse property with scalar hasValue"
                                                  {:restriction restriction})))
                       "insert" {"@id"   "?x"
                                 "@type" rule-class}}
                     ;; Normal: if x has property has-val, then x is rule-class
                      {"where"  {"@id"    "?x"
                                 property has-val*}
                       "insert" {"@id"   "?x"
                                 "@type" rule-class}})
           rule2    (if is-inverse?
                     ;; For inverse: if x is rule-class, then has-val has property x
                      {"where"  {"@id"   "?x"
                                 "@type" rule-class}
                       "insert" (if (map? has-val*)
                                  {"@id"    (get has-val* "@id")
                                   property "?x"}
                                ;; Can't insert inverse for scalar
                                  nil)}
                     ;; Normal: if x is rule-class, then x has property has-val
                      {"where"  {"@id"   "?x"
                                 "@type" rule-class}
                       "insert" {"@id"    "?x"
                                 property has-val*}})]
       (if (and property has-val*)
         (cond-> acc
           true (conj [(str rule-class "(owl:Restriction-" property "-1)") rule1])
           ;; Only add rule2 if it has a valid insert clause
           (get rule2 "insert") (conj [(str rule-class "(owl:Restriction-" property "-2)") rule2]))
         (do (log/warn "owl:Restriction for class" rule-class
                       "is not properly defined. owl:onProperty is:" property
                       "and owl:hasValue is:" has-val
                       ". onProperty must exist and be an IRI (wrapped in {@id: ...})."
                       "hasValue must exist, but can be an IRI or literal value.")
             acc))))
   []
   restrictions))

(defn equiv-all-values
  "Handles rules cls-avf - generates both forward entailment and backward inference rules for allValuesFrom"
  [rule-class restrictions]
  (reduce
   (fn [acc restriction]
     (let [{:keys [property is-inverse?]} (extract-property-with-inverse restriction)
           all-val  (util/get-first-id restriction const/iri-owl:allValuesFrom)
           ;; Forward entailment: if x is of rule-class and has the property, then target must be of all-val type
           forward-rule (if is-inverse?
                         ;; For inverse: if x is rule-class and y has property x, then y must be of type all-val
                          {"where"  [{"@id"   "?x"
                                      "@type" rule-class}
                                     {"@id"    "?y"
                                      property "?x"}]
                           "insert" {"@id"   "?y"
                                     "@type" all-val}}
                         ;; Normal: if x is rule-class and x has property y, then y must be of type all-val
                          {"where"  [{"@id"   "?x"
                                      "@type" rule-class}
                                     {"@id"    "?x"
                                      property "?y"}]
                           "insert" {"@id"   "?y"
                                     "@type" all-val}})
           ;; Backward inference: anything that appears as a value of the property is of that type
           ;; This is needed for OWL 2 RL compliance
           backward-rule (if is-inverse?
                          ;; For inverse: if y has property x, then y is of type all-val
                           {"where"  {"@id"    "?y"
                                      property "?x"}
                            "insert" {"@id"   "?y"
                                      "@type" all-val}}
                          ;; Normal: if x has property y, then y is of type all-val
                           {"where"  {"@id"    "?x"
                                      property "?y"}
                            "insert" {"@id"   "?y"
                                      "@type" all-val}})]
       (if (and property all-val)
         (-> acc
             (conj [(str rule-class "(owl:allValuesFrom-forward-" property ")") forward-rule])
             (conj [(str all-val "(owl:allValuesFrom-backward-" property ")") backward-rule]))
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
  (let [result
        (reduce
         (fn [{:keys [acc var-counter]} some-values-statement]
           (let [{:keys [property is-inverse?]} (extract-property-with-inverse some-values-statement)
                 ;; Get the someValuesFrom value - could be a single value or a collection
                 some-values-val (get some-values-statement const/iri-owl:someValuesFrom)
                 ;; Ensure it's a collection for consistent processing
                 some-values-seq (if (sequential? some-values-val) some-values-val [some-values-val])
                 {:keys [classes union-of]} (group-by equiv-class-type some-values-seq)
                 ;; Use a unique variable for each restriction
                 var-name (str "?_sv" var-counter)]
             (cond
               classes
               (let [target-type (-> classes first util/get-id)]
                 (if (and property target-type)
                   {:acc (if is-inverse?
                           ;; For inverse: something of target-type has property pointing to binding-var
                           (-> acc
                               (conj {"@id"   var-name
                                      "@type" target-type})
                               (conj {"@id"    var-name
                                      property binding-var}))
                           ;; Normal: binding-var has property pointing to something of target-type
                           (-> acc
                               (conj {"@id"    binding-var
                                      property var-name})
                               (conj {"@id"   var-name
                                      "@type" target-type})))
                    :var-counter (inc var-counter)}
                   (do (log/warn (str "Ignoring owl:someValuesFrom rule: " some-values-statement
                                      " as property or target @type not supported by owl2rl reasoning."))
                       {:acc acc :var-counter var-counter})))

               union-of
               (let [union-classes   (-> union-of
                                         first ;; always sequential, but only can be one value so take first
                                         (get-named-ids const/iri-owl:unionOf))
                     with-property-q (if is-inverse?
                                      ;; For inverse with union
                                       {"@id"    var-name
                                        property binding-var}
                                      ;; Normal with union
                                       {"@id"    binding-var
                                        property var-name})
                     of-classes-q    (reduce (fn [acc class]
                                               (conj acc {"@id"   var-name
                                                          "@type" {"@id" class}}))
                                             ["union"]
                                             union-classes)]
                 {:acc (conj acc with-property-q of-classes-q)
                  :var-counter (inc var-counter)})

               :else
               (do (log/warn "Ignoring some rules from nested owl:someValuesFrom values."
                             "Currently only support explicit classes and owl:unionOf values."
                             "Please let us know if there is a rule you think should be supported.")
                   {:acc acc :var-counter var-counter}))))
         {:acc [] :var-counter 0}
         some-values-statements)]
    ;; Return just the accumulated conditions
    (:acc result)))

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
           has-value* (cond
                        (util/get-id has-value)
                        {"@id" (util/get-id has-value)}

                        ;; For typed data values, preserve the full object for matching
                        (and (map? has-value) (contains? has-value "@value"))
                        has-value

                        ;; For simple values, use raw value
                        :else
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
     (let [{:keys [property is-inverse? is-chain? property-chain]} (extract-property-with-inverse restriction)
           some-values-val (get restriction const/iri-owl:someValuesFrom)
           ;; someValuesFrom could be a single class or a collection - ensure it's a collection
           some-values-seq (if (sequential? some-values-val) some-values-val [some-values-val])
           {:keys [classes one-of union-of]} (group-by equiv-class-type some-values-seq)
           rule     (cond
                      ;; Handle property chain in restriction
                      is-chain?
                      (let [chain-elements (mapv extract-chain-property property-chain)
                            chain-vars (mapv #(str "?chain" %) (range (inc (count chain-elements))))
                            where-clauses (mapv (fn [idx {:keys [property is-inverse?]}]
                                                  (if is-inverse?
                                                    {"@id" (get chain-vars (inc idx))
                                                     property (get chain-vars idx)}
                                                    {"@id" (get chain-vars idx)
                                                     property (get chain-vars (inc idx))}))
                                                (range (count chain-elements))
                                                chain-elements)]
                        (when (and classes (seq chain-elements))
                          {"where" (conj where-clauses
                                         {"@id" (last chain-vars)
                                          "@type" (-> classes first util/get-id)})
                           "insert" {"@id" "?chain0"
                                     "@type" rule-class}}))

                      ;; Normal property handling
                      property
                      (cond
                        ;; special case where someValuesFrom is owl:Thing, means
                        ;; everything with property should be in the class (cls-svf2)
                        (and classes (= const/iri-owl:Thing (-> classes first util/get-id)))
                        (if is-inverse?
                          ;; For inverse: anything that is pointed to by something should be in the class
                          {"where"  {"@id"    "?y"
                                     property "?x"}
                           "insert" {"@id"   "?x"
                                     "@type" rule-class}}
                          ;; Normal: anything with the property should be in the class
                          {"where"  {"@id"    "?x"
                                     property nil}
                           "insert" {"@id"   "?x"
                                     "@type" rule-class}})

                        ;; an explicit class is defined for someValuesFrom (cls-svf1)
                        classes
                        (if is-inverse?
                          ;; For inverse: if y has property x, and y is of type Class, then x is rule-class
                          {"where"  [{"@id"   "?y"
                                      "@type" (-> classes first util/get-id)}
                                     {"@id"    "?y"
                                      property "?x"}]
                           "insert" {"@id"   "?x"
                                     "@type" rule-class}}
                          ;; Normal: if x has property y, and y is of type Class, then x is rule-class
                          {"where"  [{"@id"    "?x"
                                      property "?y"}
                                     {"@id"   "?y"
                                      "@type" (-> classes first util/get-id)}]
                           "insert" {"@id"   "?x"
                                     "@type" rule-class}})

                        ;; one-of is defined for someValuesFrom (cls-svf1)
                        one-of
                        (if is-inverse?
                          ;; For inverse with one-of: build union of conditions where y is one of the individuals and y has property x
                          (let [individuals (get-named-ids (first one-of) const/iri-owl:oneOf)
                                union-conditions (reduce (fn [acc i]
                                                           (conj acc {"@id"    i
                                                                      property "?x"}))
                                                         ["union"]
                                                         individuals)]
                            {"where"  [union-conditions]
                             "insert" {"@id"   "?x"
                                       "@type" rule-class}})
                          {"where"  [(one-of-condition "?x" property (first one-of))]
                           "insert" {"@id"   "?x"
                                     "@type" rule-class}})

                        ;; union-of is defined for someValuesFrom - generate multiple rules
                        union-of
                        :union-of ;; Return special marker to handle below
                        )

                      ;; No valid property - can't generate rule
                      :else nil)]
       (cond
         ;; Handle union-of case - generate multiple rules (one for each union member)
         (= rule :union-of)
         (let [{:keys [property is-inverse?]} (extract-property-with-inverse restriction)
               union-classes (-> union-of first (get const/iri-owl:unionOf) util/unwrap-list)]
           (reduce (fn [acc* union-class]
                     (cond
                       ;; Restriction in union - check this first before simple class ID
                       (and (util/of-type? union-class const/iri-owl:Restriction)
                            (contains? union-class const/iri-owl:onProperty)
                            (contains? union-class const/iri-owl:someValuesFrom))
                       (let [restr-prop (util/get-first-id union-class const/iri-owl:onProperty)
                             restr-class (util/get-first-id union-class const/iri-owl:someValuesFrom)]
                         (if (and restr-prop restr-class)
                           (let [restr-rule (if is-inverse?
                                             ;; For inverse with restriction: complex pattern
                                              {"where"  [{"@id"    "?y"
                                                          restr-prop "?z"}
                                                         {"@id"   "?z"
                                                          "@type" restr-class}
                                                         {"@id"    "?y"
                                                          property "?x"}]
                                               "insert" {"@id"   "?x"
                                                         "@type" rule-class}}
                                             ;; Normal with restriction
                                              {"where"  [{"@id"    "?x"
                                                          property "?y"}
                                                         {"@id"    "?y"
                                                          restr-prop "?z"}
                                                         {"@id"   "?z"
                                                          "@type" restr-class}]
                                               "insert" {"@id"   "?x"
                                                         "@type" rule-class}})]
                             (conj acc* [(str rule-class "(owl:someValuesFrom-" property "-union-restriction-" restr-prop ")") restr-rule]))
                           acc*))

                       ;; Nested union
                       (contains? union-class const/iri-owl:unionOf)
                       (let [nested-union-classes (util/unwrap-list (get union-class const/iri-owl:unionOf))]
                         (reduce (fn [acc** nested-class]
                                   (if-let [nested-class-id (util/get-id nested-class)]
                                     (let [nested-rule (if is-inverse?
                                                         {"where"  [{"@id"   "?y"
                                                                     "@type" nested-class-id}
                                                                    {"@id"    "?y"
                                                                     property "?x"}]
                                                          "insert" {"@id"   "?x"
                                                                    "@type" rule-class}}
                                                         {"where"  [{"@id"    "?x"
                                                                     property "?y"}
                                                                    {"@id"   "?y"
                                                                     "@type" nested-class-id}]
                                                          "insert" {"@id"   "?x"
                                                                    "@type" rule-class}})]
                                       (conj acc** [(str rule-class "(owl:someValuesFrom-" property "-nested-union-" nested-class-id ")") nested-rule]))
                                     acc**))
                                 acc*
                                 nested-union-classes))

                       ;; Simple class ID (check after restriction and nested union)
                       (util/get-id union-class)
                       (let [union-class-id (util/get-id union-class)
                             union-rule (if is-inverse?
                                         ;; For inverse: if y has property x, and y is of union type, then x is rule-class
                                          {"where"  [{"@id"   "?y"
                                                      "@type" union-class-id}
                                                     {"@id"    "?y"
                                                      property "?x"}]
                                           "insert" {"@id"   "?x"
                                                     "@type" rule-class}}
                                         ;; Normal: if x has property y, and y is of union type, then x is rule-class
                                          {"where"  [{"@id"    "?x"
                                                      property "?y"}
                                                     {"@id"   "?y"
                                                      "@type" union-class-id}]
                                           "insert" {"@id"   "?x"
                                                     "@type" rule-class}})]
                         (conj acc* [(str rule-class "(owl:someValuesFrom-" property "-union-" union-class-id ")") union-rule]))

                       :else
                       (do (log/warn "Ignoring unsupported union member in someValuesFrom:" union-class)
                           acc*)))
                   acc
                   union-classes))

         ;; Normal rule case
         rule
         (conj acc [(str rule-class "(owl:someValuesFrom-" property ")") rule])

         ;; No rule generated - log warning
         :else
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
           {:keys [classes has-value some-values all-values qual-cardinality union-of]} (group-by equiv-class-type intersections)
           ;; Build union conditions for intersection from union-of group
           union-conditions (reduce (fn [acc* union-class]
                                      (let [union-members (util/unwrap-list (get union-class const/iri-owl:unionOf))
                                            union-ids (keep util/get-id union-members)]
                                        (if (seq union-ids)
                                          (conj acc* (reduce (fn [union-acc id]
                                                               (conj union-acc {"@id" "?y"
                                                                                "@type" id}))
                                                             ["union"]
                                                             union-ids))
                                          acc*)))
                                    []
                                    union-of)
           restrictions  (cond->> union-conditions
                           has-value (into (has-value-condition "?y" has-value))
                           some-values (into (some-values-condition "?y" some-values)))
           ;; Note: all-values restrictions don't add conditions, they create separate forward rules
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

           ;; Generate forward entailment rules for allValuesFrom in intersections
           all-values-rules (reduce (fn [acc* all-val-restriction]
                                      (let [{:keys [property is-inverse?]} (extract-property-with-inverse all-val-restriction)
                                            target-class (util/get-first-id all-val-restriction const/iri-owl:allValuesFrom)]
                                        (if (and property target-class)
                                          (let [rule (if is-inverse?
                                                      ;; For inverse: if x is rule-class and y has property x, then y is target-class
                                                       {"where"  [{"@id"   "?x"
                                                                   "@type" rule-class}
                                                                  {"@id"    "?y"
                                                                   property "?x"}]
                                                        "insert" {"@id"   "?y"
                                                                  "@type" target-class}}
                                                      ;; Normal: if x is rule-class and x has property y, then y is target-class
                                                       {"where"  [{"@id"   "?x"
                                                                   "@type" rule-class}
                                                                  {"@id"    "?x"
                                                                   property "?y"}]
                                                        "insert" {"@id"   "?y"
                                                                  "@type" target-class}})]
                                            (conj acc* [(str rule-class "(owl:allValuesFrom-forward-" property ")") rule]))
                                          acc*)))
                                    []
                                    all-values)

           ;; Generate forward entailment rules for hasValue in intersections
           has-value-rules (reduce (fn [acc* has-val-restriction]
                                     (let [{:keys [property is-inverse?]} (extract-property-with-inverse has-val-restriction)
                                           has-val  (util/get-first has-val-restriction const/iri-owl:hasValue)
                                           has-val* (cond
                                                      (util/get-id has-val)
                                                      {"@id" (util/get-id has-val)}

                                                    ;; For typed data values, preserve the full object for matching
                                                      (and (map? has-val) (contains? has-val "@value"))
                                                      has-val

                                                    ;; For simple values, use raw value
                                                      :else
                                                      (util/get-value has-val))]
                                       (if (and property has-val*)
                                         (let [rule (if is-inverse?
                                                    ;; For inverse: if x is rule-class, then has-val has property x
                                                      (when (map? has-val*)  ;; Can't do inverse for scalar values
                                                        {"where"  {"@id"   "?x"
                                                                   "@type" rule-class}
                                                         "insert" {"@id"    (get has-val* "@id")
                                                                   property "?x"}})
                                                    ;; Normal: if x is rule-class, then x has property has-val
                                                      {"where"  {"@id"   "?x"
                                                                 "@type" rule-class}
                                                       "insert" {"@id"    "?x"
                                                                 property has-val*}})]
                                           (if rule
                                             (conj acc* [(str rule-class "(owl:hasValue-forward-" property ")") rule])
                                             acc*))
                                         acc*)))
                                   []
                                   has-value)

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
         cls-int2 (conj [(str rule-class "(owl:intersectionOf-2)#" (hash class-list)) cls-int2])
         (seq all-values-rules) (into all-values-rules)
         (seq has-value-rules) (into has-value-rules))))
   []
   intersection-of-statements))

(defn equiv-union-of
  "Handles rules cls-uni, scm-uni"
  [rule-class union-of-statements inserts]
  (reduce
   (fn [acc union-of-statement]
     (let [unions            (util/unwrap-list (get union-of-statement const/iri-owl:unionOf))
           ;; Process each union member to extract classes (including from nested unions)
           expanded-classes  (reduce (fn [acc* union-member]
                                       (cond
                                         ;; Simple class
                                         (and (util/get-id union-member)
                                              (not (contains? union-member const/iri-owl:unionOf)))
                                         (conj acc* (util/get-id union-member))

                                         ;; Nested union - flatten it
                                         (contains? union-member const/iri-owl:unionOf)
                                         (let [nested-unions (util/unwrap-list (get union-member const/iri-owl:unionOf))]
                                           (reduce (fn [acc** nested]
                                                     (if-let [nested-id (util/get-id nested)]
                                                       (conj acc** nested-id)
                                                       acc**))
                                                   acc*
                                                   nested-unions))

                                         :else acc*))
                                     []
                                     unions)
           {:keys [has-value]} (group-by equiv-class-type unions)
           restrictions      (cond->> []
                               has-value (into (has-value-condition "?y" has-value)))
           restriction-rules (map (fn [where]
                                    [(str rule-class "(owl:unionOf->owl:hasValue)#" (hash where))
                                     {"where"  where
                                      "insert" {"@id"   "?y"
                                                "@type" rule-class}}])
                                  restrictions)
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
                              expanded-classes)

           triples           (reduce
                              (fn [triples* c]
                                (conj triples* [c const/iri-rdfs:subClassOf {"@id" rule-class}]))
                              []
                              expanded-classes)]

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
        equiv-class-val (get owl-statement const/iri-owl:equivalentClass)
        unwrapped (util/unwrap-list equiv-class-val)
        ;; Ensure unwrapped is always a sequence for group-by
        unwrapped-seq (if (sequential? unwrapped) unwrapped [unwrapped])
        ;; combine with all other equivalent classes for a set of 2+ total classes
        {:keys [classes intersection-of union-of one-of
                has-value some-values all-values
                max-cardinality max-qual-cardinality]} (->> unwrapped-seq
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
  [_ _ owl-statement _all-rules]
  (throw (ex-info "Unsupported OWL statement" {:owl-statement owl-statement})))

(def base-rules
  [;; eq-sym, eq-trans, eq-rep-s covered by below rule
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
  (log/debug "OWL->Datalog processing" (count owl-graph) "statements")
  (let [rules (->> owl-graph
                   (mapcat (partial statement->datalog inserts))
                   (into base-rules))]
    (log/debug "Generated" (count rules) "rules from OWL statements")
    (doseq [[id _] (take 5 rules)]
      (log/debug "  Rule:" id))
    rules))
