(ns fluree.db.reasoner.owl-datalog
  (:require [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log]
            [fluree.db.util.json :as json]))

;; conversions of owl statements to datalog

;; property expressions
(def ^:const $rdfs-domain "http://www.w3.org/2000/01/rdf-schema#domain")
(def ^:const $rdfs-range "http://www.w3.org/2000/01/rdf-schema#range")
(def ^:const $owl-sameAs "http://www.w3.org/2002/07/owl#sameAs")
(def ^:const $owl-FunctionalProperty "http://www.w3.org/2002/07/owl#FunctionalProperty")
(def ^:const $owl-InverseFunctionalProperty "http://www.w3.org/2002/07/owl#InverseFunctionalProperty")
(def ^:const $owl-SymetricProperty "http://www.w3.org/2002/07/owl#SymetricProperty")
(def ^:const $owl-TransitiveProperty "http://www.w3.org/2002/07/owl#TransitiveProperty")
(def ^:const $owl-propertyChainAxiom "http://www.w3.org/2002/07/owl#propertyChainAxiom")
(def ^:const $owl-inverseOf "http://www.w3.org/2002/07/owl#inverseOf")
(def ^:const $owl-hasKey "http://www.w3.org/2002/07/owl#hasKey")

;; class expressions
(def ^:const $owl-equivalentClass "http://www.w3.org/2002/07/owl#equivalentClass")
(def ^:const $owl-intersectionOf "http://www.w3.org/2002/07/owl#intersectionOf")
(def ^:const $owl-unionOf "http://www.w3.org/2002/07/owl#unionOf")
(def ^:const $owl-Restriction "http://www.w3.org/2002/07/owl#Restriction")
(def ^:const $owl-onProperty "http://www.w3.org/2002/07/owl#onProperty")
(def ^:const $owl-onClass "http://www.w3.org/2002/07/owl#onClass")
(def ^:const $owl-hasValue "http://www.w3.org/2002/07/owl#hasValue")
(def ^:const $owl-someValuesFrom "http://www.w3.org/2002/07/owl#someValuesFrom")
(def ^:const $owl-allValuesFrom "http://www.w3.org/2002/07/owl#allValuesFrom")
(def ^:const $owl-maxCardinality "http://www.w3.org/2002/07/owl#maxCardinality")
(def ^:const $owl-maxQualifiedCardinality "http://www.w3.org/2002/07/owl#maxQualifiedCardinality")

(def ^:const $owl-Class "http://www.w3.org/2002/07/owl#Class")
(def ^:const $owl-Thing "http://www.w3.org/2002/07/owl#Thing")


(defn blank-node
  []
  (str "_:" (rand-int 2147483647)))

(defmulti to-datalog (fn [rule-type inserts owl-statement all-rules]
                       rule-type))

(defmethod to-datalog ::eq-sym
  [_ inserts owl-statement all-rules]
  ;; note any owl:sameAs are just inserts into the current db
  ;; the owl:sameAs rule is a base rule for any existing owl:sameAs
  ;; that might already existing in the current db
  (let [id      (:id owl-statement)
        sa-ids  (->> (get owl-statement $owl-sameAs)
                     util/sequential
                     (mapv :id))
        rule-id (str $owl-sameAs "(" id ")")
        triples (->> sa-ids
                     (mapcat (fn [sa-id]
                               ;; insert triples both ways
                               [[id $owl-sameAs {"@id" sa-id}]
                                [sa-id $owl-sameAs {"@id" id}]]))
                     set)]

    ;; insert sameAs triples into inserts atom
    (swap! inserts update rule-id (fn [et]
                                    (into triples et)))

    all-rules))

(defmethod to-datalog ::prp-dom
  [_ _ owl-statement all-rules]
  (let [domain   (->> (get owl-statement $rdfs-domain)
                      (mapv :id))
        property (:id owl-statement)
        rule     {"where"  {"@id"    "?s"
                            property nil}
                  "insert" {"@id"   "?s"
                            "@type" domain}}]
    ;; rule-id *is* the property
    (conj all-rules [(str property "(rdfs:domain)") rule])))

(defmethod to-datalog ::prp-rng
  [_ _ owl-statement all-rules]
  (let [range    (->> (get owl-statement $rdfs-range)
                      (mapv :id))
        property (:id owl-statement)
        rule     {"where"  {"@id"    nil,
                            property "?ps"}
                  "insert" {"@id"   "?ps"
                            "@type" range}}]
    ;; rule-id *is* the property
    (conj all-rules [(str property "(rdfs:range)") rule])))

;; TODO - re-enable once filter function bug is fixed
(defmethod to-datalog ::prp-fp
  [_ _ owl-statement all-rules]
  (do
    (log/warn "FunctionalProperty not supported yet")
    all-rules)
  #_(let [fp   (:id owl-statement)
          rule {"where"  [{"@id" "?s"
                           fp    "?fp-vals"}
                          {"@id" "?s"
                           fp    "?fp-vals2"}
                          ["filter" "(not= ?fp-vals ?fp-vals2)"]]
                "insert" {"@id"       "?fp-vals"
                          $owl-sameAs "?fp-vals2"}}]
      (conj all-rules [(str $owl-FunctionalProperty "(" fp ")") rule])))

;; TODO - re-enable once filter function bug is fixed
(defmethod to-datalog ::prp-ifp
  [_ _ owl-statement all-rules]
  (do
    (log/warn "InverseFunctionalProperty not supported yet")
    all-rules)
  #_(let [ifp  (:id owl-statement)
          rule {"where"  [{"@id" "?x1"
                           ifp   "?y"}
                          {"@id" "?x2"
                           ifp   "?y"}
                          ["filter" "(not= ?x1 ?x2)"]]
                "insert" {"@id"       "?x1"
                          $owl-sameAs "?x2"}}]
      (conj all-rules [(str $owl-InverseFunctionalProperty "(" ifp ")") rule])))

(defmethod to-datalog ::prp-symp
  [_ _ owl-statement all-rules]
  (let [symp (:id owl-statement)
        rule {"where"  [{"@id" "?x"
                         symp  "?y"}]
              "insert" {"@id" "?y"
                        symp  "?x"}}]
    (conj all-rules [(str symp "(owl:SymetricProperty)") rule])))

(defmethod to-datalog ::prp-trp
  [_ _ owl-statement all-rules]
  (let [trp  (:id owl-statement)
        rule {"where"  [{"@id" "?x"
                         trp   "?y"}
                        {"@id" "?y"
                         trp   "?z"}]
              "insert" {"@id" "?x"
                        trp   "?z"}}]
    (conj all-rules [(str trp "(owl:TransitiveProperty)") rule])))

;; turns list of properties into:
;; [{"@id" "?u0", "?p1" "?u1"}, {"@id" "?u2", "?p2" "?u3"} ... ]
(defmethod to-datalog ::prp-spo2
  [_ _ owl-statement all-rules]
  (try*
    (let [prop    (:id owl-statement)
          p-chain (->> (get owl-statement $owl-propertyChainAxiom)
                       first
                       :list
                       (mapv :id))
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
  (let [prop     (util/get-first-id owl-statement $owl-inverseOf)
        inv-prop (:id owl-statement)
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
                   (map :id)
                   (into #{rule-class}))]
    (map-indexed (fn [idx cn]
                   (let [rule-id (str rule-class "(owl:equivalentClass-" idx ")")
                         rule    {"where"  {"@id"   "?x"
                                            "@type" cn}
                                  "insert" {"@id"   "?x"
                                            "@type" (into [] (disj c-set cn))}}]
                     [rule-id rule]))
                 c-set)))

;; TODO - re-enable once filter function bug is fixed
(defn equiv-class-max-qual-cardinality
  "Handles rule cls-maxqc3, cls-maxqc4"
  [rule-class restrictions]
  (do
    (log/warn "owl:maxQualifiedCardinality not supported yet")
    [])
  #_(reduce
      (fn [acc restriction]
        (let [property  (util/get-first-id restriction $owl-onProperty)
              max-q-val (util/get-first-value restriction $owl-maxQualifiedCardinality)
              on-class  (util/get-first-id restriction $owl-onClass)
              rule      (if (= $owl-Thing on-class)
                          ;; special case for rule cls-maxqc4 where onClass
                          ;; is owl:Thing, means every object of property is sameAs
                          {"where"  [{"@id"    "?u"
                                      "@type"  rule-class
                                      property "?y1"}
                                     {"@id"    "?u"
                                      property "?y2"}
                                     ["filter" "(not= ?y1 ?y2)"]]
                           "insert" {"@id"       "?y1"
                                     $owl-sameAs "?y2"}}
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
                           "insert" {"@id"       "?y1"
                                     $owl-sameAs "?y2"}})]
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

;; TODO - re-enable once filter function bug is fixed
(defn equiv-class-max-cardinality
  "Handles rule cls-maxc2"
  [rule-class restrictions]
  (do
    (log/warn "owl:maxCardinality not supported yet")
    [])
  #_(reduce
      (fn [acc restriction]
        (let [property (util/get-first-id restriction $owl-onProperty)
              max-val  (util/get-first-value restriction $owl-maxCardinality)
              rule     {"where"  [{"@id"    "?x"
                                   "@type"  rule-class
                                   property "?y1"}
                                  {"@id"    "?x"
                                   "@type"  rule-class
                                   property "?y2"}
                                  ["filter" "(not= ?y1 ?y2)"]]
                        "insert" {"@id"       "?y1"
                                  $owl-sameAs "?y2"}}]
          (if (and property (= 1 max-val))
            (conj acc [(str rule-class "(owl:maxCardinality-" property ")") rule])
            (do (log/info "owl:Restriction for class" rule-class
                          "is being ignored. owl:maxCardinality can only infer"
                          "new facts when owl:maxCardinality=1. Property" property
                          "has owl:maxCardinality equal to:" max-val)
                acc))))
      []
      restrictions))

(defn equiv-class-all-values
  "Handles rules cls-avf"
  [rule-class restrictions]
  (reduce
    (fn [acc restriction]
      (let [property (util/get-first-id restriction $owl-onProperty)
            all-val  (util/get-first-id restriction $owl-allValuesFrom)
            rule     {"where"  {"@id"    "?x"
                                property "?y"}
                      "insert" {"@id"   "?y"
                                "@type" all-val}}]
        (if (and property all-val)
          (conj acc [(str rule-class "(owl:allValuesFrom-" property ")") rule])
          (do (log/warn "owl:Restriction for class" rule-class
                        "is not properly defined. owl:onProperty is:" (get restriction $owl-onProperty)
                        "and owl:allValuesFrom is:" (util/get-first restriction $owl-allValuesFrom)
                        ". onProperty must exist and be an IRI (wrapped in {@id: ...})."
                        "allValuesFrom must exist and must be an IRI.")
              acc))))
    []
    restrictions))

(defn equiv-class-some-values
  "Handles rules cls-svf1, cls-svf2"
  [rule-class restrictions]
  (reduce
    (fn [acc restriction]
      (let [property (util/get-first-id restriction $owl-onProperty)
            some-val (util/get-first-id restriction $owl-someValuesFrom)
            rule     (if (= $owl-Thing some-val)
                       ;; special case where someValuesFrom is owl:Thing, means
                       ;; everything with property should be in the class (cls-svf2)
                       {"where"  {"@id"    "?x"
                                  property nil}
                        "insert" {"@id"   "?x"
                                  "@type" rule-class}}
                       ;; normal case where someValuesFrom is a class (cls-svf1)
                       {"where"  [{"@id"    "?x"
                                   property "?y"}
                                  {"@id"   "?y"
                                   "@type" some-val}]
                        "insert" {"@id"   "?x"
                                  "@type" rule-class}})]
        (if (and property some-val)
          (conj acc [(str rule-class "(owl:someValuesFrom-" property ")") rule])
          (do (log/warn "owl:Restriction for class" rule-class
                        "is not properly defined. owl:onProperty is:" (get restriction $owl-onProperty)
                        "and owl:someValuesFrom is:" (util/get-first restriction $owl-someValuesFrom)
                        ". onProperty must exist and be an IRI (wrapped in {@id: ...})."
                        "someValuesFrom must exist and must be an IRI.")
              acc))))
    []
    restrictions))

(defn equiv-class-has-value
  "Handles rules cls-hv1, cls-hv2"
  [rule-class restrictions]
  (reduce
    (fn [acc restriction]
      (let [property (util/get-first-id restriction $owl-onProperty)
            one-val  (if-let [one-val (util/get-first restriction $owl-hasValue)]
                       (if-let [one-val-id (:id one-val)]
                         {"@id" one-val-id}
                         (:value one-val)))
            rule1    {"where"  {"@id"    "?x"
                                property one-val}
                      "insert" {"@id"   "?x"
                                "@type" rule-class}}
            rule2    {"where"  {"@id"   "?x"
                                "@type" rule-class}
                      "insert" {"@id"    "?x"
                                property one-val}}]
        (if (and property one-val)
          (-> acc
              (conj [(str rule-class "(owl:Restriction-" property "-1)") rule1])
              (conj [(str rule-class "(owl:Restriction-" property "-2)") rule2]))
          (do (log/warn "owl:Restriction for class" rule-class
                        "is not properly defined. owl:onProperty is:" (get restriction $owl-onProperty)
                        "and owl:hasValue is:" (util/get-first restriction $owl-hasValue)
                        ". onProperty must exist and be an IRI (wrapped in {@id: ...})."
                        "hasValue must exist, but can be an IRI or literal value.")
              acc))))
    []
    restrictions))

(defn equiv-class-type
  [equiv-class-statement]
  (cond (some #(= % $owl-Restriction) (:type equiv-class-statement))
        (cond
          (contains? equiv-class-statement $owl-hasValue)
          :has-value

          (contains? equiv-class-statement $owl-someValuesFrom)
          :some-values

          (contains? equiv-class-statement $owl-allValuesFrom)
          :all-values

          (contains? equiv-class-statement $owl-maxCardinality)
          :max-cardinality

          (contains? equiv-class-statement $owl-maxQualifiedCardinality)
          :max-qual-cardinality

          :else
          (do
            (log/warn "Unsupported owl:Restriction" equiv-class-statement)
            nil))

        (contains? equiv-class-statement $owl-intersectionOf)
        :intersection-of

        (contains? equiv-class-statement $owl-unionOf)
        :union-of

        (contains? equiv-class-statement :id)
        :classes

        :else nil))

(defmethod to-datalog ::cax-eqc
  [_ _ owl-statement all-rules]
  (let [c1 (:id owl-statement) ;; the class which is the subject
        ;; combine with all other equivalent classes for a set of 2+ total classes
        {:keys [classes intersection-of union-of
                has-value some-values all-values
                max-cardinality max-qual-cardinality]} (group-by equiv-class-type (get owl-statement $owl-equivalentClass))]
    (cond-> all-rules
            classes (into (equiv-class-rules c1 classes)) ;; cax-eqc1, cax-eqc2
            has-value (into (equiv-class-has-value c1 has-value)) ;; cls-hv1, cls-hv1
            some-values (into (equiv-class-some-values c1 some-values)) ;; cls-svf1, cls-svf2
            all-values (into (equiv-class-all-values c1 all-values)) ;; cls-svf1, cls-svf2
            max-cardinality (into (equiv-class-max-cardinality c1 max-cardinality)) ;; cls-maxc2
            max-qual-cardinality (into (equiv-class-max-qual-cardinality c1 max-qual-cardinality)) ;; cls-maxqc3, cls-maxqc4
            )))


;; TODO - re-enable once filter function bug is fixed
(defmethod to-datalog ::prp-key
  [_ _ owl-statement all-rules]
  (do
    (log/warn "InverseFunctionalProperty not supported yet")
    all-rules)
  #_(let [class     (:id owl-statement)
          props     (get owl-statement $owl-hasKey)
          ;; support props in either @list form, or just as a set of values
          prop-list (or (some->> props first :list (map :id))
                        (map props :id))
          where     (->> prop-list
                         (map-indexed (fn [idx prop]
                                        [prop (str "?z" idx)]))
                         (into {"@type" class}))
          rule      {"where"  [(assoc where "@id" "?x")
                               (assoc where "@id" "?y")
                               ["filter" "(not= ?x ?y)"]]
                     "insert" [{"@id"       "?x"
                                $owl-sameAs "?y"}
                               {"@id"       "?y"
                                $owl-sameAs "?x"}]}]
      (conj all-rules [(str $owl-hasKey "(" class ")") rule])))

(defmethod to-datalog :default
  [_ _ owl-statement all-rules]
  (throw (ex-info "Unsupported OWL statement" {:owl-statement owl-statement})))

(def base-rules
  [
   ;; eq-sym
   [$owl-sameAs
    {"where"  {"@id"       "?s"
               $owl-sameAs "?ps"}
     "insert" {"@id"       "?ps"
               $owl-sameAs "?s"}}]
   ;; eq-trans
   [(str $owl-sameAs "(trans)")
    {"where"  [{"@id"       "?s"
                $owl-sameAs "?same"}
               {"@id"       "?same"
                $owl-sameAs "?same-same"}]
     "insert" {"@id"       "?s"
               $owl-sameAs "?same-same"}}]])

(defn statement->datalog
  [inserts owl-statement]
  (cond
    (contains? owl-statement $owl-sameAs) ;; TODO - can probably take this out of multi-fn
    (to-datalog ::eq-sym inserts owl-statement [])

    :else
    (cond->> []
             (contains? owl-statement $rdfs-domain)
             (to-datalog ::prp-dom inserts owl-statement)

             (contains? owl-statement $rdfs-range)
             (to-datalog ::prp-rng inserts owl-statement)

             (contains? owl-statement $owl-propertyChainAxiom)
             (to-datalog ::prp-spo2 inserts owl-statement)

             (contains? owl-statement $owl-inverseOf)
             (to-datalog ::prp-inv inserts owl-statement)

             (contains? owl-statement $owl-hasKey)
             (to-datalog ::prp-key inserts owl-statement)

             (contains? owl-statement $owl-equivalentClass)
             (to-datalog ::cax-eqc inserts owl-statement)

             (some #(= $owl-FunctionalProperty %) (:type owl-statement))
             (to-datalog ::prp-fp inserts owl-statement)

             (some #(= $owl-InverseFunctionalProperty %) (:type owl-statement))
             (to-datalog ::prp-ifp inserts owl-statement)

             (some #(= $owl-SymetricProperty %) (:type owl-statement))
             (to-datalog ::prp-symp inserts owl-statement)

             (some #(= $owl-TransitiveProperty %) (:type owl-statement))
             (to-datalog ::prp-trp inserts owl-statement)

             )))

(defn owl->datalog
  [inserts owl-graph]
  (->> owl-graph
       (mapcat (partial statement->datalog inserts))
       (into base-rules)))

