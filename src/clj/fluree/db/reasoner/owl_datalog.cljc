(ns fluree.db.reasoner.owl-datalog
  (:require [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log]
            [fluree.db.util.json :as json]))

;; conversions of owl statements to datalog

(def ^:const $rdfs-domain "http://www.w3.org/2000/01/rdf-schema#domain")
(def ^:const $rdfs-range "http://www.w3.org/2000/01/rdf-schema#range")
(def ^:const $owl-sameAs "http://www.w3.org/2002/07/owl#sameAs")
(def ^:const $owl-FunctionalProperty "http://www.w3.org/2002/07/owl#FunctionalProperty")
(def ^:const $owl-InverseFunctionalProperty "http://www.w3.org/2002/07/owl#InverseFunctionalProperty")
(def ^:const $owl-SymetricProperty "http://www.w3.org/2002/07/owl#SymetricProperty")
(def ^:const $owl-TransitiveProperty "http://www.w3.org/2002/07/owl#TransitiveProperty")
(def ^:const $owl-propertyChainAxiom "http://www.w3.org/2002/07/owl#propertyChainAxiom")

(def ^:const $owl-Class "http://www.w3.org/2002/07/owl#Class")
(def ^:const $owl-equivalentClass "http://www.w3.org/2002/07/owl#equivalentClass")
(def ^:const $owl-Restriction "http://www.w3.org/2002/07/owl#Restriction")
(def ^:const $owl-hasValue "http://www.w3.org/2002/07/owl#hasValue")
(def ^:const $owl-someValuesFrom "http://www.w3.org/2002/07/owl#someValuesFrom")
(def ^:const $owl-onProperty "http://www.w3.org/2002/07/owl#onProperty")


(def ^:const $f:rule "http://flur.ee/ns/ledger#rule")

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
    (conj all-rules [(str property "(prp-dom)") rule])))

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
    (conj all-rules [(str property "(prp-rng)") rule])))

(defmethod to-datalog ::prp-symp
  [_ _ owl-statement all-rules]
  (let [symp (:id owl-statement)
        rule {"where"  [{"@id" "?x"
                         symp  "?y"}]
              "insert" {"@id" "?y"
                        symp  "?x"}}]
    (conj all-rules [(str $owl-SymetricProperty "(" symp ")") rule])))

(defmethod to-datalog ::prp-trp
  [_ _ owl-statement all-rules]
  (let [trp  (:id owl-statement)
        rule {"where"  [{"@id" "?x"
                         trp   "?y"}
                        {"@id" "?y"
                         trp   "?z"}]
              "insert" {"@id" "?x"
                        trp   "?z"}}]
    (conj all-rules [(str $owl-TransitiveProperty "(" trp ")") rule])))


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
      (conj all-rules [(str $owl-propertyChainAxiom "(" prop "}") rule]))
    (catch* e (log/warn (str "Ignoring OWL rule " (ex-message e)))
            all-rules)))


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

