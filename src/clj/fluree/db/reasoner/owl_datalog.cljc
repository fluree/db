(ns fluree.db.reasoner.owl-datalog
  (:require [fluree.db.util.core :as util]
            [fluree.db.util.json :as json]))

;; conversions of owl statements to datalog

(def ^:const $rdfs-domain "http://www.w3.org/2000/01/rdf-schema#domain")
(def ^:const $rdfs-range "http://www.w3.org/2000/01/rdf-schema#range")
(def ^:const $owl-sameAs "http://www.w3.org/2002/07/owl#sameAs")
(def ^:const $owl-FunctionalProperty "http://www.w3.org/2002/07/owl#FunctionalProperty")
(def ^:const $owl-InverseFunctionalProperty "http://www.w3.org/2002/07/owl#InverseFunctionalProperty")
(def ^:const $owl-SymetricProperty "http://www.w3.org/2002/07/owl#SymetricProperty")

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
        rule     {"where"  {"@id"    "?s",
                            property nil},
                  "insert" {"@id"   "?s",
                            "@type" domain}}]
    ;; rule-id *is* the property
    (conj all-rules [(str property "(prp-dom)") rule])))

(defmethod to-datalog ::prp-rng
  [_ _ owl-statement all-rules]
  (let [range    (->> (get owl-statement $rdfs-range)
                      (mapv :id))
        property (:id owl-statement)
        rule     {"where"  {"@id"    nil,
                            property "?ps"},
                  "insert" {"@id"   "?ps",
                            "@type" range}}]
    ;; rule-id *is* the property
    (conj all-rules [(str property "(prp-rng)") rule])))

(defmethod to-datalog ::prp-symp
  [_ _ owl-statement all-rules]
  (let [symp (:id owl-statement)
        rule {"where"  [{"@id" "?x"
                         symp  "?y"}]
              "insert" {"@id" "?y",
                        symp  "?x"}}]
    ;; rule-id *is* the property
    (conj all-rules [(str $owl-SymetricProperty "(" symp ")") rule])))


(defmethod to-datalog :default
  [_ _ owl-statement all-rules]
  (throw (ex-info "Unsupported OWL statement" {:owl-statement owl-statement})))

(def base-rules
  [
   ;; eq-sym
   [$owl-sameAs
    {"where"  {"@id"       "?s",
               $owl-sameAs "?ps"},
     "insert" {"@id"       "?ps",
               $owl-sameAs "?s"}}]
   ;; eq-trans
   [(str $owl-sameAs "(trans)")
    {"where"  [{"@id"       "?s",
                $owl-sameAs "?same"}
               {"@id"       "?same",
                $owl-sameAs "?same-same"}],
     "insert" {"@id"       "?s",
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

             (some #(= $owl-SymetricProperty %) (:type owl-statement))
             (to-datalog ::prp-symp inserts owl-statement)

             )))

(defn owl->datalog
  [inserts owl-graph]
  (->> owl-graph
       (mapcat (partial statement->datalog inserts))
       (into base-rules)))

