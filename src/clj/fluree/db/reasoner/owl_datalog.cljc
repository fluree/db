(ns fluree.db.reasoner.owl-datalog
  (:require [fluree.db.util.core :as util]
            [fluree.db.util.json :as json]))

;; conversions of owl statements to datalog

(def ^:const $rdfs-domain "http://www.w3.org/2000/01/rdf-schema#domain")
(def ^:const $rdfs-range "http://www.w3.org/2000/01/rdf-schema#range")
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

(defmulti to-datalog (fn [rule-type owl-statement all-rules]
                       rule-type))

(defmethod to-datalog ::prp-dom
  [_ owl-statement all-rules]
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
  [_ owl-statement all-rules]
  (let [range    (->> (get owl-statement $rdfs-range)
                      (mapv :id))
        property (:id owl-statement)
        rule     {"where"  {"@id"    nil,
                            property "?ps"},
                  "insert" {"@id"   "?ps",
                            "@type" range}}]
    ;; rule-id *is* the property
    (conj all-rules [(str property "(prp-rng)") rule])))

(defmethod to-datalog :default
  [_ owl-statement all-rules]
  (throw (ex-info "Unsupported OWL statement" {:owl-statement owl-statement})))


(defn statement->datalog
  [owl-statement]
  (cond->> []
           (contains? owl-statement $rdfs-domain)
           (to-datalog ::prp-dom owl-statement)

           (contains? owl-statement $rdfs-range)
           (to-datalog ::prp-rng owl-statement)

           ))

(defn owl->datalog
  [owl-graph]
  (mapcat statement->datalog owl-graph))




