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

(defmulti to-datalog (fn [rule-type owl-statement]
                       rule-type))

(defmethod to-datalog ::prp-dom
  [_ owl-statement]
  (let [domain   (->> (get owl-statement $rdfs-domain)
                      (mapv :id))
        property (:id owl-statement)
        rule     {"where"    {"@id"    "?s",
                              property nil},
                  "insert"   {"@id"   "?s",
                              "@type" domain}}]
    ;; rule-id *is* the property
    [property rule]))

(defmethod to-datalog :default
  [_ owl-statement]
  (throw (ex-info "Unsupported OWL statement" {:owl-statement owl-statement})))


(defn statement->datalog
  [owl-statement]
  (cond
    (contains? owl-statement $rdfs-domain)
    (to-datalog ::prp-dom owl-statement)

    :else
    (to-datalog :default owl-statement)))

(defn owl->datalog
  [owl-graph]
  (mapv statement->datalog owl-graph))


