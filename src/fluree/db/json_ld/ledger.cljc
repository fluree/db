(ns fluree.db.json-ld.ledger
  (:require [fluree.json-ld :as json-ld]
            [fluree.db.flake :as flake]
            [fluree.db.constants :as const]
            [clojure.string :as str]))

;; methods to link/trace back a ledger and return flakes
#?(:clj (set! *warn-on-reflection* true))

(def class+property-iris #{"http://www.w3.org/2000/01/rdf-schema#Class"
                           "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"
                           "http://www.w3.org/2002/07/owl#Class"
                           "http://www.w3.org/2002/07/owl#ObjectProperty"
                           "http://www.w3.org/2002/07/owl#DatatypeProperty"})


(defn class-or-property?
  [{:keys [type] :as node}]
  (some class+property-iris type))

(def predefined-properties
  {"http://www.w3.org/2000/01/rdf-schema#Class"          const/$rdfs:Class
   "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property" const/$rdf:Property
   "http://www.w3.org/2002/07/owl#Class"                 const/$owl:Class
   "http://www.w3.org/2002/07/owl#ObjectProperty"        const/$owl:ObjectProperty
   "http://www.w3.org/2002/07/owl#DatatypeProperty"      const/$owl:DatatypeProperty
   "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"     const/$rdf:type})

(defn flip-key-vals
  [map]
  (reduce #(assoc %1 (val %2) (key %2)) {} map))

(def predefined-sids
  (-> predefined-properties
      flip-key-vals
      ;; use @type json-ld shorthand instead of rdf:type full URL
      (assoc const/$rdf:type "@type")))

(defn predefined-sids-compact
  "Allows predefined sids to be mapped to values based on supplied compacting function
  generated from a context"
  [compact-fn]
  (reduce-kv
    (fn [acc k v]
      (let [v* (json-ld/compact v compact-fn)]
        (assoc acc k v*)))
    {}
    predefined-sids))

(defn last-pid
  [db]
  (-> db :ecount (get const/$_predicate)))

(defn last-sid
  [db]
  (or (-> db :ecount (get const/$_default))
      (dec (flake/->sid const/$_default 0))))

(defn generate-new-sid
  [{:keys [id] :as node} iris next-pid next-sid]
  (let [new-sid (if (class-or-property? node)
                  (next-pid)
                  (next-sid))]
    (vswap! iris assoc id new-sid)
    new-sid))

(defn generate-new-pid
  [property-iri iris next-pid]
  (let [new-pid (next-pid)]
    (vswap! iris assoc property-iri new-pid)
    new-pid))

(defn get-iri-sid
  "Gets the IRI for any existing subject ID."
  [iri db iris]
  (if-let [cached (get @iris iri)]
    cached
    ;; TODO following, if a retract was made there could be 2 matching flakes and want to make sure we take the latest add:true
    (when-let [sid (some-> (flake/match-post (get-in db [:novelty :post]) const/$iri iri)
                           first
                           :s)]
      (vswap! iris assoc iri sid)
      sid)))
