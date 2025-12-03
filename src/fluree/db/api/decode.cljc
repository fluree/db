(ns fluree.db.api.decode
  "API-layer decoding utilities for converting internal SIDs to external IRIs."
  (:require [clojure.set :as set]
            [fluree.db.json-ld.iri :as iri]))

(defn property-data
  "Decodes property data SIDs to IRIs. Returns map with :types, :ref-classes, :langs.
   Each contains a map of SID/IRI -> count."
  [prop-data ns-codes]
  (cond-> {:types {} :ref-classes {} :langs {}}
    (:types prop-data)
    (assoc :types (update-keys (:types prop-data) #(iri/sid->iri % ns-codes)))

    (:ref-classes prop-data)
    (assoc :ref-classes (update-keys (:ref-classes prop-data) #(iri/sid->iri % ns-codes)))

    (:langs prop-data)
    (assoc :langs (:langs prop-data))))

(defn class-properties
  "Decodes nested property maps within class stats."
  [props ns-codes]
  (reduce-kv
   (fn [acc prop-sid prop-data]
     (let [prop-iri (iri/sid->iri prop-sid ns-codes)
           decoded (property-data prop-data ns-codes)]
       (assoc acc prop-iri decoded)))
   {}
   props))

(defn sid-keys
  "Decodes a map's SID keys to IRIs."
  [m ns-codes]
  (update-keys m #(iri/sid->iri % ns-codes)))

(defn classes
  "Decodes class stats map, including nested property details."
  [class-stats ns-codes]
  (reduce-kv
   (fn [acc class-sid stats]
     (let [class-iri (iri/sid->iri class-sid ns-codes)
           decoded-stats (if-let [props (:properties stats)]
                           (assoc stats :properties (class-properties props ns-codes))
                           stats)]
       (assoc acc class-iri decoded-stats)))
   {}
   class-stats))

(defn invert-namespace-codes
  "Inverts namespace-codes map from {code -> ns} to {ns -> code}."
  [ns-codes]
  (set/map-invert ns-codes))

(defn- decode-sid-set
  "Converts a set of SIDs to a vector of IRIs."
  [sid-set ns-codes]
  (when (seq sid-set)
    (mapv #(iri/sid->iri % ns-codes) sid-set)))

(defn- merge-property-hierarchy
  "Merges property hierarchy (subPropertyOf) into property stats.
   Returns property stats with :sub-property-of added where applicable."
  [property-stats schema ns-codes]
  (let [pred-map (get schema :pred {})]
    (reduce-kv
     (fn [acc prop-iri prop-stats]
       ;; pred-map is indexed by both SID and IRI, so we can look up directly
       (let [parent-props (get-in pred-map [prop-iri :parentProps])
             parent-iris (decode-sid-set parent-props ns-codes)]
         (if parent-iris
           (assoc acc prop-iri (assoc prop-stats :sub-property-of parent-iris))
           (assoc acc prop-iri prop-stats))))
     {}
     property-stats)))

(defn- merge-class-hierarchy
  "Merges class hierarchy (subClassOf) into class stats.
   Returns class stats with :subclass-of added where applicable."
  [class-stats schema ns-codes]
  (let [pred-map (get schema :pred {})]
    (reduce-kv
     (fn [acc class-iri class-data]
       ;; pred-map is indexed by both SID and IRI, so we can look up directly
       (let [parent-classes (get-in pred-map [class-iri :subclassOf])
             parent-iris (decode-sid-set parent-classes ns-codes)]
         (if parent-iris
           (assoc acc class-iri (assoc class-data :subclass-of parent-iris))
           (assoc acc class-iri class-data))))
     {}
     class-stats)))

(defn ledger-info
  "Decodes ledger info by converting SIDs to IRIs and preparing for external consumption.
   Merges schema hierarchy (subClassOf, subPropertyOf) into stats for classes and properties."
  [info]
  (let [ns-codes (:namespace-codes info)
        schema (:schema info)
        props (sid-keys (get-in info [:stats :properties]) ns-codes)
        class-stats (classes (get-in info [:stats :classes] {}) ns-codes)
        ;; Merge hierarchy info into stats
        props-with-hierarchy (merge-property-hierarchy props schema ns-codes)
        classes-with-hierarchy (merge-class-hierarchy class-stats schema ns-codes)
        inverted-ns (invert-namespace-codes ns-codes)]
    (-> info
        (assoc-in [:stats :properties] props-with-hierarchy)
        (assoc-in [:stats :classes] classes-with-hierarchy)
        (assoc :namespace-codes inverted-ns)
        (dissoc :novelty-post :schema))))
