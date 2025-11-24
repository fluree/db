(ns fluree.db.api.decode
  "API-layer decoding utilities for converting internal SIDs to external IRIs."
  (:require [fluree.db.json-ld.iri :as iri]))

(defn property-data
  "Decodes property data SIDs to IRIs. Returns map with :types, :ref-classes, :langs.
   Each contains a map of SID/IRI -> count."
  [prop-data ns-codes]
  (cond-> {:types {} :ref-classes {} :langs {}}
    (:types prop-data)
    (assoc :types (reduce-kv (fn [acc sid count]
                               (assoc acc (iri/sid->iri sid ns-codes) count))
                             {}
                             (:types prop-data)))

    (:ref-classes prop-data)
    (assoc :ref-classes (reduce-kv (fn [acc sid count]
                                     (assoc acc (iri/sid->iri sid ns-codes) count))
                                   {}
                                   (:ref-classes prop-data)))

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
  (reduce-kv
   (fn [acc sid val]
     (assoc acc (iri/sid->iri sid ns-codes) val))
   {}
   m))

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
  (reduce-kv #(assoc %1 %3 %2) {} ns-codes))

(defn- decode-sid-set
  "Converts a set of SIDs to a vector of IRIs."
  [sid-set ns-codes]
  (when (seq sid-set)
    (vec (map #(iri/sid->iri % ns-codes) sid-set))))

(defn- merge-property-hierarchy
  "Merges property hierarchy (subPropertyOf) into property stats.
   Returns property stats with :sub-property-of added where applicable."
  [property-stats schema ns-codes]
  (let [pred-map (get schema :pred {})]
    (reduce-kv
     (fn [acc prop-iri prop-stats]
       (let [;; Find the SID for this property IRI
             prop-sid (some (fn [[sid prop-data]]
                              (when (= (:iri prop-data) prop-iri)
                                sid))
                            pred-map)
             ;; Get parent properties (subPropertyOf)
             parent-props (when prop-sid
                            (get-in pred-map [prop-sid :parentProps]))
             ;; Decode parent SIDs to IRIs
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
       (let [class-sid (some (fn [[sid class-info]]
                               (when (= (:iri class-info) class-iri)
                                 sid))
                             pred-map)
             parent-classes (when class-sid
                              (get-in pred-map [class-sid :subclassOf]))
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
