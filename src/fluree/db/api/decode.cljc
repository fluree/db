(ns fluree.db.api.decode
  "API-layer decoding utilities for converting internal SIDs to external IRIs."
  (:require [fluree.db.json-ld.iri :as iri]))

(defn property-data
  "Decodes property data SIDs to IRIs. Returns map with :types, :ref-classes, :langs."
  [prop-data ns-codes]
  (cond-> {:types #{} :ref-classes #{} :langs #{}}
    (:types prop-data)
    (assoc :types (into #{} (map #(iri/sid->iri % ns-codes)) (:types prop-data)))

    (:ref-classes prop-data)
    (assoc :ref-classes (into #{} (map #(iri/sid->iri % ns-codes)) (:ref-classes prop-data)))

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

(defn ledger-info
  "Decodes ledger info by converting SIDs to IRIs and preparing for external consumption."
  [info]
  (let [ns-codes (:namespace-codes info)
        props (sid-keys (get-in info [:stats :properties]) ns-codes)
        class-stats (classes (get-in info [:stats :classes] {}) ns-codes)
        inverted-ns (invert-namespace-codes ns-codes)]
    (-> info
        (assoc-in [:stats :properties] props)
        (assoc-in [:stats :classes] class-stats)
        (assoc :namespace-codes inverted-ns)
        (dissoc :novelty-post))))
