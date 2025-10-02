(ns fluree.db.util.branch
  "Utility functions for branch metadata management and conversion between
   internal representation and flat JSON-LD fields."
  (:require [clojure.set :as set]))

(def metadata-field-mapping
  "Mapping between internal metadata keys and JSON-LD field names"
  {:created-at    "f:createdAt"
   :source-branch "f:sourceBranch"
   :source-commit "f:sourceCommit"
   :protected     "f:protected"
   :description   "f:description"})

(defn metadata->flat-fields
  "Convert internal metadata map to flat JSON-LD fields.
   Returns a map with 'f:' prefixed field names."
  [metadata]
  (reduce-kv (fn [acc k v]
               (if-let [field-name (get metadata-field-mapping k)]
                 (if (some? v)
                   (assoc acc field-name v)
                   acc)
                 acc))
             {}
             metadata))

(defn flat-fields->metadata
  "Convert flat JSON-LD fields to internal metadata map.
   Extracts 'f:' prefixed fields and converts to internal keys."
  [data]
  (reduce-kv (fn [acc field-name internal-key]
               (if-let [value (get data field-name)]
                 (assoc acc internal-key value)
                 acc))
             {}
             (set/map-invert metadata-field-mapping)))

(defn augment-commit-with-metadata
  "Add flat metadata fields to a commit JSON-LD document.
   Returns the commit with metadata fields added."
  [commit metadata]
  (merge commit (metadata->flat-fields metadata)))

(defn branch-creation-response
  "Create a standardized branch creation response map."
  [branch-name metadata commit-id]
  {:name          branch-name
   :created-at    (:created-at metadata)
   :source-branch (:source-branch metadata)
   :source-commit (:source-commit metadata)
   :head          commit-id})

(defn extract-branch-metadata
  "Extract branch metadata from a nameservice record or similar structure.
   Returns only the metadata fields, not other record data."
  [record]
  (flat-fields->metadata record))