(ns fluree.db.virtual-graph.iceberg.r2rml
  "R2RML parsing and vocabulary for Iceberg virtual graphs.

   R2RML (RDB to RDF Mapping Language) is a W3C standard for expressing
   mappings from relational databases to RDF. This namespace provides:

   - R2RML vocabulary constants (namespace IRIs)
   - Parsing of R2RML mappings from Turtle or JSON-LD
   - Extraction of mapping metadata (tables, columns, templates)

   See: https://www.w3.org/TR/r2rml/"
  (:require [clojure.string :as str]
            [fluree.db.constants :as const]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.fql.parse :as fql-parse]
            [fluree.db.query.turtle.parse :as turtle]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; R2RML Vocabulary
;;; ---------------------------------------------------------------------------

(def ^:const r2rml-ns "http://www.w3.org/ns/r2rml#")
(def ^:const r2rml-triples-map (str r2rml-ns "TriplesMap"))
(def ^:const r2rml-logical-table (str r2rml-ns "logicalTable"))
(def ^:const r2rml-table-name (str r2rml-ns "tableName"))
(def ^:const r2rml-subject-map (str r2rml-ns "subjectMap"))
(def ^:const r2rml-template (str r2rml-ns "template"))
(def ^:const r2rml-class (str r2rml-ns "class"))
(def ^:const r2rml-predicate-object-map (str r2rml-ns "predicateObjectMap"))
(def ^:const r2rml-predicate (str r2rml-ns "predicate"))
(def ^:const r2rml-object-map (str r2rml-ns "objectMap"))
(def ^:const r2rml-column (str r2rml-ns "column"))
(def ^:const r2rml-datatype (str r2rml-ns "datatype"))

;;; ---------------------------------------------------------------------------
;;; Parsing Helpers
;;; ---------------------------------------------------------------------------

(defn extract-template-cols
  "Extract column names from an R2RML template string.

   Templates use {columnName} syntax to reference columns.
   Example: 'http://example.org/airline/{id}' -> ['id']"
  [template]
  (when template
    (->> (re-seq #"\{([^}]+)\}" template)
         (map second)
         vec)))

(defn- get-iri
  "Extract IRI from a value, handling both raw strings and where-match maps."
  [x]
  (if (string? x) x (::where/iri x)))

;;; ---------------------------------------------------------------------------
;;; R2RML Parsing
;;; ---------------------------------------------------------------------------

(defn- parse-r2rml-from-triples
  "Parse R2RML mappings from grouped triples.

   Takes triples grouped by subject and extracts:
   - Logical table (table name)
   - Subject template
   - RDF class
   - Predicate-to-column mappings with optional datatypes

   Returns a map of {table-key -> mapping-info}."
  [by-subject]
  (->> by-subject
       (filter (fn [[_subject triples]]
                 (some (fn [[_s p o]]
                         (and (= const/iri-rdf-type (get-iri p))
                              (= r2rml-triples-map (get-iri o))))
                       triples)))
       (map (fn [[_subject triples]]
              (let [props (into {} (map (fn [[_s p o]] [(get-iri p) o]) triples))
                    logical-table-node (get-iri (get props r2rml-logical-table))
                    logical-table (when logical-table-node
                                    (let [lt-triples (get by-subject logical-table-node)
                                          table-name (some (fn [[_s p o]]
                                                             (when (= r2rml-table-name (get-iri p))
                                                               (::where/val o)))
                                                           lt-triples)]
                                      (when table-name
                                        {:type :table-name :name table-name})))
                    subject-map-node (get-iri (get props r2rml-subject-map))
                    [template rdf-class] (when subject-map-node
                                           (let [sm-triples (get by-subject subject-map-node)
                                                 template (some (fn [[_s p o]]
                                                                  (when (= r2rml-template (get-iri p))
                                                                    (::where/val o)))
                                                                sm-triples)
                                                 rdf-class (some (fn [[_s p o]]
                                                                   (when (= r2rml-class (get-iri p))
                                                                     (get-iri o)))
                                                                 sm-triples)]
                                             [template rdf-class]))
                    pom-nodes (keep (fn [[_s p o]]
                                      (when (= r2rml-predicate-object-map (get-iri p))
                                        (get-iri o)))
                                    triples)
                    predicates (reduce (fn [acc pom-node]
                                         (let [pom-id (get-iri pom-node)
                                               pom-triples (get by-subject pom-id)
                                               pred (some (fn [[_s p o]]
                                                            (when (= r2rml-predicate (get-iri p))
                                                              (or (get-iri o) (::where/val o))))
                                                          pom-triples)
                                               obj-map-node (some (fn [[_s p o]]
                                                                    (when (= r2rml-object-map (get-iri p))
                                                                      (get-iri o)))
                                                                  pom-triples)
                                               object-map (when obj-map-node
                                                            (let [om-triples (get by-subject obj-map-node)
                                                                  column (some (fn [[_s p o]]
                                                                                 (when (= r2rml-column (get-iri p))
                                                                                   (::where/val o)))
                                                                               om-triples)
                                                                  datatype (some (fn [[_s p o]]
                                                                                   (when (= r2rml-datatype (get-iri p))
                                                                                     (get-iri o)))
                                                                                 om-triples)]
                                                              (when column
                                                                {:type :column :value column :datatype datatype})))]
                                           (if (and pred object-map)
                                             (assoc acc pred object-map)
                                             acc)))
                                       {}
                                       pom-nodes)]
                (when logical-table
                  (let [table-key (keyword (str/replace (:name logical-table) "\"" ""))]
                    [table-key
                     {:logical-table logical-table
                      :table (:name logical-table)
                      :subject-template template
                      :class rdf-class
                      :predicates predicates}])))))
       (filter some?)
       (into {})))

(defn parse-r2rml
  "Parse an R2RML mapping from a file path, Turtle string, or JSON-LD.

   Args:
     mapping-source - One of:
       - File path to a .ttl or .json file
       - Inline Turtle string
       - Inline JSON-LD map/vector

   Returns a map of {table-key -> mapping} where each mapping contains:
     :logical-table    - {:type :table-name :name \"table\"}
     :table            - Table name string
     :subject-template - IRI template for subjects
     :class            - RDF class IRI
     :predicates       - Map of {predicate-iri -> {:type :column :value \"col\" :datatype \"xsd:...\"}}

   Example:
     (parse-r2rml \"/path/to/mapping.ttl\")
     ;; => {:airlines {:table \"openflights/airlines\"
     ;;                :class \"http://example.org/Airline\"
     ;;                :predicates {...}}}"
  [mapping-source]
  (let [content (cond
                  (and (string? mapping-source)
                       (.exists (java.io.File. ^String mapping-source)))
                  (slurp mapping-source)
                  :else mapping-source)
        turtle? (and (string? content)
                     (not (or (str/starts-with? (str/trim content) "{")
                              (str/starts-with? (str/trim content) "["))))
        triples (if turtle?
                  (turtle/parse content)
                  (fql-parse/jld->parsed-triples content nil
                                                 {"@vocab" "http://www.w3.org/ns/r2rml#"
                                                  "rr" "http://www.w3.org/ns/r2rml#"
                                                  "rdf" "http://www.w3.org/1999/02/22-rdf-syntax-ns#"}))
        by-subject (group-by #(get-iri (first %)) triples)]
    (parse-r2rml-from-triples by-subject)))
