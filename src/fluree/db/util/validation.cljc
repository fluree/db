(ns fluree.db.util.validation
  (:require [clojure.string :as str]
            [malli.core :as m]
            [fluree.db.util.log :as log]))

(def non-empty-string
  (m/schema [:string {:min 1}]))

(def value? (complement coll?))

(defn qualified-keyword->json-ld
  [kw]
  (str (namespace kw) ":" (name kw)))

(defn compact-iri->keyword
  "Converts compact IRI strings to keywords. If there is a colon in the iri,
  the part before the colon becomes the keyword's namespace.
  E.g.
  \"foo\" -> :foo
  \"foo:bar\" -> :foo/bar"
  [iri]
  (-> iri
      (str/split #":")
      (->> (cons nil)
           (take-last 2)
           (apply keyword))))

(def iri-key
  "Decodes all string values to keywords even if they don't look like compact
  IRIs. Intended to support e.g. \"id\" -> :id."
  (m/schema
    [:orn
     {:decode/json
      (fn [v]
        (log/debug "decoding iri key:" v)
        (if (string? v)
          (if (str/includes? v "://") ; non-compact IRI
            v
            (compact-iri->keyword v))
          v))
      :encode/json
      (fn [v]
        (log/debug "encoding iri key:" v)
        (if (qualified-keyword? v)
          (qualified-keyword->json-ld v)
          v))}
     [:string non-empty-string]
     [:keyword :keyword]]))

(def iri
  "Decodes only compact IRIs to qualified keywords, e.g. \"foo:bar\" -> :foo/bar
  but leaves the value as-is otherwise."
  (m/schema
    [:orn
     {:decode/json
      (fn [v]
        (log/debug "decoding iri:" v)
        (if (string? v)
          (if (str/includes? v "://") ; non-compact IRI
            v
            (if (str/includes? v ":") ; compact IRI
              (compact-iri->keyword v)
              v))
          v))
      :encode/json
      (fn [v]
        (log/debug "encoding iri:" v)
        (if (qualified-keyword? v)
          (qualified-keyword->json-ld v)
          v))}
     [:string non-empty-string]
     [:keyword :keyword]]))

