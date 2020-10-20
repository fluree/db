(ns fluree.db.query.sql.template
  (:require [clojure.string :as str]
            [clojure.walk :refer [postwalk]]))

(defn build-var
  "Formats `s` as a var by prepending '?'"
  [s]
  (str "?" s))

(defn build-predicate
  "Formats the collection string `c` and the field string `f` by joining them
  with a '/'"
  [c f]
  (str c "/" f))

(defn build-fn-call
  "Formats `terms` as a function call"
  [terms]
  (str "(" (str/join " " terms) ")"))

(defn template-for
  [kw]
  (str "@<" kw ">"))

(defn fill-in
  [tmpl-str tmpl v]
  (str/replace tmpl-str tmpl v))


(def collection
  "Template for representing flake collections"
  (template-for :collection))

(def collection-var
  "Template for storing flake subjects as variables"
  (build-var collection))

(defn fill-in-collection
  "Fills in the known collection name `coll-name` wherever the collection template
  appears in `tmpl-str`"
  [coll-name tmpl-data]
  (postwalk (fn [c]
              (if (string? c)
                (fill-in c collection coll-name)
                c))
            tmpl-data))

(def field
  "Template for represent collection fields"
  (template-for :field))

(def field-var
  "Template for storing flake fields as variables"
  (build-var field))

(defn field->predicate-template
  "Build a flake predicate template string from the collection template and the
  known field value `f`"
  [f]
  (build-predicate collection f))

(def predicate
  "Template for representing flake predicates with both collection and field
  missing"
  (build-predicate collection field))
