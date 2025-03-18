(ns fluree.db.query.sql.template
  (:require [clojure.string :as str]
            [clojure.walk :refer [postwalk]]))

#?(:clj (set! *warn-on-reflection* true))

(defn capitalize-first
  "Capitalizes the first letter (and only the first letter) of `s`"
  [s]
  (if (<= (count s) 1)
    (str/upper-case s)
    (-> (subs s 0 1)
        str/upper-case
        (str (subs s 1)))))

(defn combine-str
  "Combines `s1` and `s2` by concatenating `s1` with the result of capitalizing
  the first character of `s2`"
  [s1 s2]
  (->> s2
       capitalize-first
       (str s1)))

(defn normalize
  "Formats `s` by removing any '/' and capitalizing the following character for
  each '/' removed"
  [s]
  (reduce combine-str (str/split s #"/")))

(defn build-var
  "Formats `s` as a var by prepending '?', filtering out '/', and lowerCamelCasing
  the remaining string"
  [s]
  (->> s
       normalize
       (str "?")))

(defn build-predicate
  "Formats the collection string `c` and the field string `f` by joining them
  with a '/'"
  [c f]
  (str c "/" f))

(defn predicate?
  "Returns true if `s` is a predicate string"
  [s]
  (and s
       (str/includes? s "/")))

(defn build-fn-call
  "Formats `terms` as a function call"
  ([fst sec]
   (build-fn-call [fst sec]))
  ([terms]
   (str "(" (str/join " " terms) ")")))

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
