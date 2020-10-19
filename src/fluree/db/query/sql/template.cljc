(ns fluree.db.query.sql.template
  (:require [clojure.string :as str]))

(defn template-for
  [kw]
  (str "@<" kw ">"))

(defn fill-in
  [tmpl-str tmpl v]
  (str/replace tmpl-str tmpl v))


(def subject
  "Template for representing flake subjects"
  (template-for :subject))

(def subject-var
  "Template for storing flake subjects as variables"
  (str "?" subject))

(defn fill-in-subject
  "Fills in the flake subject value `subj` wherever the subject template appears
  in `tmpl-str`"
  [tmpl-str subj]
  (fill-in tmpl-str subject subj))


(def collection
  "Template for representing flake collections"
  (template-for :collection))

(defn field->predicate-template
  "Build a flake predicate template string from the collection template and the
  known field value `field`"
  [field]
  (str collection "/" field))

(defn fill-in-collection
  "Fills in the known collection value `coll` wherever the collection template
  appears in `tmpl-str`"
  [tmpl-str coll]
  (fill-in tmpl-str collection coll))
