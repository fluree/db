(ns fluree.db.virtual-graph.bm25.stopwords
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]))

(set! *warn-on-reflection* true)

(defn lang-filename
  [lang]
  (-> lang str/lower-case (str ".txt")))

(defn resource-path
  [filename]
  (str (io/file "stopwords" filename)))

(defn read-lang
  [lang]
  (-> lang lang-filename resource-path io/resource slurp (str/split #"[\n\r\s]+") set))

(defn initialize
  "Returns the default set of stop words for `lang` combined with the extra stop
  words `extras`, if present."
  ([lang]
   (initialize lang #{}))
  ([lang extras]
   (-> lang read-lang (set/union extras))))
