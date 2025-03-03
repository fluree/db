(ns fluree.db.virtual-graph.bm25.stopwords
  (:require [clojure.string :as str]))

(set! *warn-on-reflection* true)

;; TODO - need to add new stopword language support... right now everything is english
(defn initialize
  "Returns a fn that will return truthy if the word is a stopword"
  [lang]
  (let [filename (str "resources/stopwords/" (str/lower-case lang) ".txt")
        data     (slurp filename)
        tokens (set
                (str/split data #"[\n\r\s]+"))]
    (fn [word]
      (tokens word))))
