(ns fluree.db.virtual-graph.bm25.stemmer
  (:import (org.tartarus.snowball SnowballStemmer)
           (org.tartarus.snowball.ext englishStemmer)))

(set! *warn-on-reflection* true)

(defn stem
  [^SnowballStemmer stemmer word]
  (doto stemmer
    (.setCurrent word)
    (.stem))
  (.getCurrent stemmer))

;; TODO need to add additional language support - right now everything is english
(defn initialize
  [lang]
  (englishStemmer.))
