(ns fluree.db.virtual-graph.bm25.stemmer
  (:import (org.tartarus.snowball SnowballStemmer)
           (org.tartarus.snowball.ext englishStemmer)))

(set! *warn-on-reflection* true)

(defprotocol Stemmer
  (stem [stemmer word]))

(extend-protocol Stemmer
  SnowballStemmer
  (stem [snowball word]
    (doto snowball
      (.setCurrent word)
      (.stem))
    (.getCurrent snowball)))

;; TODO need to add additional language support - right now everything is english
(defn initialize
  [lang]
  (englishStemmer.))
