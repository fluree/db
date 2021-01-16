(ns fluree.db.full-text
  (:require [fluree.db.constants :as const])
  (:import fluree.db.flake.Flake
           org.apache.lucene.analysis.en.EnglishAnalyzer
           org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer
           org.apache.lucene.analysis.hi.HindiAnalyzer
           org.apache.lucene.analysis.es.SpanishAnalyzer
           org.apache.lucene.analysis.ar.ArabicAnalyzer
           org.apache.lucene.analysis.id.IndonesianAnalyzer
           org.apache.lucene.analysis.ru.RussianAnalyzer
           org.apache.lucene.analysis.bn.BengaliAnalyzer
           org.apache.lucene.analysis.br.BrazilianAnalyzer
           org.apache.lucene.analysis.fr.FrenchAnalyzer
           org.apache.lucene.index.IndexWriter))

(defn predicate?
  [^Flake f]
  (= const/$_predicate:fullText
     (.-p f)))
