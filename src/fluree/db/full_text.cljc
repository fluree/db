(ns fluree.db.full-text
  (:require [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [clojure.string :as str]
            [clucie.analysis :as lucene-analysis]
            [clucie.core :as lucene]
            [clucie.store :as lucene-store])
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

(defn storage-path
  [base-path [network dbid]]
  (str/join "/" [base-path network dbid "lucene"]))

(defn storage
  [base-path [network dbid]]
  (let [path (storage-path base-path [network dbid])]
    (lucene-store/disk-store path)))

;; TODO: determine size impact of these analyzers - can we package them
;;       separately if large impact?
(defn analyzer
  "Analyzers for the top ten most spoken languages in the world, along with the
  standard analyzer for all others.
  https://en.wikipedia.org/wiki/List_of_languages_by_total_number_of_speakers"
  [lang]
  (case lang
    :ar (ArabicAnalyzer.)
    :bn (BengaliAnalyzer.)
    :br (BrazilianAnalyzer.)
    :cn (SmartChineseAnalyzer.)
    :en (EnglishAnalyzer.)
    :es (SpanishAnalyzer.)
    :fr (FrenchAnalyzer.)
    :hi (HindiAnalyzer.)
    :id (IndonesianAnalyzer.)
    :ru (RussianAnalyzer.)
    (lucene-analysis/standard-analyzer)))

(defn writer
  [idx-store lang]
  (lucene-store/store-writer idx-store lang))

(defn reader
  [idx-store]
  (lucene-store/store-reader idx-store))

(defn writer->reader
  [^IndexWriter w]
  (-> w .getDirectory reader))

(defn add-subject
  [idx-writer subj pred-vals]
  (let [subj-id  (str subj)
        cid      (flake/sid->cid subj)
        subj-map (merge {:_id subj-id, :_collection cid}
                        pred-vals)
        map-keys (keys subj-map)]
    (lucene/add! idx-writer [subj-map] map-keys)))

(defn get-subject
  [idx-reader lang subj]
  (let [subj-id  (str subj)]
    (-> idx-reader
        (lucene/search {:_id subj-id} 1 analyzer 0 1)
        first)))

(defn update-subject
  [idx-writer subj-map pred-vals]
  (when-let [{id :_id} subj-map]
    (let [new-map  (merge subj-map pred-vals)
          map-keys (keys new-map)]
      (lucene/update! idx-writer new-map map-keys :_id id))))

(defn put-subject
  [^IndexWriter idx-writer subj pred-vals]
  (with-open [idx-reader (writer->reader idx-writer)]
    (let [lang (.getAnalyzer idx-writer)]
      (if-let [subj-map (get-subject idx-reader lang subj)]
        (update-subject idx-writer subj-map pred-vals)
        (add-subject idx-writer subj pred-vals)))))

(defn purge-subject
  [idx-writer lang subj pred-vals]
  (with-open [idx-reader (writer->reader idx-writer)]
    (let [lang (.getAnalyzer idx-writer)]
      (when-let [{id :_id, :as subj-map} (get-subject idx-reader lang subj)]
        (let [purge-map (->> subj-map
                             (filter (fn [[k v]]
                                      (or (#{:_id :_collection} k)
                                          (not (contains? pred-vals k))
                                          (not (= v (get pred-vals k))))))
                             (into {}))
              map-keys  (keys purge-map)]
          (lucene/update! idx-writer purge-map map-keys :_id id))))))
