(ns fluree.db.full-text
  (:require [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clucie.analysis :as lucene-analysis]
            [clucie.core :as lucene]
            [clucie.store :as lucene-store])
  (:import fluree.db.flake.Flake))

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
  (let [anlz (analyzer lang)]
    (lucene-store/store-writer idx-store anlz)))

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
  [idx-reader anlz subj]
  (let [subj-id  (str subj)]
    (-> idx-reader
        (lucene/search {:_id subj-id} 1 anlz 0 1)
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
    (let [anlz (.getAnalyzer idx-writer)]
      (if-let [subj-map (get-subject idx-reader anlz subj)]
        (update-subject idx-writer subj-map pred-vals)
        (add-subject idx-writer subj pred-vals)))))

(defn purge-subject
  [idx-writer subj pred-vals]
  (with-open [idx-reader (writer->reader idx-writer)]
    (let [anlz (.getAnalyzer idx-writer)]
      (when-let [{id :_id, :as subj-map} (get-subject idx-reader anlz subj)]
        (let [purge-map (->> subj-map
                             (filter (fn [[k v]]
                                      (or (#{:_id :_collection} k)
                                          (not (contains? pred-vals k))
                                          (not (= v (get pred-vals k))))))
                             (into {}))
              map-keys  (keys purge-map)]
          (lucene/update! idx-writer purge-map map-keys :_id id))))))

(defn block-registry-path
  [base-path [network db-id]]
  (let [parent (storage-path base-path [network dbid])]
    (str/join "/" [store "block_registry.edn"])))

(defn read-block-registry
  [base-path [network db-id]]
  (let [registry-file (-> base-path
                          (block-registry-path [network db-id])
                          io/as-file)]
    (if (.exists registry-file)
      (-> registry-file slurp edn/read-string))))

(defn register-block
  [base-path [network dbid] block]
  (let [registry (-> block :block prn-str)]
    (-> base-path
        (block-registry-path [network dbid])
        (spit registry))))
