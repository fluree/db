(ns fluree.db.full-text
  (:require [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
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
  (let [anyz (analyzer lang)]
    (lucene-store/store-writer idx-store anyz)))

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
  [idx-reader anyz subj]
  (let [subj-id  (str subj)]
    (-> idx-reader
        (lucene/search {:_id subj-id} 1 anyz 0 1)
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
    (let [anyz (.getAnalyzer idx-writer)]
      (if-let [subj-map (get-subject idx-reader anyz subj)]
        (update-subject idx-writer subj-map pred-vals)
        (add-subject idx-writer subj pred-vals)))))

(defn purge-subject
  [idx-writer subj pred-vals]
  (with-open [idx-reader (writer->reader idx-writer)]
    (let [anyz (.getAnalyzer idx-writer)]
      (when-let [{id :_id, :as subj-map} (get-subject idx-reader anyz subj)]
        (let [purge-map (->> subj-map
                             (filter (fn [[k v]]
                                      (or (#{:_id :_collection} k)
                                          (not (contains? pred-vals k))
                                          (not (= v (get pred-vals k))))))
                             (into {}))
              map-keys  (keys purge-map)]
          (lucene/update! idx-writer purge-map map-keys :_id id))))))


#_(track-full-text storage-dir (str network "/" dbid) (:block block))

(defn track-full-text
  [dir ledger block]
  (with-open [w (clojure.java.io/writer (str dir ledger "/lucene/block.txt") :append false)]
    (.write w (str block))))
