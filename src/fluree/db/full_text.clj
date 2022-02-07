(ns fluree.db.full-text
  (:require [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.full-text.block-registry :as block-registry]
            [clojure.string :as str]
            [clojure.walk :refer [keywordize-keys]]
            [clucie.analysis :as lucene-analysis]
            [clucie.core :as lucene]
            [clucie.store :as lucene-store])
  (:import (java.io Closeable)
           (org.apache.lucene.analysis Analyzer)
           (org.apache.lucene.analysis.en EnglishAnalyzer)
           (org.apache.lucene.analysis.cn.smart SmartChineseAnalyzer)
           (org.apache.lucene.analysis.hi HindiAnalyzer)
           (org.apache.lucene.analysis.es SpanishAnalyzer)
           (org.apache.lucene.analysis.ar ArabicAnalyzer)
           (org.apache.lucene.analysis.id IndonesianAnalyzer)
           (org.apache.lucene.analysis.ru RussianAnalyzer)
           (org.apache.lucene.analysis.bn BengaliAnalyzer)
           (org.apache.lucene.analysis.br BrazilianAnalyzer)
           (org.apache.lucene.analysis.fr FrenchAnalyzer)
           (org.apache.lucene.index IndexWriter)
           (org.apache.lucene.store Directory)))

(set! *warn-on-reflection* true)

(def search-limit Integer/MAX_VALUE)

(defrecord Index [^Directory storage ^Analyzer analyzer block-registry]
  Closeable
  (close [_]
    (.close analyzer)
    (.close storage)))

;; TODO: determine size impact of these analyzers - can we package them
;;       separately if large impact?
(defn lang->analyzer
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

(defn base-storage-path
  [network dbid path]
  (str/join "/" [path network dbid "full_text"]))

(defn subject-storage-path
  [base-path]
  (str/join "/" [base-path "lucene"]))

(defn disk-index
  [base-path network dbid lang]
  (let [storage-path  (base-storage-path network dbid base-path)
        subject-store (-> storage-path
                          subject-storage-path
                          lucene-store/disk-store)
        registry      (block-registry/disk-registry storage-path)
        analyzer      (lang->analyzer lang)]
    (->Index subject-store analyzer registry)))

(defn memory-index ^Index
  [lang]
  (let [subject-store (lucene-store/memory-store)
        analyzer      (lang->analyzer lang)
        registry      (block-registry/memory-registry)]
    (->Index subject-store analyzer registry)))

(defprotocol IndexConnection
  (open-storage [conn network dbid lang]))

(defn predicate?
  [f]
  (= const/$_predicate:fullText
     (flake/p f)))

(defn full-text-predicates
  [db coll-name]
  (->> db
       :schema
       :pred
       vals
       (filter (fn [pred]
                 (and (:fullText pred)
                      (str/starts-with? (:name pred)
                                        (str coll-name "/")))))
       (map :id)))

(defn sanitize
  [pred-map]
  (reduce-kv (fn [m k v]
               (let [k* (-> k str keyword)]
                 (assoc m k* v)))
             {} pred-map))

(defn writer ^IndexWriter
  [{:keys [storage analyzer]}]
  (lucene-store/store-writer storage analyzer))

(defn reader
  [{:keys [storage]}]
  (lucene-store/store-reader storage))

(defn get-subject
  [{:keys [analyzer] :as idx} subj-id]
  (let [subj-id  (str subj-id)]
    (with-open [^Closeable rdr (reader idx)]
      (-> rdr
          (lucene/search {:_id subj-id} 1 analyzer 0 1)
          first))))

(defn put-subject
  [idx wrtr subj pred-vals]
  (let [prev-subj (or (get-subject idx subj)
                      {:_id         (str subj)
                       :_collection (-> subj flake/sid->cid str)})
        updates   (sanitize pred-vals)
        subj-map  (merge prev-subj updates)
        map-keys  (keys subj-map)]
    (lucene/update! wrtr subj-map map-keys :_id subj)))

(defn purge-subject
  [idx wrtr subj pred-vals]
  (when-let [{id :_id, :as subj-map} (get-subject idx subj)]
    (let [attrs     (sanitize pred-vals)
          purge-map (->> subj-map
                         (filter (fn [[k v]]
                                   (or (#{:_id :_collection} k)
                                       (not (contains? attrs k))
                                       (not (= v (get attrs k))))))
                         (into {}))
          map-keys  (keys purge-map)]
      (lucene/update! wrtr purge-map map-keys :_id id))))

(defn register-block
  [{:keys [block-registry]} _wrtr block-status]
  (block-registry/register block-registry block-status))

(defn read-block-registry
  [{:keys [block-registry]}]
  (block-registry/read block-registry))

(defn forget
  [{:keys [block-registry]} ^IndexWriter wrtr]
  (doto wrtr .deleteAll .commit)
  (block-registry/reset block-registry))

(defn parse-domain
  [search]
  (-> search
      (str/split #"^fullText:")
      second))

(defn predicate-domain?
  [domain]
  (str/includes? domain "/"))

(defn build-predicate-query
  [db pred param]
  (let [pid (dbproto/-p-prop db :id pred)]
    {pid param}))

(defn build-collection-query
  [db coll param]
  (let [cid    (dbproto/-c-prop db :id coll)
        params (->> (full-text-predicates db cid)
                    (map (fn [pid]
                           {pid param}))
                    (into #{}))]
    [{:_collection cid} params]))

(defn build-query
  [db domain param]
  (if (predicate-domain? domain)
    (build-predicate-query db domain param)
    (build-collection-query db domain param)))

(defn wildcard?
  [param]
  (or (str/includes? param "*")
      (str/includes? param "?")))

(defn search
  [{:keys [storage analyzer]} db [var search param]]
  (let [domain (parse-domain search)
        query  (build-query db domain param)
        res    (if (wildcard? param)
                 (lucene/wildcard-search storage query search-limit analyzer 0 search-limit)
                 (lucene/search storage query search-limit analyzer 0 search-limit))
        tuples (map #(->> % :_id read-string vector) res)]
    {:headers [var]
     :tuples  tuples
     :vars    {}}))
