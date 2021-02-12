(ns fluree.db.full-text
  (:require [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clucie.analysis :as lucene-analysis]
            [clucie.core :as lucene]
            [clucie.store :as lucene-store])
  (:import fluree.db.flake.Flake
           java.io.File
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

(defn full-text-predicates
  [db collection]
  (->> db
       :schema
       :pred
       vals
       (filter (fn [pred]
                 (and (:fullText pred)
                      (str/starts-with? (:name pred)
                                        collection))))
       (map :id)))

(defn storage-path
  [base-path {:keys [network dbid] :as db}]
  (str/join "/" [base-path network dbid "lucene"]))

(defn storage
  [path]
  (lucene-store/disk-store path))

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

(defn writer->storage-path
  [^IndexWriter w]
  (-> w .getDirectory .getDirectory .toString))

(defn get-subject
  [idx-reader anlz subj]
  (let [subj-id  (str subj)]
    (-> idx-reader
        (lucene/search {:_id subj-id} 1 anlz 0 1)
        first)))

(defn put-subject
  [idx-writer subj pred-vals]
  (let [subj-id  (str subj)
        cid      (-> subj flake/sid->cid str)
        subj-map (merge {:_id subj-id, :_collection cid}
                        pred-vals)
        map-keys (keys subj-map)]
    (lucene/update! idx-writer subj-map map-keys :_id subj)))

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

(defn block-registry-file
  [writer]
  (let [parent (writer->storage-path writer)
        path   (str/join "/" [parent "block_registry.edn"])]
    (io/as-file path)))

(defn read-block-registry
  [writer]
  (let [^File registry-file (block-registry-file writer)]
    (when (.exists registry-file)
      (-> registry-file slurp edn/read-string))))

(defn register-block
  [writer status]
  (let [registry-file (block-registry-file writer)
        registry      (prn-str status)]
    (spit registry-file registry)))

(defn forget-block-registry
  [writer]
  (let [^File registry-file (block-registry-file writer)]
    (when (.exists registry-file)
      (io/delete-file registry-file))))

(defn forget
  [^IndexWriter w]
  (doto w .deleteAll .commit)
  (forget-block-registry w))

(defn search
  [db store [var search search-param]]
  (let [lang   (-> db :settings :language)
        limit  Integer/MAX_VALUE
        search (-> search
                   (str/split #"^fullText:")
                   second)
        query  (if (str/includes? search "/")
                 ;; This is a predicate-specific query, i.e. fullText:_user/username
                 (let [pid  (dbproto/-p-prop db :id search)]
                   {pid search-param})

                 ;; This is a collection-based query, i.e. fullText:_user
                 (let [cid           (str (dbproto/-c-prop db :id search))
                       predicates    (full-text-predicates db search)
                       search-params (->> predicates
                                          (map (fn [p]
                                                 {p search-param}))
                                          (into #{}))]
                   [{:_collection cid} search-params]))
        res    (lucene/search store query limit (analyzer lang) 0 limit)]
    {:headers [var]
     :tuples  (map #(->> % :_id read-string (conj [])) res)
     :vars    {}}))
