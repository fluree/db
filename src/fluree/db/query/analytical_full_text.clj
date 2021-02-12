(ns fluree.db.query.analytical-full-text
  (:require [clucie.core :as clucie]
            [clucie.analysis :as analysis]
            [clucie.store :as store]
            [clojure.string :as str]
            [fluree.db.flake :as flake]
            [fluree.db.util.log :as log]
            [fluree.db.dbproto :as dbproto])
  (:import (org.apache.lucene.analysis.en EnglishAnalyzer)
           (org.apache.lucene.analysis.cn.smart SmartChineseAnalyzer)
           (org.apache.lucene.analysis.hi HindiAnalyzer)
           (org.apache.lucene.analysis.es SpanishAnalyzer)
           (org.apache.lucene.analysis.ar ArabicAnalyzer)
           (org.apache.lucene.analysis.id IndonesianAnalyzer)
           (org.apache.lucene.analysis.ru RussianAnalyzer)
           (org.apache.lucene.analysis.bn BengaliAnalyzer)
           (org.apache.lucene.analysis.br BrazilianAnalyzer)
           (org.apache.lucene.analysis.fr FrenchAnalyzer)))


;; I chose the top ten most spoken languages in the world
;; https://en.wikipedia.org/wiki/List_of_languages_by_total_number_of_speakers
;; TODO - determine size impact of these analyzers - can we package them separately if large impact?

(defn analyzer
  [language]
  (condp = language :ar (ArabicAnalyzer.)

                    :bn (BengaliAnalyzer.)

                    :br (BrazilianAnalyzer.)

                    :cn (SmartChineseAnalyzer.)

                    :en (EnglishAnalyzer.)

                    :es (SpanishAnalyzer.)

                    :fr (FrenchAnalyzer.)

                    :hi (HindiAnalyzer.)

                    :id (IndonesianAnalyzer.)

                    :ru (RussianAnalyzer.)

                    (analysis/standard-analyzer)))

(defn index-store
  [path-to-dir network dbid]
  (store/disk-store (str path-to-dir network "/" dbid "/lucene")))

(defn collection-predicates-full-text
  [db collection-name]
  (->> (filter #(and (:fullText %)
                     (str/starts-with? (:name %) collection-name))
               (vals (-> db :schema :pred)))
       (map :id)))

(defn search
  [db store clause language]
  (let [[var search search-param] clause
        limit  Integer/MAX_VALUE
        search (-> (str/split search #"^fullText:")
                   second
                   (str/split #"/"))
        query  (if (= 2 (count search))
                 ;; This is a predicate-specific query, i.e. fullText:_user/username
                 (let [pid (-> (dbproto/-p-prop db :id (str/join "/" search))
                               str keyword)]
                   {pid search-param})

                 ;; This is a collection-based query, i.e. fullText:_user
                 (let [partition     (dbproto/-c-prop db :partition (first search))
                       fullTextPreds (collection-predicates-full-text db (first search))
                       search-params (->> (map #(assoc {} (-> % str keyword) search-param)
                                               fullTextPreds) (into #{}))]
                   [{:collection (str partition)} search-params]))
        res    (clucie/search store query limit (analyzer language) 0 limit)]
    {:headers [var]
     :tuples  (map #(->> % :_id read-string (conj [])) res)
     :vars    {}}))




(comment

  (map #(->> % :_id read-string (into []))
       [{:1000 "jdoe", :1001 "Jane Doe", :_id "351843720888321", :collection "20"}])

  (def db (clojure.core.async/<!! (fluree.db.api/db (:conn user/system) "fluree/test")))

  db




  (vals (-> db :schema :pred))


  (str/starts-with? "str" "s")

  (dbproto/-p-prop db :id "_user/username")

  (def store (index-store "data/ledger/" "fluree" "test"))


  (str/join "/" ["_user" "username"])

  (clucie/search store
                 [{:collection "20"}
                  #{{:1000 "jdoe"} {:1001 "jdoe"}}

                  ]

                 10000 (analyzer) 0 10000)

  ;; If we add a predicate to fullText search
  ;; - if members of that collection are already in FT
  ;; If we remove a predicate from fullText search
  ;; - if members of that collection are still in FT

  )

