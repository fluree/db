(ns fluree.db.performance
  (:require [clojure.java.io :as io]
            [criterium.core :as criterium]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.log :as log :include-macros true]
            [clojure.string :as str]))

(set! *warn-on-reflection* true)

(def query-coll
  [{:select ["*"] :from "_collection"}
   {:select ["*"] :from 369435906932737}
   {:select ["*"] :from ["person/handle" "jdoe"]}
   {:select ["*"] :from [369435906932737, ["person/handle", "jdoe"],
                         387028092977153, ["person/handle", "zsmith"]]}
   {:select ["*"] :from "person/handle"}
   {:select ["chat/message" "chat/person"] :from "chat" :limit 100}
   {:select ["chat/message" "chat/instant"] :where "chat/instant > 1517437000000"}
   {:select ["*"] :from "chat" :block 2}
   {:select ["*"] :from "chat" :block "PT5M"}
   {:select ["*"] :from "chat" :limit 100}
   {:select ["*"] :from "chat" :offset 1}
   {:select ["*"] :from "chat" :limit 10 :offset 1}
   {:select ["*" {"chat/person" ["*"]}] :from "chat"}
   {:select ["*" {"chat/_person" ["*"]}] :from "person"}
   {:select ["*" {"chat/_person" ["*" {"chat/person" ["*"]}]}] :from "person"}
   {:select ["handle" "fullName"] :from "person"}
   {:select ["handle" {"fullName" [{:_as "name"}]}] :from "person"}
   {:select ["handle" {"comment/_person" ["*" {:_as "comment" :_limit 1}]}] :from "person"}
   {:select ["handle" {"person/follows" ["handle"]}] :from "person"}
   {:select ["handle" {"person/follows" ["handle" {:_recur 10}]}] :from "person"}])

(def analytical-query-coll
  [{:select "?nums" :where [["$fdb" ["person/handle" "zsmith"] "person/favNums" "?nums"]]}
   {:select "?nums" :where [["$fdb" nil "person/favNums" "?nums"]]}
   {:select "?nums" :where [["$fdb" ["person/handle" "zsmith"] "person/favNums" "?nums"]
                            ["$fdb" ["person/handle" "jdoe"] "person/favNums" "?nums"]]}
   {:select ["?nums1" "?nums2"]
    :where  [["$fdb" ["person/handle" "zsmith"] "person/favNums" "?nums1"]
             ["$fdb" ["person/handle" "jdoe"] "person/favNums" "?nums2"]]}
   {:select "(sum ?nums)"
    :where  [[["person/handle" "zsmith"] "person/favNums" "?nums"]]}
   {:select "(sample 10 ?nums)"
    :where  [[nil "person/favNums" "?nums"]]}
   {:select {"?artist" ["*" {"person/_favArtists" ["*"]}]}
    :where  [[nil "person/favArtists" "?artist"]]}
   {:select ["?name", "?artist", "?artwork", "?artworkLabel"]
    :where  [[["person/handle", "jdoe"], "person/favArtists", "?artist"],
             ["?artist", "artist/name", "?name"],
             ["$wd", "?artwork", "wdt:P170", "?creator"],
             ["$wd", "?creator", "?label", "?name"]]}
   {:select ["?handle", "?title", "?narrative_locationLabel"],
    :where  [["?user", "person/favMovies", "?movie"],
             ["?movie", "movie/title", "?title"],
             ["$wd", "?wdMovie", "?label", "?title"],
             ["$wd", "?wdMovie", "wdt:P840", "?narrative_location"],
             ["$wd", "?wdMovie", "wdt:P31", "wd:Q11424"],
             ["?user", "person/handle", "?handle"]]}
   {:select ["?name", "?artist", "?artwork"],
    :where  [[["person/handle", "jdoe"], "person/favArtists", "?artist"],
             ["?artist", "artist/name", "?name"],
             ["$wd", "?artwork", "wdt:P170", "?creator", {:limit 5, :distinct false}],
             ["$wd", "?creator", "?label", "?name"]]}])

(def block-query-coll
  [{:block 3}
   {:block "PT1M"}
   {:block [3 5]}
   {:block [3]}
   {:block [3] :pretty-print true}])

(def history-query-coll
  [{:history 369435906932737 :block 4}
   {:history ["person/handle" "zsmith"] :block 4}
   {:history [["person/handle" "zsmith"] "person/follows"]}
   {:history [nil "person/handle" "jdoe"]}
   {:history [nil "person/handle" "jdoe"] :pretty-print true}])


(def graphql-query-coll
  [{:query "{ graph {\n  person {\n    _id\n    handle\n    chat_Via_person (limit: 10) {\n      instant\n      message\n      comments {\n        message\n      }\n    }\n  }\n}}"}
   {:query "{ graph {\n  chat {\n    _id\n    message\n  }\n}\n}"}
   {:query "{ graph {\n  chat {\n    _id\n    message\n    person {\n        _id\n        handle\n    }\n  }\n}\n}"}
   {:query "{ graph {\n  person {\n    *\n  }\n}\n}"}
   {:query "{ graph {\n  person {\n    chat_Via_person {\n      _id\n      instant\n      message\n    }\n  }\n}\n}"}
   {:query "query  {\n  block(from: 3, to: 3)\n}"}
   {:query "query  {\n  block(from: 3, to: 5)\n}"}
   {:query "query  {\n  block(from: 3)\n}"}])

(def multi-query-coll
  [{:chatQuery {:select ["*"] :from "chat"} :personQuery {:select ["*"] :from "person"}}])

(def sparql-query-coll
  ["SELECT ?person\nWHERE {\n  ?person fd:person/handle \"jdoe\".\n  ?person fd:person/fullName ?fullName.\n}"
   "SELECT ?person ?fullName ?favNums\nWHERE {\n  ?person fd:person/handle \"jdoe\";\n          fd:person/fullName ?fullName;\n          fd:person/favNums  ?favNums.\n}"
   "SELECT ?person\nWHERE {\n  ?person fd:person/handle \"jdoe\", \"zsmith\".\n}"
   "SELECT ?person ?fullName ?favNums\nWHERE {\n  ?person fd:person/fullName ?fullName;\n          fd:person/favNums  ?favNums;\n          fd:person/handle \"jdoe\", \"zsmith\".\n}"
   "SELECT DISTINCT ?horse ?horseLabel ?mother \n{\n    ?horse wdt:P31/wdt:P279* wd:Q726 .    \n    OPTIONAL{?horse wdt:P25 ?mother .}\n}"])

#_(defn add-and-delete-data
  [conn ledger-id]
  (let [txn       [{:_id "person" :favNums [1]}]
        res       (fdb/transact conn ledger-id txn)
        _id       (-> res :tempids (get "person$1"))
        deleteTxn [{:_id _id :_action "delete"}]
        deleteRes (fdb/transact conn ledger-id deleteTxn)]
    deleteRes))

#_(defn add-and-update-data
  [conn ledger-id]
  (let [txn       [{:_id "person" :favNums [1]}]
        res       (fdb/transact conn ledger-id txn)
        _id       (-> res :tempids (get "person$1"))
        updateTxn [{:_id _id :favNums [2]}]
        updateRes (fdb/transact conn ledger-id updateTxn)]
    updateRes))

#_(defn time-return-data
  [f & args]
  (let [start-time (System/nanoTime)
        _          (apply f args)
        end-time   (System/nanoTime)]
    (float (/ (- end-time start-time) 1000000))))

#_(defn format-res
  [q res]
  (let [mean (-> res :mean first)
        [scale unit] (criterium/scale-time mean)]
    {:issued    q
     :sample    (-> res :sample-count)
     :mean      mean
     :mean-time (criterium/format-value mean scale unit)}))

#_(defn test-queries
  [db f coll]
  (map (fn [q]
         (try (let [res (criterium/benchmark (f db q) nil)]
                (format-res q res))
              (catch Exception e {:issued q :error true})))
       coll))

#_(defn add-schema-performance-check
  [conn ledger-id]
  (let [collections (-> "basicschema/collections.edn" io/resource slurp read-string)
        coll-txn    (time-return-data fdb/transact conn ledger-id collections)
        predicates  (-> "basicschema/predicates.edn" io/resource slurp read-string)
        pred-txn    (time-return-data fdb/transact conn ledger-id predicates)
        data        (-> "basicschema/data.edn" io/resource slurp read-string)
        data-txn    (time-return-data fdb/transact conn ledger-id data)]
    (concat [coll-txn] pred-txn data-txn)))

#_(defn performance-check
  "NOTE: This performance check will take more than an hour."
  [conn ledger-id]
  (let [myDb                   (fdb/db conn ledger-id)
        query-bench            (test-queries myDb fdb/query query-coll)
        _                      (log/info "Query bench results: " query-bench)
        analytical-query-bench (test-queries myDb fdb/query analytical-query-coll)
        _                      (log/info "Analytical query bench results: " analytical-query-bench)
        history-query-bench    (test-queries myDb fdb/history-query-async history-query-coll)
        _                      (log/info "History query bench results" history-query-bench)
        sparql-query-bench     (test-queries myDb fdb/sparql-async sparql-query-coll)
        _                      (log/info "SPARQL query bench results: " sparql-query-bench)
        graphql-query-bench    (test-queries myDb (fn [db q] (fdb/graphql-async conn ledger-id q)) graphql-query-coll)
        _                      (log/info "GraphQL query bench results:" graphql-query-bench)
        multi-query-bench      (test-queries myDb fdb/multi-query-async multi-query-coll)
        _                      (log/info "Multi-query bench results: " multi-query-bench)
        add-data-bench         (->> (criterium/benchmark (fdb/transact conn ledger-id [{:_id "person" :favNums [1]}]) nil)
                                    (format-res :addData))
        _                      (log/info "Add data bench: " add-data-bench)
        add-update-bench       (->> (criterium/benchmark (add-and-update-data conn ledger-id) nil)
                                    (format-res :addUpdateData))
        _                      (log/info "Add and update data bench: " add-update-bench)
        add-delete-bench       (->> (criterium/benchmark (add-and-delete-data conn ledger-id) nil)
                                    (format-res :addDeleteData))
        _                      (log/info "Add and delete data bench: " add-delete-bench)
        res                    (reduce (fn [acc res] (assoc acc (-> (:issued res)
                                                                    (str/replace #"\s+" " "))
                                                                (dissoc res :issued)))
                                       {} (concat query-bench analytical-query-bench
                                                  history-query-bench sparql-query-bench
                                                  graphql-query-bench multi-query-bench
                                                  add-data-bench add-update-bench
                                                  add-delete-bench))]
    res))

(defn compare-results
  ([res1 res2]
   (compare-results res1 res2 0.5))
  ([res1 res2 percentChange]
   (reduce (fn [acc [res2Key res2Val]]
             (if-let [res1Time (-> (get res1 res2Key) :mean)]
               (let [res2Time (-> res2Val :mean)
                     diff     (- res2Time res1Time)
                     percent  (/ diff res1Time)]
                 (if (>= percent (abs percentChange))
                   (let [[scale unit] (criterium/scale-time diff)
                         diff-formatted (criterium/format-value diff scale unit)
                         key            (if (neg? percentChange) :decreased :increased)]
                     (update acc key conj {:query          res2Key :oldTime res1Time :newTime res2Time
                                           :diff           diff :percentDiff percent
                                           :diff-formatted diff-formatted}))

                   acc))
               (update acc :no-match conj res2Key)))
           {} res2)))


(comment


  (def conn (:conn user/system))
  #_(def mydb (fdb/db conn "test/me"))


  (def collections (-> "basicschema/collections.edn" io/resource slurp read-string))
  (def predicates (-> "basicschema/predicates.edn" io/resource slurp read-string))
  (def data (-> "basicschema/data.edn" io/resource slurp read-string))

  #_(time-return-data fdb/transact conn "test/me" data)

  #_(fdb/history-query mydb)


  #_(test-queries mydb fdb/history-query history-query-coll)


  (def res1 (-> "performanceMetrics/performance.edn" io/resource slurp read-string))



  (def res2 (assoc res1 {:block 3} {:sample 60, :mean 2.2178411651301957E-4, :mean-time "221.784117 µs"}))

  (compare-results res1 res2))
