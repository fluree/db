(ns fluree.indexer.api-test
  (:require
   [clojure.test :as test :refer :all]
   [fluree.indexer.api :as idxr]
   [fluree.store.api :as store]
   [clojure.core.async :as async]
   [fluree.common.iri :as iri]
   [fluree.common.model :as model]))

(deftest indexer
  (let [idxr (idxr/start {:idxr/store-config {:store/method :memory}})

        db0-address (idxr/init idxr "indexertest" {:reindex-min-bytes 1})

        ;; two different stages onto the same db
        db1-summary           (idxr/stage idxr db0-address
                                          {"@context" {"me" "http://dan.com/"}
                                           "@id"      "me:dan"
                                           "me:prop1" "bar"})
        db2-summary           (idxr/stage idxr (get db1-summary iri/DbBlockAddress)
                                          {"@context" {"me" "http://dan.com/"}
                                           "@id"      "me:dan"
                                           "me:prop2" "foo"})
        sibling-stage-summary (idxr/stage idxr db0-address
                                          {"@context" {"me" "http://dan.com/"}
                                           "@id"      "me:dan"
                                           "me:prop1" "DIFFERENT BRANCH"})
        db0-results           (idxr/query idxr db0-address {:select ["?s" "?p" "?o"] :where [["?s" "?p" "?o"]]})
        db1-results           (idxr/query idxr (get db1-summary iri/DbBlockAddress)
                                          {:select {"?s" [:*]} :where [["?s" "@id" "http://dan.com/dan"]]})
        db2-results           (idxr/query idxr (get db2-summary iri/DbBlockAddress)
                                          {:select {"?s" [:*]} :where [["?s" "@id" "http://dan.com/dan"]]})
        sibling-stage-results (idxr/query idxr (get sibling-stage-summary iri/DbBlockAddress)
                                          {:select {"?s" [:*]} :where [["?s" "@id" "http://dan.com/dan"]]})]

    (testing "initial db"
      (is (= "fluree:db:memory:indexertest/db/init"
             db0-address))
      (is (= [] db0-results)))
    (testing "consecutive stages"
      (is (= {"https://ns.flur.ee/DbBlock#reindexMin" 1,
              "https://ns.flur.ee/DbBlock#address"
              "fluree:db:memory:indexertest/db/31af39137b85dd4273becc7edde9bc9bd42b0670cd68a2b1c5d7ad0916640155",
              "https://ns.flur.ee/DbBlock#reindexMax" 1000000,
              "https://ns.flur.ee/DbBlock#size" 828,
              "https://ns.flur.ee/DbBlock#v" 0,
              "@type" "https://ns.flur.ee/DbBlockSummary/",
              "https://ns.flur.ee/DbBlock#t" 1}
             db1-summary))
      (is (model/valid? idxr/DbBlockSummary db1-summary))

      (is (= {"https://ns.flur.ee/DbBlock#reindexMin" 1,
              "https://ns.flur.ee/DbBlock#address"
              "fluree:db:memory:indexertest/db/3abc11deba4a312f64cd1ef35bd0a7fefc520bb400137a4aebe8b7ae0d27dc28",
              "https://ns.flur.ee/DbBlock#reindexMax" 1000000,
              "https://ns.flur.ee/DbBlock#size" 958,
              "https://ns.flur.ee/DbBlock#v" 0,
              "@type" "https://ns.flur.ee/DbBlockSummary/",
              "https://ns.flur.ee/DbBlock#t" 2}
             db2-summary))
      (is (model/valid? idxr/DbBlockSummary db2-summary))

      (is (= [{"@id"                  "http://dan.com/dan"
               "http://dan.com/prop1" "bar"
               "http://dan.com/prop2" "foo"}]
             db2-results)))
    (testing "two sibling stages"
      (is (not= (get db1-summary iri/DbBlockAddress)
                (get sibling-stage-summary iri/DbBlockAddress)))

      (is (= [{"@id" "http://dan.com/dan" "http://dan.com/prop1" "bar"}]
             db1-results))
      (is (= [{"@id" "http://dan.com/dan" "http://dan.com/prop1" "DIFFERENT BRANCH"}]
             sibling-stage-results)))

    (testing "indexer persistence"
      (let [store (:store idxr)

            idxr2 (idxr/start {:idxr/store store})

            loaded-summary (idxr/load idxr2 (get db2-summary iri/DbBlockAddress))
            loaded-results (idxr/query idxr (get db2-summary iri/DbBlockAddress)
                                       {:select {"?s" [:*]} :where [["?s" "@id" "http://dan.com/dan"]]})]
        (is (= ["indexertest/db/31af39137b85dd4273becc7edde9bc9bd42b0670cd68a2b1c5d7ad0916640155"
                "indexertest/db/3abc11deba4a312f64cd1ef35bd0a7fefc520bb400137a4aebe8b7ae0d27dc28"
                "indexertest/db/dd84b006d788b1169dce5dc4597bdab463eb33e816acdd3a3ad1a0b356f1a271"]
               (sort (async/<!! (store/list store "indexertest/db")))))

        (is (= [true true true]
               (map (fn [block-path] (model/valid? idxr/DbBlock (async/<!! (store/read store block-path))))
                    ["indexertest/db/31af39137b85dd4273becc7edde9bc9bd42b0670cd68a2b1c5d7ad0916640155"
                     "indexertest/db/3abc11deba4a312f64cd1ef35bd0a7fefc520bb400137a4aebe8b7ae0d27dc28"
                     "indexertest/db/dd84b006d788b1169dce5dc4597bdab463eb33e816acdd3a3ad1a0b356f1a271"])))
        ;; index keys are nondeterministic, so can only assert count
        (is (= 26
               (count (async/<!! (store/list store "indexertest/index")))))
        ;; TODO: merge-flakes counts the db stats differently than final-db
        #_(is (= db2-summary
                 loaded-summary))
        ;; query results are the same
        (is (= db2-results
               loaded-results))))))
