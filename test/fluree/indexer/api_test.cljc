(ns fluree.indexer.api-test
  (:require
   [clojure.test :as test :refer :all]
   [fluree.indexer.api :as idxr]
   [fluree.store.api :as store]
   [clojure.core.async :as async]
   [fluree.common.iri :as iri]
   [fluree.common.model :as model]
   [fluree.db.did :as did]
   [fluree.db.test-utils :as test-utils]))

(deftest indexer
  (let [idxr (idxr/start {:idxr/store-config {:store/method :memory}
                          :idxr/did          (did/private->did-map test-utils/default-private-key)
                          :idxr/trust        :all})

        db0-address (idxr/init idxr "indexertest" {:reindex-min-bytes 1})

        ;; two different stages onto the same db
        db1-summary           (idxr/stage idxr db0-address
                                          {"@context" {"me" "http://dan.com/"}
                                           "@id"      "me:dan"
                                           "me:prop1" "bar"}
                                          {:tx-id "TX-ID1"})
        db2-summary           (idxr/stage idxr (get db1-summary iri/DbBlockAddress)
                                          {"@context" {"me" "http://dan.com/"}
                                           "@id"      "me:dan"
                                           "me:prop2" "foo"}
                                          {:tx-id "TX-ID2"})
        sibling-stage-summary (idxr/stage idxr db0-address
                                          {"@context" {"me" "http://dan.com/"}
                                           "@id"      "me:dan"
                                           "me:prop1" "DIFFERENT BRANCH"}
                                          {:tx-id "TX-IDOTHER"})
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
              "fluree:db:memory:indexertest/db/1ed976882c0db213d475590378758a490f631d2ed1d223c8540b0540689aa3bf",
              "https://ns.flur.ee/DbBlock#reindexMax" 1000000,
              "https://ns.flur.ee/DbBlock#size" 828,
              "https://ns.flur.ee/DbBlock#v" 0,
              "https://ns.flur.ee/DbBlock#txId" "TX-ID1",
              "@type" "https://ns.flur.ee/DbBlockSummary/",
              "https://ns.flur.ee/DbBlock#t" 1}
             db1-summary))
      (is (model/valid? idxr/DbBlockSummary db1-summary))

      (is (= {"https://ns.flur.ee/DbBlock#reindexMin" 1,
              "https://ns.flur.ee/DbBlock#address"
              "fluree:db:memory:indexertest/db/545b2c139d73fd08fc3b74a0a3cdf93ab796cd761c2e39c2a0cb40b01345780d",
              "https://ns.flur.ee/DbBlock#reindexMax" 1000000,
              "https://ns.flur.ee/DbBlock#size" 958,
              "https://ns.flur.ee/DbBlock#v" 0,
              "https://ns.flur.ee/DbBlock#txId" "TX-ID2",
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

            idxr2 (idxr/start {:idxr/store store
                               :idxr/did   (did/private->did-map test-utils/default-private-key)
                               :idxr/trust :all})

            loaded-summary (idxr/load idxr2 (get db2-summary iri/DbBlockAddress))
            loaded-results (idxr/query idxr (get db2-summary iri/DbBlockAddress)
                                       {:select {"?s" [:*]} :where [["?s" "@id" "http://dan.com/dan"]]})]
        (is (= ["indexertest/db/1ed976882c0db213d475590378758a490f631d2ed1d223c8540b0540689aa3bf"
                "indexertest/db/545b2c139d73fd08fc3b74a0a3cdf93ab796cd761c2e39c2a0cb40b01345780d"
                "indexertest/db/6c3954582f8699a0c7e7a424b32292b7de3799ef7db1a0bf0ba3e3829ff5543f"]
               (sort (async/<!! (store/list store "indexertest/db")))))

        (is (= [true true true]
               (map (fn [block-path] (model/valid? idxr/DbBlock (async/<!! (store/read store block-path))))
                    ["indexertest/db/1ed976882c0db213d475590378758a490f631d2ed1d223c8540b0540689aa3bf"
                     "indexertest/db/545b2c139d73fd08fc3b74a0a3cdf93ab796cd761c2e39c2a0cb40b01345780d"
                     "indexertest/db/6c3954582f8699a0c7e7a424b32292b7de3799ef7db1a0bf0ba3e3829ff5543f"])))
        ;; index keys are nondeterministic, so can only assert count
        (is (= 26
               (count (async/<!! (store/list store "indexertest/index")))))
        ;; TODO: merge-flakes counts the db stats differently than final-db
        #_(is (= db2-summary
                 loaded-summary))
        ;; query results are the same
        (is (= db2-results
               loaded-results))))))
