(ns fluree.db.query.history-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.core :as util]))

(deftest ^:integration history-query
  (let [ts-primeval (util/current-time-iso)

        conn        (test-utils/create-conn)
        ledger      @(fluree/create conn "historytest"
                                    {"defaults"
                                     {"@context"
                                      ["" {:ex "http://example.org/ns/"}]}})

        db1         @(test-utils/transact ledger [{"id"   "ex:dan"
                                                   "ex:x" "foo-1"
                                                   "ex:y" "bar-1"}
                                                  {"id"   "ex:cat"
                                                   "ex:x" "foo-1"
                                                   "ex:y" "bar-1"}
                                                  {"id"   "ex:dog"
                                                   "ex:x" "foo-1"
                                                   "ex:y" "bar-1"}])
        db2         @(test-utils/transact ledger {"id"   "ex:dan"
                                                  "ex:x" "foo-2"
                                                  "ex:y" "bar-2"})
        ts2         (-> db2 :commit :time)
        db3         @(test-utils/transact ledger {"id"   "ex:dan"
                                                  "ex:x" "foo-3"
                                                  "ex:y" "bar-3"})

        ts3         (-> db3 :commit :time)
        db4         @(test-utils/transact ledger [{"id"   "ex:cat"
                                                   "ex:x" "foo-cat"
                                                   "ex:y" "bar-cat"}
                                                  {"id"   "ex:dog"
                                                   "ex:x" "foo-dog"
                                                   "ex:y" "bar-dog"}])
        db5         @(test-utils/transact ledger {"id"   "ex:dan"
                                                  "ex:x" "foo-cat"
                                                  "ex:y" "bar-cat"})]
    (testing "subject history"
      (is (= [{"f:t"       1
               "f:assert"  [{"id" "ex:dan" "ex:x" "foo-1" "ex:y" "bar-1"}]
               "f:retract" []}
              {"f:t"       2
               "f:assert"  [{"id" "ex:dan" "ex:x" "foo-2" "ex:y" "bar-2"}]
               "f:retract" [{"id" "ex:dan" "ex:x" "foo-1" "ex:y" "bar-1"}]}
              {"f:t"       3
               "f:assert"  [{"id" "ex:dan" "ex:x" "foo-3" "ex:y" "bar-3"}]
               "f:retract" [{"id" "ex:dan" "ex:x" "foo-2" "ex:y" "bar-2"}]}
              {"f:t"       5
               "f:assert"  [{"id" "ex:dan" "ex:x" "foo-cat" "ex:y" "bar-cat"}]
               "f:retract" [{"id" "ex:dan" "ex:x" "foo-3" "ex:y" "bar-3"}]}]
             @(fluree/history ledger {"history" "ex:dan" "t" {"from" 1}}))))
    (testing "one-tuple flake history"
      (is (= [{"f:t"       1
               "f:assert"  [{"id" "ex:dan" "ex:x" "foo-1" "ex:y" "bar-1"}]
               "f:retract" []}
              {"f:t"       2
               "f:assert"  [{"ex:x" "foo-2" "ex:y" "bar-2" "id" "ex:dan"}]
               "f:retract" [{"ex:x" "foo-1" "ex:y" "bar-1" "id" "ex:dan"}]}
              {"f:t"       3
               "f:assert"  [{"ex:x" "foo-3" "ex:y" "bar-3" "id" "ex:dan"}]
               "f:retract" [{"ex:x" "foo-2" "ex:y" "bar-2" "id" "ex:dan"}]}
              {"f:t"       5
               "f:assert"  [{"ex:x" "foo-cat" "ex:y" "bar-cat" "id" "ex:dan"}]
               "f:retract" [{"ex:x" "foo-3" "ex:y" "bar-3" "id" "ex:dan"}]}]
             @(fluree/history ledger {"history" ["ex:dan"] "t" {"from" 1}}))))
    (testing "two-tuple flake history"
      (is (= [{"f:t" 1 "f:assert" [{"ex:x" "foo-1" "id" "ex:dan"}] "f:retract" []}
              {"f:t"       2
               "f:assert"  [{"ex:x" "foo-2" "id" "ex:dan"}]
               "f:retract" [{"ex:x" "foo-1" "id" "ex:dan"}]}
              {"f:t"       3
               "f:assert"  [{"ex:x" "foo-3" "id" "ex:dan"}]
               "f:retract" [{"ex:x" "foo-2" "id" "ex:dan"}]}
              {"f:t"       5
               "f:assert"  [{"ex:x" "foo-cat" "id" "ex:dan"}]
               "f:retract" [{"ex:x" "foo-3" "id" "ex:dan"}]}]
             @(fluree/history ledger {"history" ["ex:dan" "ex:x"] "t" {"from" 1}})))

      (is (= [{"f:t"       1 "f:assert" [{"ex:x" "foo-1" "id" "ex:dog"}
                                         {"ex:x" "foo-1" "id" "ex:cat"}
                                         {"ex:x" "foo-1" "id" "ex:dan"}]
               "f:retract" []}
              {"f:t"       2
               "f:assert"  [{"ex:x" "foo-2" "id" "ex:dan"}]
               "f:retract" [{"ex:x" "foo-1" "id" "ex:dan"}]}
              {"f:t"       3
               "f:assert"  [{"ex:x" "foo-3" "id" "ex:dan"}]
               "f:retract" [{"ex:x" "foo-2" "id" "ex:dan"}]}
              {"f:t"       4
               "f:assert"  [{"ex:x" "foo-dog" "id" "ex:dog"}
                            {"ex:x" "foo-cat" "id" "ex:cat"}]
               "f:retract" [{"ex:x" "foo-1" "id" "ex:dog"}
                            {"ex:x" "foo-1" "id" "ex:cat"}]}
              {"f:t"       5
               "f:assert"  [{"ex:x" "foo-cat" "id" "ex:dan"}]
               "f:retract" [{"ex:x" "foo-3" "id" "ex:dan"}]}]
             @(fluree/history ledger {"history" [nil "ex:x"] "t" {"from" 1}}))))
    (testing "three-tuple flake history"
      (is (= [{"f:t" 4 "f:assert" [{"ex:x" "foo-cat" "id" "ex:cat"}] "f:retract" []}
              {"f:t" 5 "f:assert" [{"ex:x" "foo-cat" "id" "ex:dan"}] "f:retract" []}]
             @(fluree/history ledger {"history" [nil "ex:x" "foo-cat"] "t" {"from" 1}})))
      (is (= [{"f:t"       2
               "f:assert"  [{"ex:x" "foo-2" "id" "ex:dan"}]
               "f:retract" []}
              {"f:t"       3
               "f:assert"  []
               "f:retract" [{"ex:x" "foo-2" "id" "ex:dan"}]}]
             @(fluree/history ledger {"history" [nil "ex:x" "foo-2"] "t" {"from" 1}})))
      (is (= [{"f:t" 5 "f:assert" [{"ex:x" "foo-cat" "id" "ex:dan"}] "f:retract" []}]
             @(fluree/history ledger {"history" ["ex:dan" "ex:x" "foo-cat"] "t" {"from" 1}}))))

    (testing "at-t"
      (let [expected [{"f:t"       3
                       "f:assert"  [{"ex:x" "foo-3" "id" "ex:dan"}]
                       "f:retract" [{"ex:x" "foo-2" "id" "ex:dan"}]}]]
        (is (= expected
               @(fluree/history ledger {"history" ["ex:dan" "ex:x"] "t" {"from" 3 "to" 3}})))
        (is (= expected
               @(fluree/history ledger {"history" ["ex:dan" "ex:x"] "t" {"at" 3}})))))
    (testing "from-t"
      (is (= [{"f:t"       3
               "f:assert"  [{"ex:x" "foo-3" "id" "ex:dan"}]
               "f:retract" [{"ex:x" "foo-2" "id" "ex:dan"}]}
              {"f:t"       5
               "f:assert"  [{"ex:x" "foo-cat" "id" "ex:dan"}]
               "f:retract" [{"ex:x" "foo-3" "id" "ex:dan"}]}]
             @(fluree/history ledger {"history" ["ex:dan" "ex:x"] "t" {"from" 3}}))))
    (testing "to-t"
      (is (= [{"f:t"       1
               "f:assert"  [{"ex:x" "foo-1" "id" "ex:dan"}]
               "f:retract" []}
              {"f:t"       2
               "f:assert"  [{"ex:x" "foo-2" "id" "ex:dan"}]
               "f:retract" [{"ex:x" "foo-1" "id" "ex:dan"}]}
              {"f:t"       3
               "f:assert"  [{"ex:x" "foo-3" "id" "ex:dan"}]
               "f:retract" [{"ex:x" "foo-2" "id" "ex:dan"}]}]
             @(fluree/history ledger {"history" ["ex:dan" "ex:x"] "t" {"to" 3}}))))
    (testing "t-range"
      (is (= [{"f:t"       2
               "f:assert"  [{"ex:x" "foo-2" "id" "ex:dan"}]
               "f:retract" [{"ex:x" "foo-1" "id" "ex:dan"}]}
              {"f:t"       3
               "f:assert"  [{"ex:x" "foo-3" "id" "ex:dan"}]
               "f:retract" [{"ex:x" "foo-2" "id" "ex:dan"}]}
              {"f:t"       4
               "f:assert"  [{"ex:x" "foo-dog" "id" "ex:dog"} {"ex:x" "foo-cat" "id" "ex:cat"}]
               "f:retract" [{"ex:x" "foo-1" "id" "ex:dog"} {"ex:x" "foo-1" "id" "ex:cat"}]}]
             @(fluree/history ledger {"history" [nil "ex:x"] "t" {"from" 2 "to" 4}}))))
    (testing "datetime-t"
      (is (= [{"f:t"       2
               "f:assert"  [{"ex:x" "foo-2" "id" "ex:dan"}]
               "f:retract" [{"ex:x" "foo-1" "id" "ex:dan"}]}
              {"f:t"       3
               "f:assert"  [{"ex:x" "foo-3" "id" "ex:dan"}]
               "f:retract" [{"ex:x" "foo-2" "id" "ex:dan"}]}]
             @(fluree/history ledger {"history" [nil "ex:x"] "t" {"from" ts2 "to" ts3}}))
          "does not include t 1 4 or 5")
      (is (= [{"f:t"       5
               "f:assert"  [{"ex:x" "foo-cat" "id" "ex:dan"}]
               "f:retract" [{"ex:x" "foo-3" "id" "ex:dan"}]}]
             @(fluree/history ledger {"history" ["ex:dan" "ex:x"] "t" {"from" (util/current-time-iso)}}))
          "timestamp translates to first t before ts")

      (is (= (str "There is no data as of " ts-primeval)
             (-> @(fluree/history ledger {"history" ["ex:dan" "ex:x"] "t" {"from" ts-primeval}})
                 (Throwable->map)
                 :cause))))

    (testing "invalid query"
      (is (= "History query not properly formatted. Provided {\"history\" []}"
             (-> @(fluree/history ledger {"history" []})
                 (Throwable->map)
                 :cause))))

    (testing "small cache"
      (let [conn   (test-utils/create-conn)
            ledger @(fluree/create conn "historycachetest"
                                   {"defaults"
                                    {"@context"
                                     ["" {"ex" "http://example.org/ns/"}]}})

            db1    @(test-utils/transact ledger [{"id"   "ex:dan"
                                                  "ex:x" "foo-1"
                                                  "ex:y" "bar-1"}])
            db2    @(test-utils/transact ledger {"id"   "ex:dan"
                                                 "ex:x" "foo-2"
                                                 "ex:y" "bar-2"})]
        (testing "no t-range cache collision"
          (is (= [{"f:t"       2
                   "f:assert"  [{"ex:x" "foo-2" "ex:y" "bar-2" "id" "ex:dan"}]
                   "f:retract" [{"ex:x" "foo-1" "ex:y" "bar-1" "id" "ex:dan"}]}]
                 @(fluree/history ledger {"history" ["ex:dan"] "t" {"from" 2}}))))))))

(deftest ^:integration commit-details
  (with-redefs [fluree.db.util.core/current-time-iso (fn [] "1970-01-01T00:12:00.00000Z")]
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "committest"
                                 {"defaults"
                                  {"@context"
                                   ["" {"ex" "http://example.org/ns/"}]}})

          db1    @(test-utils/transact ledger {"id"   "ex:alice"
                                               "ex:x" "foo-1"
                                               "ex:y" "bar-1"})
          db2    @(test-utils/transact ledger {"id"   "ex:alice"
                                               "ex:x" "foo-2"
                                               "ex:y" "bar-2"})
          db3    @(test-utils/transact ledger {"id"   "ex:alice"
                                               "ex:x" "foo-3"
                                               "ex:y" "bar-3"})
          db4    @(test-utils/transact ledger {"id"   "ex:cat"
                                               "ex:x" "foo-cat"
                                               "ex:y" "bar-cat"})
          db5    @(test-utils/transact ledger {"id"   "ex:alice"
                                               "ex:x" "foo-cat"
                                               "ex:y" "bar-cat"}
                                       {"message" "meow"})]
      (testing "at time t"
        (is (= [{"f:commit" {"https://www.w3.org/2018/credentials#issuer"
                             {"id" "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                             "f:address" "fluree:memory://c2e0047d4d75cad2700d5c8d0db0ad3d7dd2bbbbbdf55ba7c28dd4252a557664"
                             "f:alias"   "committest"
                             "f:branch"  "main"
                             "f:context" "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"
                             "f:data"    {"f:address" "fluree:memory://bcb581e731a7c0ceadcfbf432b4ee8cf046de377cc33f047bd05b6c47f9da94d"
                                          "f:assert"  [{"ex:x" "foo-1"
                                                        "ex:y" "bar-1"
                                                        "id"   "ex:alice"}]
                                          "f:flakes"  11
                                          "f:retract" []
                                          "f:size"    996
                                          "f:t"       1}
                             "f:time"    720000
                             "f:v"       0
                             "id"        "fluree:commit:sha256:bn6sykdmktzuxcavgrsa5ejwdzfae6njj4q3lonb5cexlfhauvpc"}}]
               @(fluree/history ledger {"commit-details" true "t" {"from" 1 "to" 1}})))
        (let [commit-5 {"f:commit" {"https://www.w3.org/2018/credentials#issuer"
                                    {"id" "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                                    "f:address"  "fluree:memory://2716b3eaef91b763b32daebab8ba3733a8537de37a864f726c5021464b90277f"
                                    "f:alias"    "committest"
                                    "f:branch"   "main"
                                    "f:context"  "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"
                                    "f:data"     {"f:address"  "fluree:memory://2b8125a4c996f8612ae62f16cc62b167b762c517b0c5aa7b16fa21dfe47e7b2a"
                                                  "f:assert"   [{"ex:x" "foo-cat"
                                                                 "ex:y" "bar-cat"
                                                                 "id"   "ex:alice"}]
                                                  "f:flakes"   102
                                                  "f:previous" {"id" "fluree:db:sha256:bbtdwia2mle22abe2z7mmdrs4vufs77yubzxip3chhkmffvfk4npk"}
                                                  "f:retract"  [{"ex:x" "foo-3"
                                                                 "ex:y" "bar-3"
                                                                 "id"   "ex:alice"}]
                                                  "f:size"     9326
                                                  "f:t"        5}
                                    "f:message"  "meow"
                                    "f:previous" {"id" "fluree:commit:sha256:bhcunj52uwxshi6jws2ypsjiziyncfa5gal4xptfldj5utb54cpf"}
                                    "f:time"     720000
                                    "f:v"        0
                                    "id"         "fluree:commit:sha256:bq4r74wu4sru43z5f4byipe5zzkgyi2kksqxlmwuttdcwcgwpsrn"}}
              commit-4 {"f:commit" {"https://www.w3.org/2018/credentials#issuer"
                                    {"id" "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                                    "f:address"  "fluree:memory://ffe008a623d3c6920f1d7d7607783893042c8093629f64ffefc8eb8472f542af"
                                    "f:alias"    "committest"
                                    "f:branch"   "main"
                                    "f:context"  "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"
                                    "f:data"     {"f:address"  "fluree:memory://2cac0ce4036dd82a0c23eb3deca0e7775386ed220411edb271803144b001326c"
                                                  "f:assert"   [{"ex:x" "foo-cat"
                                                                 "ex:y" "bar-cat"
                                                                 "id"   "ex:cat"}]
                                                  "f:flakes"   82
                                                  "f:previous" {"id" "fluree:db:sha256:bcl3anjpvmxaciox7inzx4za6teagj7ipacuadzlnwg45y6z77ts"}
                                                  "f:retract"  []
                                                  "f:size"     7588
                                                  "f:t"        4}
                                    "f:previous" {"id" "fluree:commit:sha256:b5jlses24wzjhcmqywvcdwgxxzmjlmxruh2duqsgxbxy7522utlg"}
                                    "f:time"     720000
                                    "f:v"        0
                                    "id"         "fluree:commit:sha256:bhcunj52uwxshi6jws2ypsjiziyncfa5gal4xptfldj5utb54cpf"}}]
          (is (= [commit-4 commit-5]
                 @(fluree/history ledger {"commit-details" true "t" {"from" 4 "to" 5}})))
          (is (= [commit-5]
                 @(fluree/history ledger {"commit-details" true "t" {"at" "latest"}})))))

      (testing "time range"
        (let [[c2 c3 c4 :as response] @(fluree/history
                                         ledger
                                         {"commit-details" true
                                          "t"              {"from" 2 "to" 4}})]
          (testing "all commits in time range are returned"
            (is (= 3 (count response)))
            (is (= {"f:commit" {"https://www.w3.org/2018/credentials#issuer"
                                {"id" "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                                "f:address"  "fluree:memory://ffe008a623d3c6920f1d7d7607783893042c8093629f64ffefc8eb8472f542af"
                                "f:alias"    "committest"
                                "f:branch"   "main"
                                "f:context"  "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"
                                "f:data"     {"f:address"  "fluree:memory://2cac0ce4036dd82a0c23eb3deca0e7775386ed220411edb271803144b001326c"
                                              "f:assert"   [{"ex:x" "foo-cat"
                                                             "ex:y" "bar-cat"
                                                             "id"   "ex:cat"}]
                                              "f:flakes"   82
                                              "f:previous" {"id" "fluree:db:sha256:bcl3anjpvmxaciox7inzx4za6teagj7ipacuadzlnwg45y6z77ts"}
                                              "f:retract"  []
                                              "f:size"     7588
                                              "f:t"        4}
                                "f:previous" {"id" "fluree:commit:sha256:b5jlses24wzjhcmqywvcdwgxxzmjlmxruh2duqsgxbxy7522utlg"}
                                "f:time"     720000
                                "f:v"        0
                                "id"         "fluree:commit:sha256:bhcunj52uwxshi6jws2ypsjiziyncfa5gal4xptfldj5utb54cpf"}}
                   c4)))
          (is (= {"f:commit" {"https://www.w3.org/2018/credentials#issuer"
                              {"id" "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                              "f:address"  "fluree:memory://f29bfb686c834d667dc62f8c46af1802f02c62567b400d50c0202428e489d1fe"
                              "f:alias"    "committest"
                              "f:branch"   "main"
                              "f:context"  "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"
                              "f:data"     {"f:address"  "fluree:memory://cb16fc43954b9ed029be2c96c6f73fa5e34e5ca1607111c5e8f8d6e337b648f6"
                                            "f:assert"   [{"ex:x" "foo-3"
                                                           "ex:y" "bar-3"
                                                           "id"   "ex:alice"}]
                                            "f:flakes"   63
                                            "f:previous" {"id" "fluree:db:sha256:bjufs3dmyea7wzbkrjrh2pzua2mtqgijnl3cqpkktk7wzvy5wlnq"}
                                            "f:retract"  [{"ex:x" "foo-2"
                                                           "ex:y" "bar-2"
                                                           "id"   "ex:alice"}]
                                            "f:size"     5864
                                            "f:t"        3}
                              "f:previous" {"id" "fluree:commit:sha256:bbvotmnlqkm4xkc27au55rctw5klntegd36j4dbh665qfilem42eq"}
                              "f:time"     720000
                              "f:v"        0
                              "id"         "fluree:commit:sha256:b5jlses24wzjhcmqywvcdwgxxzmjlmxruh2duqsgxbxy7522utlg"}}
                 c3))
          (is (= {"f:commit" {"https://www.w3.org/2018/credentials#issuer"
                              {"id" "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                              "f:address"  "fluree:memory://a3f34c963d5724782dc33aae1c9e0d8dc8b1f35092a659cb2431d7204b95288c"
                              "f:alias"    "committest"
                              "f:branch"   "main"
                              "f:context"  "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"
                              "f:data"     {"f:address"  "fluree:memory://2fb8ce1aa6771837c7986b1d81dcdb42ca8be8927679fe3814b2fb044b903b9d"
                                            "f:assert"   [{"ex:x" "foo-2"
                                                           "ex:y" "bar-2"
                                                           "id"   "ex:alice"}]
                                            "f:flakes"   43
                                            "f:previous" {"id" "fluree:db:sha256:bbbi2zkypmbphdnt7ntmtqxtuvayt5izcbfkjaqfrlq2ixxrj5dcu"}
                                            "f:retract"  [{"ex:x" "foo-1"
                                                           "ex:y" "bar-1"
                                                           "id"   "ex:alice"}]
                                            "f:size"     4134
                                            "f:t"        2}
                              "f:previous" {"id" "fluree:commit:sha256:bn6sykdmktzuxcavgrsa5ejwdzfae6njj4q3lonb5cexlfhauvpc"}
                              "f:time"     720000
                              "f:v"        0
                              "id"         "fluree:commit:sha256:bbvotmnlqkm4xkc27au55rctw5klntegd36j4dbh665qfilem42eq"}}
                 c2))))

      (testing "time range from"
        (is (= [{"f:commit" {"https://www.w3.org/2018/credentials#issuer"
                             {"id" "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                             "f:address"  "fluree:memory://ffe008a623d3c6920f1d7d7607783893042c8093629f64ffefc8eb8472f542af"
                             "f:alias"    "committest"
                             "f:branch"   "main"
                             "f:context"  "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"
                             "f:data"     {"f:address"  "fluree:memory://2cac0ce4036dd82a0c23eb3deca0e7775386ed220411edb271803144b001326c"
                                           "f:assert"   [{"ex:x" "foo-cat"
                                                          "ex:y" "bar-cat"
                                                          "id"   "ex:cat"}]
                                           "f:flakes"   82
                                           "f:previous" {"id" "fluree:db:sha256:bcl3anjpvmxaciox7inzx4za6teagj7ipacuadzlnwg45y6z77ts"}
                                           "f:retract"  []
                                           "f:size"     7588
                                           "f:t"        4}
                             "f:previous" {"id" "fluree:commit:sha256:b5jlses24wzjhcmqywvcdwgxxzmjlmxruh2duqsgxbxy7522utlg"}
                             "f:time"     720000
                             "f:v"        0
                             "id"         "fluree:commit:sha256:bhcunj52uwxshi6jws2ypsjiziyncfa5gal4xptfldj5utb54cpf"}}
                {"f:commit" {"https://www.w3.org/2018/credentials#issuer"
                             {"id" "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                             "f:address"  "fluree:memory://2716b3eaef91b763b32daebab8ba3733a8537de37a864f726c5021464b90277f"
                             "f:alias"    "committest"
                             "f:branch"   "main"
                             "f:context"  "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"
                             "f:data"     {"f:address"  "fluree:memory://2b8125a4c996f8612ae62f16cc62b167b762c517b0c5aa7b16fa21dfe47e7b2a"
                                           "f:assert"   [{"ex:x" "foo-cat"
                                                          "ex:y" "bar-cat"
                                                          "id"   "ex:alice"}]
                                           "f:flakes"   102
                                           "f:previous" {"id" "fluree:db:sha256:bbtdwia2mle22abe2z7mmdrs4vufs77yubzxip3chhkmffvfk4npk"}
                                           "f:retract"  [{"ex:x" "foo-3"
                                                          "ex:y" "bar-3"
                                                          "id"   "ex:alice"}]
                                           "f:size"     9326
                                           "f:t"        5}
                             "f:message"  "meow"
                             "f:previous" {"id" "fluree:commit:sha256:bhcunj52uwxshi6jws2ypsjiziyncfa5gal4xptfldj5utb54cpf"}
                             "f:time"     720000
                             "f:v"        0
                             "id"         "fluree:commit:sha256:bq4r74wu4sru43z5f4byipe5zzkgyi2kksqxlmwuttdcwcgwpsrn"}}]
               @(fluree/history ledger {"commit-details" true "t" {"from" 4}}))))

      (testing "time range to"
        (is (= [{"f:commit" {"https://www.w3.org/2018/credentials#issuer"
                             {"id" "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                             "f:address" "fluree:memory://c2e0047d4d75cad2700d5c8d0db0ad3d7dd2bbbbbdf55ba7c28dd4252a557664"
                             "f:alias"   "committest"
                             "f:branch"  "main"
                             "f:context" "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"
                             "f:data"    {"f:address" "fluree:memory://bcb581e731a7c0ceadcfbf432b4ee8cf046de377cc33f047bd05b6c47f9da94d"
                                          "f:assert"  [{"ex:x" "foo-1"
                                                        "ex:y" "bar-1"
                                                        "id"   "ex:alice"}]
                                          "f:flakes"  11
                                          "f:retract" []
                                          "f:size"    996
                                          "f:t"       1}
                             "f:time"    720000
                             "f:v"       0
                             "id"        "fluree:commit:sha256:bn6sykdmktzuxcavgrsa5ejwdzfae6njj4q3lonb5cexlfhauvpc"}}]
               @(fluree/history ledger {"commit-details" true "t" {"to" 1}}))))

      (testing "history commit details"
        (is (= [{"f:assert"  [{"ex:x" "foo-3"
                               "ex:y" "bar-3"
                               "id"   "ex:alice"}]
                 "f:commit"  {"https://www.w3.org/2018/credentials#issuer"
                              {"id" "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                              "f:address"  "fluree:memory://f29bfb686c834d667dc62f8c46af1802f02c62567b400d50c0202428e489d1fe"
                              "f:alias"    "committest"
                              "f:branch"   "main"
                              "f:context"  "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"
                              "f:data"     {"f:address"  "fluree:memory://cb16fc43954b9ed029be2c96c6f73fa5e34e5ca1607111c5e8f8d6e337b648f6"
                                            "f:assert"   [{"ex:x" "foo-3"
                                                           "ex:y" "bar-3"
                                                           "id"   "ex:alice"}]
                                            "f:flakes"   63
                                            "f:previous" {"id" "fluree:db:sha256:bjufs3dmyea7wzbkrjrh2pzua2mtqgijnl3cqpkktk7wzvy5wlnq"}
                                            "f:retract"  [{"ex:x" "foo-2"
                                                           "ex:y" "bar-2"
                                                           "id"   "ex:alice"}]
                                            "f:size"     5864
                                            "f:t"        3}
                              "f:previous" {"id" "fluree:commit:sha256:bbvotmnlqkm4xkc27au55rctw5klntegd36j4dbh665qfilem42eq"}
                              "f:time"     720000
                              "f:v"        0
                              "id"         "fluree:commit:sha256:b5jlses24wzjhcmqywvcdwgxxzmjlmxruh2duqsgxbxy7522utlg"}
                 "f:retract" [{"ex:x" "foo-2"
                               "ex:y" "bar-2"
                               "id"   "ex:alice"}]
                 "f:t"       3}
                {"f:assert"  [{"ex:x" "foo-cat"
                               "ex:y" "bar-cat"
                               "id"   "ex:alice"}]
                 "f:commit"  {"https://www.w3.org/2018/credentials#issuer"
                              {"id" "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                              "f:address"  "fluree:memory://2716b3eaef91b763b32daebab8ba3733a8537de37a864f726c5021464b90277f"
                              "f:alias"    "committest"
                              "f:branch"   "main"
                              "f:context"  "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"
                              "f:data"     {"f:address"  "fluree:memory://2b8125a4c996f8612ae62f16cc62b167b762c517b0c5aa7b16fa21dfe47e7b2a"
                                            "f:assert"   [{"ex:x" "foo-cat"
                                                           "ex:y" "bar-cat"
                                                           "id"   "ex:alice"}]
                                            "f:flakes"   102
                                            "f:previous" {"id" "fluree:db:sha256:bbtdwia2mle22abe2z7mmdrs4vufs77yubzxip3chhkmffvfk4npk"}
                                            "f:retract"  [{"ex:x" "foo-3"
                                                           "ex:y" "bar-3"
                                                           "id"   "ex:alice"}]
                                            "f:size"     9326
                                            "f:t"        5}
                              "f:message"  "meow"
                              "f:previous" {"id" "fluree:commit:sha256:bhcunj52uwxshi6jws2ypsjiziyncfa5gal4xptfldj5utb54cpf"}
                              "f:time"     720000
                              "f:v"        0
                              "id"         "fluree:commit:sha256:bq4r74wu4sru43z5f4byipe5zzkgyi2kksqxlmwuttdcwcgwpsrn"}
                 "f:retract" [{"ex:x" "foo-3"
                               "ex:y" "bar-3"
                               "id"   "ex:alice"}]
                 "f:t"       5}]
               @(fluree/history ledger {"history" "ex:alice" "commit-details" true "t" {"from" 3}}))))

      ;; TODO: Fix this https://github.com/fluree/db/issues/451
      #_(testing "history commit details on a loaded ledger"
          (is (= [{"f:assert"  [{"ex:x" "foo-3"
                                 "ex:y" "bar-3"
                                 "id"   "ex:alice"}]
                   "f:commit"  {"https://www.w3.org/2018/credentials#issuer"
                                {"id" "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                                "f:address"  "fluree:memory://f29bfb686c834d667dc62f8c46af1802f02c62567b400d50c0202428e489d1fe"
                                "f:alias"    "committest"
                                "f:branch"   "main"
                                "f:context"  "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"
                                "f:data"     {"f:address"  "fluree:memory://cb16fc43954b9ed029be2c96c6f73fa5e34e5ca1607111c5e8f8d6e337b648f6"
                                              "f:assert"   [{"ex:x" "foo-3"
                                                             "ex:y" "bar-3"
                                                             "id"   "ex:alice"}]
                                              "f:flakes"   63
                                              "f:previous" {"id" "fluree:db:sha256:bjufs3dmyea7wzbkrjrh2pzua2mtqgijnl3cqpkktk7wzvy5wlnq"}
                                              "f:retract"  [{"ex:x" "foo-2"
                                                             "ex:y" "bar-2"
                                                             "id"   "ex:alice"}]
                                              "f:size"     5864
                                              "f:t"        3}
                                "f:previous" {"id" "fluree:commit:sha256:bbvotmnlqkm4xkc27au55rctw5klntegd36j4dbh665qfilem42eq"}
                                "f:time"     720000
                                "f:v"        0
                                "id"         "fluree:commit:sha256:b5jlses24wzjhcmqywvcdwgxxzmjlmxruh2duqsgxbxy7522utlg"}
                   "f:retract" [{"ex:x" "foo-2"
                                 "ex:y" "bar-2"
                                 "id"   "ex:alice"}]
                   "f:t"       3}
                  {"f:assert"  [{"ex:x" "foo-cat"
                                 "ex:y" "bar-cat"
                                 "id"   "ex:alice"}]
                   "f:commit"  {"https://www.w3.org/2018/credentials#issuer"
                                {"id" "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                                "f:address"  "fluree:memory://2716b3eaef91b763b32daebab8ba3733a8537de37a864f726c5021464b90277f"
                                "f:alias"    "committest"
                                "f:branch"   "main"
                                "f:context"  "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"
                                "f:data"     {"f:address"  "fluree:memory://2b8125a4c996f8612ae62f16cc62b167b762c517b0c5aa7b16fa21dfe47e7b2a"
                                              "f:assert"   [{"ex:x" "foo-cat"
                                                             "ex:y" "bar-cat"
                                                             "id"   "ex:alice"}]
                                              "f:flakes"   102
                                              "f:previous" {"id" "fluree:db:sha256:bbtdwia2mle22abe2z7mmdrs4vufs77yubzxip3chhkmffvfk4npk"}
                                              "f:retract"  [{"ex:x" "foo-3"
                                                             "ex:y" "bar-3"
                                                             "id"   "ex:alice"}]
                                              "f:size"     9326
                                              "f:t"        5}
                                "f:message"  "meow"
                                "f:previous" {"id" "fluree:commit:sha256:bhcunj52uwxshi6jws2ypsjiziyncfa5gal4xptfldj5utb54cpf"}
                                "f:time"     720000
                                "f:v"        0
                                "id"         "fluree:commit:sha256:bq4r74wu4sru43z5f4byipe5zzkgyi2kksqxlmwuttdcwcgwpsrn"}
                   "f:retract" [{"ex:x" "foo-3"
                                 "ex:y" "bar-3"
                                 "id"   "ex:alice"}]
                   "f:t"       5}]
                 (let [loaded-ledger @(fluree/load conn "committest")]
                   @(fluree/history loaded-ledger {"history" "ex:alice"
                                                   "commit-details" true
                                                   "t" {"from" 3}})))))

      (testing "multiple history results"
        (let [history-with-commits @(fluree/history ledger {"history" "ex:alice" "commit-details" true "t" {"from" 1 "to" 5}})]
          (testing "all `t`s with changes to subject are returned"
            (is (= [1 2 3 5]
                   (mapv #(get % "f:t") history-with-commits))))
          (testing "all expected commits are present and associated with the correct results"
            (is (= [[1 1] [2 2] [3 3] [5 5]]
                   (map (fn [history-map]
                          (let [commit-t (get-in history-map ["f:commit" "f:data" "f:t"])]
                            (vector (get history-map "f:t") commit-t)))
                        history-with-commits)))))))))
