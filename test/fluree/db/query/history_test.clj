(ns fluree.db.query.history-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.core :as util]
            [clojure.string :as str]))

(deftest ^:integration history-query
  (let [ts-primeval (util/current-time-iso)

        conn        (test-utils/create-conn)
        ledger      @(fluree/create conn "historytest" {:defaultContext ["" {:ex "http://example.org/ns/"}]})

        db1         @(test-utils/transact ledger [{:id   :ex/dan
                                                   :ex/x "foo-1"
                                                   :ex/y "bar-1"}
                                                  {:id   :ex/cat
                                                   :ex/x "foo-1"
                                                   :ex/y "bar-1"}
                                                  {:id   :ex/dog
                                                   :ex/x "foo-1"
                                                   :ex/y "bar-1"}])
        db2         @(test-utils/transact ledger {:id   :ex/dan
                                                  :ex/x "foo-2"
                                                  :ex/y "bar-2"})
        ts2         (-> db2 :commit :time)
        db3         @(test-utils/transact ledger {:id   :ex/dan
                                                  :ex/x "foo-3"
                                                  :ex/y "bar-3"})

        ts3         (-> db3 :commit :time)
        db4         @(test-utils/transact ledger [{:id   :ex/cat
                                                   :ex/x "foo-cat"
                                                   :ex/y "bar-cat"}
                                                  {:id   :ex/dog
                                                   :ex/x "foo-dog"
                                                   :ex/y "bar-dog"}])
        db5         @(test-utils/transact ledger {:id   :ex/dan
                                                  :ex/x "foo-cat"
                                                  :ex/y "bar-cat"})]
    (testing "subject history"
      (is (= [{:f/t       1
               :f/assert  [{:id :ex/dan :ex/x "foo-1" :ex/y "bar-1"}]
               :f/retract []}
              {:f/t       2
               :f/assert  [{:id :ex/dan :ex/x "foo-2" :ex/y "bar-2"}]
               :f/retract [{:id :ex/dan :ex/x "foo-1" :ex/y "bar-1"}]}
              {:f/t       3
               :f/assert  [{:id :ex/dan :ex/x "foo-3" :ex/y "bar-3"}]
               :f/retract [{:id :ex/dan :ex/x "foo-2" :ex/y "bar-2"}]}
              {:f/t       5
               :f/assert  [{:id :ex/dan :ex/x "foo-cat" :ex/y "bar-cat"}]
               :f/retract [{:id :ex/dan :ex/x "foo-3" :ex/y "bar-3"}]}]
             @(fluree/history ledger {:history :ex/dan :t {:from 1}}))))
    (testing "one-tuple flake history"
      (is (= [{:f/t       1
               :f/assert  [{:id :ex/dan :ex/x "foo-1" :ex/y "bar-1"}]
               :f/retract []}
              {:f/t       2
               :f/assert  [{:ex/x "foo-2" :ex/y "bar-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :ex/y "bar-1" :id :ex/dan}]}
              {:f/t       3
               :f/assert  [{:ex/x "foo-3" :ex/y "bar-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :ex/y "bar-2" :id :ex/dan}]}
              {:f/t       5
               :f/assert  [{:ex/x "foo-cat" :ex/y "bar-cat" :id :ex/dan}]
               :f/retract [{:ex/x "foo-3" :ex/y "bar-3" :id :ex/dan}]}]
             @(fluree/history ledger {:history [:ex/dan] :t {:from 1}}))))
    (testing "two-tuple flake history"
      (is (= [{:f/t 1 :f/assert [{:ex/x "foo-1" :id :ex/dan}] :f/retract []}
              {:f/t       2
               :f/assert  [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :id :ex/dan}]}
              {:f/t       3
               :f/assert  [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}
              {:f/t       5
               :f/assert  [{:ex/x "foo-cat" :id :ex/dan}]
               :f/retract [{:ex/x "foo-3" :id :ex/dan}]}]
             @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:from 1}})))

      (is (= [{:f/t       1 :f/assert [{:ex/x "foo-1" :id :ex/dog}
                                       {:ex/x "foo-1" :id :ex/cat}
                                       {:ex/x "foo-1" :id :ex/dan}]
               :f/retract []}
              {:f/t       2
               :f/assert  [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :id :ex/dan}]}
              {:f/t       3
               :f/assert  [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}
              {:f/t       4
               :f/assert  [{:ex/x "foo-dog" :id :ex/dog}
                           {:ex/x "foo-cat" :id :ex/cat}]
               :f/retract [{:ex/x "foo-1" :id :ex/dog}
                           {:ex/x "foo-1" :id :ex/cat}]}
              {:f/t       5
               :f/assert  [{:ex/x "foo-cat" :id :ex/dan}]
               :f/retract [{:ex/x "foo-3" :id :ex/dan}]}]
             @(fluree/history ledger {:history [nil :ex/x] :t {:from 1}}))))
    (testing "three-tuple flake history"
      (is (= [{:f/t 4 :f/assert [{:ex/x "foo-cat" :id :ex/cat}] :f/retract []}
              {:f/t 5 :f/assert [{:ex/x "foo-cat" :id :ex/dan}] :f/retract []}]
             @(fluree/history ledger {:history [nil :ex/x "foo-cat"] :t {:from 1}})))
      (is (= [{:f/t       2
               :f/assert  [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract []}
              {:f/t       3
               :f/assert  []
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}]
             @(fluree/history ledger {:history [nil :ex/x "foo-2"] :t {:from 1}})))
      (is (= [{:f/t 5 :f/assert [{:ex/x "foo-cat" :id :ex/dan}] :f/retract []}]
             @(fluree/history ledger {:history [:ex/dan :ex/x "foo-cat"] :t {:from 1}}))))

    (testing "at-t"
      (let [expected [{:f/t       3
                       :f/assert  [{:ex/x "foo-3" :id :ex/dan}]
                       :f/retract [{:ex/x "foo-2" :id :ex/dan}]}]]
        (is (= expected
               @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:from 3 :to 3}})))
        (is (= expected
               @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:at 3}})))))
    (testing "from-t"
      (is (= [{:f/t       3
               :f/assert  [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}
              {:f/t       5
               :f/assert  [{:ex/x "foo-cat" :id :ex/dan}]
               :f/retract [{:ex/x "foo-3" :id :ex/dan}]}]
             @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:from 3}}))))
    (testing "to-t"
      (is (= [{:f/t       1
               :f/assert  [{:ex/x "foo-1" :id :ex/dan}]
               :f/retract []}
              {:f/t       2
               :f/assert  [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :id :ex/dan}]}
              {:f/t       3
               :f/assert  [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}]
             @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:to 3}}))))
    (testing "t-range"
      (is (= [{:f/t       2
               :f/assert  [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :id :ex/dan}]}
              {:f/t       3
               :f/assert  [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}
              {:f/t       4
               :f/assert  [{:ex/x "foo-dog" :id :ex/dog} {:ex/x "foo-cat" :id :ex/cat}]
               :f/retract [{:ex/x "foo-1" :id :ex/dog} {:ex/x "foo-1" :id :ex/cat}]}]
             @(fluree/history ledger {:history [nil :ex/x] :t {:from 2 :to 4}}))))
    (testing "datetime-t"
      (is (= [{:f/t       2
               :f/assert  [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :id :ex/dan}]}
              {:f/t       3
               :f/assert  [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}]
             @(fluree/history ledger {:history [nil :ex/x] :t {:from ts2 :to ts3}}))
          "does not include t 1 4 or 5")
      (is (= [{:f/t       5
               :f/assert  [{:ex/x "foo-cat" :id :ex/dan}]
               :f/retract [{:ex/x "foo-3" :id :ex/dan}]}]
             @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:from (util/current-time-iso)}}))
          "timestamp translates to first t before ts")

      (is (= (str "There is no data as of " ts-primeval)
             (-> @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:from ts-primeval}})
                 (Throwable->map)
                 :cause))))

    (testing "invalid query"
      (is (= "History query not properly formatted. Provided {:history []}"
             (-> @(fluree/history ledger {:history []})
                 (Throwable->map)
                 :cause))))

    (testing "small cache"
      (let [conn   (test-utils/create-conn)
            ledger @(fluree/create conn "historycachetest" {:defaultContext ["" {:ex "http://example.org/ns/"}]})

            db1    @(test-utils/transact ledger [{:id   :ex/dan
                                                  :ex/x "foo-1"
                                                  :ex/y "bar-1"}])
            db2    @(test-utils/transact ledger {:id   :ex/dan
                                                 :ex/x "foo-2"
                                                 :ex/y "bar-2"})]
        (testing "no t-range cache collision"
          (is (= [{:f/t       2
                   :f/assert  [{:ex/x "foo-2" :ex/y "bar-2" :id :ex/dan}]
                   :f/retract [{:ex/x "foo-1" :ex/y "bar-1" :id :ex/dan}]}]
                 @(fluree/history ledger {:history [:ex/dan] :t {:from 2}}))))))))

(deftest ^:integration commit-details
  (with-redefs [fluree.db.util.core/current-time-iso (fn [] "1970-01-01T00:12:00.00000Z")]
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "committest" {:defaultContext ["" {:ex "http://example.org/ns/"}]})

          db1    @(test-utils/transact ledger {:id   :ex/alice
                                               :ex/x "foo-1"
                                               :ex/y "bar-1"})
          db2    @(test-utils/transact ledger {:id   :ex/alice
                                               :ex/x "foo-2"
                                               :ex/y "bar-2"})
          db3    @(test-utils/transact ledger {:id   :ex/alice
                                               :ex/x "foo-3"
                                               :ex/y "bar-3"})
          db4    @(test-utils/transact ledger {:id   :ex/cat
                                               :ex/x "foo-cat"
                                               :ex/y "bar-cat"})
          db5    @(test-utils/transact ledger {:id   :ex/alice
                                               :ex/x "foo-cat"
                                               :ex/y "bar-cat"}
                                       {:message "meow"})]
      (testing "at time t"
        (is (= [{:f/commit {"https://www.w3.org/2018/credentials#issuer"
                            {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                            :f/address        "fluree:memory://f552d786403cca33da44d3cd26606a787636dc8fae891523b7155609a0065cbe"
                            :f/alias          "committest"
                            :f/branch         "main"
                            :f/defaultContext {:id "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee",}
                            :f/data           #:f{:address "fluree:memory://bcb581e731a7c0ceadcfbf432b4ee8cf046de377cc33f047bd05b6c47f9da94d"
                                                  :assert  [{:ex/x "foo-1"
                                                             :ex/y "bar-1"
                                                             :id   :ex/alice}]
                                                  :flakes  11
                                                  :retract []
                                                  :size    996
                                                  :t       1}
                            :f/time           720000
                            :f/v              0
                            :id               "fluree:commit:sha256:bqach3bavh2jnzepzvpvzsdkhgd5iu6pzs42pcc6lttciaubznqj"}}]
               @(fluree/history ledger {:commit-details true :t {:from 1 :to 1}})))
        (let [commit-5 {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                                   {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                                   :f/address        "fluree:memory://11da6993f6e8c6afa0923faeea8afa113f5f3412af258abf65066a87301a2ed2"
                                   :f/alias          "committest"
                                   :f/branch         "main"
                                   :f/defaultContext {:id "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee",}
                                   :f/data           #:f{:address  "fluree:memory://fc07c9e086f121928801fd3b3ca7d995d4183c5b53db9cb63fcb24d1312e593e"
                                                         :assert   [{:ex/x "foo-cat"
                                                                     :ex/y "bar-cat"
                                                                     :id   :ex/alice}]
                                                         :flakes   104
                                                         :previous {:id "fluree:db:sha256:bbxuu3o3hbbgkaww5kux2mt2pbzsmfbus3jfg6vp3gt7ur7ews4ib"}
                                                         :retract  [{:ex/x "foo-3"
                                                                     :ex/y "bar-3"
                                                                     :id   :ex/alice}]
                                                         :size     9136
                                                         :t        5}
                                   :f/message        "meow"
                                   :f/previous       {:id "fluree:commit:sha256:bbtc7emczjpsndhqxaezru3tijpl7j2mqlygiebzcfhof7qnrlyej"}
                                   :f/time           720000
                                   :f/v              0
                                   :id               "fluree:commit:sha256:bsvdy5ckeo6x62r4eadzckcfkzlyvdewiwwfeyaysvtpbvmejkbq"}}
              commit-4 {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                                   {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                                   :f/address        "fluree:memory://26b66163e38838e4fd3a3b9747ab62293b0e94e901ec30a04f8681ce78e7f305"
                                   :f/alias          "committest"
                                   :f/branch         "main"
                                   :f/defaultContext {:id "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee",}
                                   :f/data           #:f{:address  "fluree:memory://9d6208e645a959344da7980e834fac25ca5a48d065cb5ea0eed6b570c71e316d"
                                                         :assert   [{:ex/x "foo-cat"
                                                                     :ex/y "bar-cat"
                                                                     :id   :ex/cat}]
                                                         :flakes   84
                                                         :previous {:id "fluree:db:sha256:btltglu2b4trfwzsd4jjn7u7dgg5hkymfcxzbzgfrwep6yli3xxt"}
                                                         :retract  []
                                                         :size     7548
                                                         :t        4}
                                   :f/previous       {:id "fluree:commit:sha256:bbmio7wekwnrf6ixwo22urjlkjx56kkluy6xjet5pihrsx4voapme"}
                                   :f/time           720000
                                   :f/v              0
                                   :id               "fluree:commit:sha256:bbtc7emczjpsndhqxaezru3tijpl7j2mqlygiebzcfhof7qnrlyej"}}]
          (is (= [commit-4 commit-5]
                 @(fluree/history ledger {:commit-details true :t {:from 4 :to 5}})))
          (is (= [commit-5]
                 @(fluree/history ledger {:commit-details true :t {:at :latest}})))))

      (testing "time range"
        (let [[c2 c3 c4 :as response] @(fluree/history
                                        ledger
                                        {:commit-details true
                                         :t              {:from 2 :to 4}})]
          (testing "all commits in time range are returned"
            (is (= 3 (count response)))
            (is (= {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                               {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                               :f/address        "fluree:memory://26b66163e38838e4fd3a3b9747ab62293b0e94e901ec30a04f8681ce78e7f305"
                               :f/alias          "committest"
                               :f/branch         "main"
                               :f/defaultContext {:id "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}
                               :f/data           #:f{:address  "fluree:memory://9d6208e645a959344da7980e834fac25ca5a48d065cb5ea0eed6b570c71e316d"
                                                     :assert   [{:ex/x "foo-cat"
                                                                 :ex/y "bar-cat"
                                                                 :id   :ex/cat}]
                                                     :flakes   84
                                                     :previous {:id "fluree:db:sha256:btltglu2b4trfwzsd4jjn7u7dgg5hkymfcxzbzgfrwep6yli3xxt"}
                                                     :retract  []
                                                     :size     7548
                                                     :t        4}
                               :f/previous       {:id "fluree:commit:sha256:bbmio7wekwnrf6ixwo22urjlkjx56kkluy6xjet5pihrsx4voapme"}
                               :f/time           720000
                               :f/v              0
                               :id               "fluree:commit:sha256:bbtc7emczjpsndhqxaezru3tijpl7j2mqlygiebzcfhof7qnrlyej"}}
                   c4)))
          (is (= {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                             {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                             :f/address        "fluree:memory://19356d348b3794d21a4dde576c3fdc8a94f191d75bb8f06b589d4cbe11ead2c0"
                             :f/alias          "committest"
                             :f/branch         "main"
                             :f/defaultContext {:id "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}
                             :f/data           #:f{:address  "fluree:memory://6defc84081268ac83403a0f66f89ff775b0d4aaf8b15683a8732a4ba1a3022ba"
                                                   :assert   [{:ex/x "foo-3"
                                                               :ex/y "bar-3"
                                                               :id   :ex/alice}]
                                                   :flakes   65
                                                   :previous {:id "fluree:db:sha256:bnuycsv7eakoeqwcc6qu6yktou3rqzgszs5tjnten5fylm2cinfy"}
                                                   :retract  [{:ex/x "foo-2"
                                                               :ex/y "bar-2"
                                                               :id   :ex/alice}]
                                                   :size     5974
                                                   :t        3}
                             :f/previous       {:id "fluree:commit:sha256:bu6ul5q7pc4byvmklra7sirndnigjapym523zitymb6vpcyx4mht"}
                             :f/time           720000
                             :f/v              0
                             :id               "fluree:commit:sha256:bbmio7wekwnrf6ixwo22urjlkjx56kkluy6xjet5pihrsx4voapme"}}
                 c3))
          (is (= {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                             {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                             :f/address        "fluree:memory://659a8d1976b227e5abc9750786f729dd07a91c6c58f2fe7fa98646e125926123"
                             :f/alias          "committest"
                             :f/branch         "main"
                             :f/defaultContext {:id "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}
                             :f/data           #:f{:address  "fluree:memory://c8760bdeac362deed5ef204c4bda2b004dcc3e445b86ac4111045c1781422073"
                                                   :assert   [{:ex/x "foo-2"
                                                               :ex/y "bar-2"
                                                               :id   :ex/alice}]
                                                   :flakes   45
                                                   :previous {:id "fluree:db:sha256:bbbi2zkypmbphdnt7ntmtqxtuvayt5izcbfkjaqfrlq2ixxrj5dcu"}
                                                   :retract  [{:ex/x "foo-1"
                                                               :ex/y "bar-1"
                                                               :id   :ex/alice}]
                                                   :size     4398
                                                   :t        2}
                             :f/previous       {:id "fluree:commit:sha256:bqach3bavh2jnzepzvpvzsdkhgd5iu6pzs42pcc6lttciaubznqj"}
                             :f/time           720000
                             :f/v              0
                             :id               "fluree:commit:sha256:bu6ul5q7pc4byvmklra7sirndnigjapym523zitymb6vpcyx4mht"}}
                 c2))))

      (testing "time range from"
        (is (= [{:f/commit {"https://www.w3.org/2018/credentials#issuer"
                            {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                            :f/address        "fluree:memory://26b66163e38838e4fd3a3b9747ab62293b0e94e901ec30a04f8681ce78e7f305"
                            :f/alias          "committest"
                            :f/branch         "main"
                            :f/defaultContext {:id "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee",}
                            :f/data           #:f{:address  "fluree:memory://9d6208e645a959344da7980e834fac25ca5a48d065cb5ea0eed6b570c71e316d"
                                                  :assert   [{:ex/x "foo-cat"
                                                              :ex/y "bar-cat"
                                                              :id   :ex/cat}]
                                                  :flakes   84
                                                  :previous {:id "fluree:db:sha256:btltglu2b4trfwzsd4jjn7u7dgg5hkymfcxzbzgfrwep6yli3xxt"}
                                                  :retract  []
                                                  :size     7548
                                                  :t        4}
                            :f/previous       {:id "fluree:commit:sha256:bbmio7wekwnrf6ixwo22urjlkjx56kkluy6xjet5pihrsx4voapme"}
                            :f/time           720000
                            :f/v              0
                            :id               "fluree:commit:sha256:bbtc7emczjpsndhqxaezru3tijpl7j2mqlygiebzcfhof7qnrlyej"}}
                {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                            {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                            :f/address        "fluree:memory://11da6993f6e8c6afa0923faeea8afa113f5f3412af258abf65066a87301a2ed2"
                            :f/alias          "committest"
                            :f/branch         "main"
                            :f/defaultContext {:id "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee",}
                            :f/data           #:f{:address  "fluree:memory://fc07c9e086f121928801fd3b3ca7d995d4183c5b53db9cb63fcb24d1312e593e"
                                                  :assert   [{:ex/x "foo-cat"
                                                              :ex/y "bar-cat"
                                                              :id   :ex/alice}]
                                                  :flakes   104
                                                  :previous {:id "fluree:db:sha256:bbxuu3o3hbbgkaww5kux2mt2pbzsmfbus3jfg6vp3gt7ur7ews4ib"}
                                                  :retract  [{:ex/x "foo-3"
                                                              :ex/y "bar-3"
                                                              :id   :ex/alice}]
                                                  :size     9136
                                                  :t        5}
                            :f/message        "meow"
                            :f/previous       {:id "fluree:commit:sha256:bbtc7emczjpsndhqxaezru3tijpl7j2mqlygiebzcfhof7qnrlyej"}
                            :f/time           720000
                            :f/v              0
                            :id               "fluree:commit:sha256:bsvdy5ckeo6x62r4eadzckcfkzlyvdewiwwfeyaysvtpbvmejkbq"}}]
               @(fluree/history ledger {:commit-details true :t {:from 4}}))))

      (testing "time range to"
        (is (= [{:f/commit {"https://www.w3.org/2018/credentials#issuer"
                            {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                            :f/address        "fluree:memory://f552d786403cca33da44d3cd26606a787636dc8fae891523b7155609a0065cbe"
                            :f/alias          "committest"
                            :f/branch         "main"
                            :f/defaultContext {:id "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}
                            :f/data           #:f{:address "fluree:memory://bcb581e731a7c0ceadcfbf432b4ee8cf046de377cc33f047bd05b6c47f9da94d"
                                                  :assert  [{:ex/x "foo-1"
                                                             :ex/y "bar-1"
                                                             :id   :ex/alice}]
                                                  :flakes  11
                                                  :retract []
                                                  :size    996
                                                  :t       1}
                            :f/time           720000
                            :f/v              0
                            :id               "fluree:commit:sha256:bqach3bavh2jnzepzvpvzsdkhgd5iu6pzs42pcc6lttciaubznqj"}}]
               @(fluree/history ledger {:commit-details true :t {:to 1}}))))

      (testing "history commit details"
        (is (= [#:f{:assert  [{:ex/x "foo-3"
                               :ex/y "bar-3"
                               :id   :ex/alice}]
                    :commit  {"https://www.w3.org/2018/credentials#issuer"
                              {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                              :f/address        "fluree:memory://19356d348b3794d21a4dde576c3fdc8a94f191d75bb8f06b589d4cbe11ead2c0"
                              :f/alias          "committest"
                              :f/branch         "main"
                              :f/defaultContext {:id "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}
                              :f/data           #:f{:address  "fluree:memory://6defc84081268ac83403a0f66f89ff775b0d4aaf8b15683a8732a4ba1a3022ba"
                                                    :assert   [{:ex/x "foo-3"
                                                                :ex/y "bar-3"
                                                                :id   :ex/alice}]
                                                    :flakes   65
                                                    :previous {:id "fluree:db:sha256:bnuycsv7eakoeqwcc6qu6yktou3rqzgszs5tjnten5fylm2cinfy"}
                                                    :retract  [{:ex/x "foo-2"
                                                                :ex/y "bar-2"
                                                                :id   :ex/alice}]
                                                    :size     5974
                                                    :t        3}
                              :f/previous       {:id "fluree:commit:sha256:bu6ul5q7pc4byvmklra7sirndnigjapym523zitymb6vpcyx4mht"}
                              :f/time           720000
                              :f/v              0
                              :id               "fluree:commit:sha256:bbmio7wekwnrf6ixwo22urjlkjx56kkluy6xjet5pihrsx4voapme"}
                    :retract [{:ex/x "foo-2"
                               :ex/y "bar-2"
                               :id   :ex/alice}]
                    :t       3}
                #:f{:assert  [{:ex/x "foo-cat"
                               :ex/y "bar-cat"
                               :id   :ex/alice}]
                    :commit  {"https://www.w3.org/2018/credentials#issuer"
                              {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                              :f/address        "fluree:memory://11da6993f6e8c6afa0923faeea8afa113f5f3412af258abf65066a87301a2ed2"
                              :f/alias          "committest"
                              :f/branch         "main"
                              :f/defaultContext {:id "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}
                              :f/data           #:f{:address  "fluree:memory://fc07c9e086f121928801fd3b3ca7d995d4183c5b53db9cb63fcb24d1312e593e"
                                                    :assert   [{:ex/x "foo-cat"
                                                                :ex/y "bar-cat"
                                                                :id   :ex/alice}]
                                                    :flakes   104
                                                    :previous {:id "fluree:db:sha256:bbxuu3o3hbbgkaww5kux2mt2pbzsmfbus3jfg6vp3gt7ur7ews4ib"}
                                                    :retract  [{:ex/x "foo-3"
                                                                :ex/y "bar-3"
                                                                :id   :ex/alice}]
                                                    :size     9136
                                                    :t        5}
                              :f/message        "meow"
                              :f/previous       {:id "fluree:commit:sha256:bbtc7emczjpsndhqxaezru3tijpl7j2mqlygiebzcfhof7qnrlyej"}
                              :f/time           720000
                              :f/v              0
                              :id               "fluree:commit:sha256:bsvdy5ckeo6x62r4eadzckcfkzlyvdewiwwfeyaysvtpbvmejkbq"}
                    :retract [{:ex/x "foo-3"
                               :ex/y "bar-3"
                               :id   :ex/alice}]
                    :t       5}]
               @(fluree/history ledger {:history :ex/alice :commit-details true :t {:from 3}})))
        (testing "multiple history results"
          (let [history-with-commits @(fluree/history ledger {:history :ex/alice :commit-details true :t {:from 1 :to 5}})]
            (testing "all `t`s with changes to subject are returned"
              (is (= [1 2 3 5]
                     (mapv :f/t history-with-commits))))
            (testing "all expected commits are present and associated with the correct results"
              (is (= [[1 1] [2 2] [3 3] [5 5]]
                     (map (fn [history-map]
                            (let [commit-t (get-in history-map [:f/commit :f/data :f/t])]
                              (vector (:f/t history-map) commit-t)))
                          history-with-commits))))))))))
