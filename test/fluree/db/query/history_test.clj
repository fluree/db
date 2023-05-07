(ns fluree.db.query.history-test
  (:require [clojure.test :refer :all]
            [fluree.db.did :as did]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.core :as util]
            [test-with-files.tools :refer [with-tmp-dir]]))

(deftest ^:integration history-query-test
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

(deftest ^:integration commit-details-test
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
                            :f/address        "fluree:memory://45080eafa6185f468bbb811a570a9bb12449ac402f658f2b8c767f7dd30d0757"
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
                                   :f/address        "fluree:memory://63d517697b2e531bb62ac8b8a3eff5e0df137bc289ef473bac622cec0b7cd7f8"
                                   :f/alias          "committest"
                                   :f/branch         "main"
                                   :f/defaultContext {:id "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee",}
                                   :f/data           #:f{:address  "fluree:memory://b3eb249d58013e3af179a375ac88f52b2ffd6a887971c23ce8946680456fbc1b"
                                                         :assert   [{:ex/x "foo-cat"
                                                                     :ex/y "bar-cat"
                                                                     :id   :ex/alice}]
                                                         :flakes   104
                                                         :previous {:id "fluree:db:sha256:bbx44tyzom257xbvlpmzriwnkixuwtzaf7gzg5lebuxjarq3lmkji"}
                                                         :retract  [{:ex/x "foo-3"
                                                                     :ex/y "bar-3"
                                                                     :id   :ex/alice}]
                                                         :size     9138
                                                         :t        5}
                                   :f/message        "meow"
                                   :f/previous       {:id "fluree:commit:sha256:biioetuzl6lopu2nef6bvrxdtlab456uqnklpdc54gw3wkny3sdh"}
                                   :f/time           720000
                                   :f/v              0
                                   :id               "fluree:commit:sha256:bn6hlpssv26lj3gbyvkowopjpmcwymwewtstsggy44i65xnnpn4y"}}
              commit-4 {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                                   {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                                   :f/address        "fluree:memory://248b6ee27610662ce7bef7434b82dfec6822e4075f5cc546a45aae9575210ad9"
                                   :f/alias          "committest"
                                   :f/branch         "main"
                                   :f/defaultContext {:id "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee",}
                                   :f/data           #:f{:address  "fluree:memory://0a15a055fc33b54f7d9aec79ba7ff7353f468e6cd17a4617dc5ba96f536ac0f4"
                                                         :assert   [{:ex/x "foo-cat"
                                                                     :ex/y "bar-cat"
                                                                     :id   :ex/cat}]
                                                         :flakes   84
                                                         :previous {:id "fluree:db:sha256:bbexo3dfrnt6abi6kpkb27yvnzubxofrncwvqpglnotiu2reidgp5"}
                                                         :retract  []
                                                         :size     7552
                                                         :t        4}
                                   :f/previous       {:id "fluree:commit:sha256:bb3dkz4x37v5s23c3u5o6lb3gm2vxj2jhyq3rgemtrhhr35ufaduw"}
                                   :f/time           720000
                                   :f/v              0
                                   :id               "fluree:commit:sha256:biioetuzl6lopu2nef6bvrxdtlab456uqnklpdc54gw3wkny3sdh"}}]
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
                               :f/address        "fluree:memory://248b6ee27610662ce7bef7434b82dfec6822e4075f5cc546a45aae9575210ad9"
                               :f/alias          "committest"
                               :f/branch         "main"
                               :f/defaultContext {:id "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}
                               :f/data           #:f{:address  "fluree:memory://0a15a055fc33b54f7d9aec79ba7ff7353f468e6cd17a4617dc5ba96f536ac0f4"
                                                     :assert   [{:ex/x "foo-cat"
                                                                 :ex/y "bar-cat"
                                                                 :id   :ex/cat}]
                                                     :flakes   84
                                                     :previous {:id "fluree:db:sha256:bbexo3dfrnt6abi6kpkb27yvnzubxofrncwvqpglnotiu2reidgp5"}
                                                     :retract  []
                                                     :size     7552
                                                     :t        4}
                               :f/previous       {:id "fluree:commit:sha256:bb3dkz4x37v5s23c3u5o6lb3gm2vxj2jhyq3rgemtrhhr35ufaduw"}
                               :f/time           720000
                               :f/v              0
                               :id               "fluree:commit:sha256:biioetuzl6lopu2nef6bvrxdtlab456uqnklpdc54gw3wkny3sdh"}}
                   c4)))
          (is (= {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                             {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                             :f/address        "fluree:memory://eb82b75843135a0db10ba3d4113e3d2386f94d4f4f9dd3259f3fcec3a9f945e9"
                             :f/alias          "committest"
                             :f/branch         "main"
                             :f/defaultContext {:id "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}
                             :f/data           #:f{:address  "fluree:memory://e970082d077d51cbc40e7616537421aaeb0256a02b91ad316f057088ef9ab6ef"
                                                   :assert   [{:ex/x "foo-3"
                                                               :ex/y "bar-3"
                                                               :id   :ex/alice}]
                                                   :flakes   65
                                                   :previous {:id "fluree:db:sha256:bnuycsv7eakoeqwcc6qu6yktou3rqzgszs5tjnten5fylm2cinfy"}
                                                   :retract  [{:ex/x "foo-2"
                                                               :ex/y "bar-2"
                                                               :id   :ex/alice}]
                                                   :size     5976
                                                   :t        3}
                             :f/previous       {:id "fluree:commit:sha256:bbkgoyhm55amgqnxhcdv5fvap7z6cllkn55a3rc67zmgyacqnanpo"}
                             :f/time           720000
                             :f/v              0
                             :id               "fluree:commit:sha256:bb3dkz4x37v5s23c3u5o6lb3gm2vxj2jhyq3rgemtrhhr35ufaduw"}}
                 c3))
          (is (= {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                             {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                             :f/address        "fluree:memory://a5c01f9e88548aa840bf8202cc8524301349d3796eede2578299b64af9b22871"
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
                             :id               "fluree:commit:sha256:bbkgoyhm55amgqnxhcdv5fvap7z6cllkn55a3rc67zmgyacqnanpo"}}
                 c2))))

      (testing "time range from"
        (is (= [{:f/commit {"https://www.w3.org/2018/credentials#issuer"
                            {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                            :f/address        "fluree:memory://248b6ee27610662ce7bef7434b82dfec6822e4075f5cc546a45aae9575210ad9"
                            :f/alias          "committest"
                            :f/branch         "main"
                            :f/defaultContext {:id "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee",}
                            :f/data           #:f{:address  "fluree:memory://0a15a055fc33b54f7d9aec79ba7ff7353f468e6cd17a4617dc5ba96f536ac0f4"
                                                  :assert   [{:ex/x "foo-cat"
                                                              :ex/y "bar-cat"
                                                              :id   :ex/cat}]
                                                  :flakes   84
                                                  :previous {:id "fluree:db:sha256:bbexo3dfrnt6abi6kpkb27yvnzubxofrncwvqpglnotiu2reidgp5"}
                                                  :retract  []
                                                  :size     7552
                                                  :t        4}
                            :f/previous       {:id "fluree:commit:sha256:bb3dkz4x37v5s23c3u5o6lb3gm2vxj2jhyq3rgemtrhhr35ufaduw"}
                            :f/time           720000
                            :f/v              0
                            :id               "fluree:commit:sha256:biioetuzl6lopu2nef6bvrxdtlab456uqnklpdc54gw3wkny3sdh"}}
                {:f/commit {"https://www.w3.org/2018/credentials#issuer"
                            {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                            :f/address        "fluree:memory://63d517697b2e531bb62ac8b8a3eff5e0df137bc289ef473bac622cec0b7cd7f8"
                            :f/alias          "committest"
                            :f/branch         "main"
                            :f/defaultContext {:id "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee",}
                            :f/data           #:f{:address  "fluree:memory://b3eb249d58013e3af179a375ac88f52b2ffd6a887971c23ce8946680456fbc1b"
                                                  :assert   [{:ex/x "foo-cat"
                                                              :ex/y "bar-cat"
                                                              :id   :ex/alice}]
                                                  :flakes   104
                                                  :previous {:id "fluree:db:sha256:bbx44tyzom257xbvlpmzriwnkixuwtzaf7gzg5lebuxjarq3lmkji"}
                                                  :retract  [{:ex/x "foo-3"
                                                              :ex/y "bar-3"
                                                              :id   :ex/alice}]
                                                  :size     9138
                                                  :t        5}
                            :f/message        "meow"
                            :f/previous       {:id "fluree:commit:sha256:biioetuzl6lopu2nef6bvrxdtlab456uqnklpdc54gw3wkny3sdh"}
                            :f/time           720000
                            :f/v              0
                            :id               "fluree:commit:sha256:bn6hlpssv26lj3gbyvkowopjpmcwymwewtstsggy44i65xnnpn4y"}}]
               @(fluree/history ledger {:commit-details true :t {:from 4}}))))

      (testing "time range to"
        (is (= [{:f/commit {"https://www.w3.org/2018/credentials#issuer"
                            {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                            :f/address        "fluree:memory://45080eafa6185f468bbb811a570a9bb12449ac402f658f2b8c767f7dd30d0757"
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
                              :f/address        "fluree:memory://eb82b75843135a0db10ba3d4113e3d2386f94d4f4f9dd3259f3fcec3a9f945e9"
                              :f/alias          "committest"
                              :f/branch         "main"
                              :f/defaultContext {:id "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}
                              :f/data           #:f{:address  "fluree:memory://e970082d077d51cbc40e7616537421aaeb0256a02b91ad316f057088ef9ab6ef"
                                                    :assert   [{:ex/x "foo-3"
                                                                :ex/y "bar-3"
                                                                :id   :ex/alice}]
                                                    :flakes   65
                                                    :previous {:id "fluree:db:sha256:bnuycsv7eakoeqwcc6qu6yktou3rqzgszs5tjnten5fylm2cinfy"}
                                                    :retract  [{:ex/x "foo-2"
                                                                :ex/y "bar-2"
                                                                :id   :ex/alice}]
                                                    :size     5976
                                                    :t        3}
                              :f/previous       {:id "fluree:commit:sha256:bbkgoyhm55amgqnxhcdv5fvap7z6cllkn55a3rc67zmgyacqnanpo"}
                              :f/time           720000
                              :f/v              0
                              :id               "fluree:commit:sha256:bb3dkz4x37v5s23c3u5o6lb3gm2vxj2jhyq3rgemtrhhr35ufaduw"}
                    :retract [{:ex/x "foo-2"
                               :ex/y "bar-2"
                               :id   :ex/alice}]
                    :t       3}
                #:f{:assert  [{:ex/x "foo-cat"
                               :ex/y "bar-cat"
                               :id   :ex/alice}]
                    :commit  {"https://www.w3.org/2018/credentials#issuer"
                              {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                              :f/address        "fluree:memory://63d517697b2e531bb62ac8b8a3eff5e0df137bc289ef473bac622cec0b7cd7f8"
                              :f/alias          "committest"
                              :f/branch         "main"
                              :f/defaultContext {:id "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}
                              :f/data           #:f{:address  "fluree:memory://b3eb249d58013e3af179a375ac88f52b2ffd6a887971c23ce8946680456fbc1b"
                                                    :assert   [{:ex/x "foo-cat"
                                                                :ex/y "bar-cat"
                                                                :id   :ex/alice}]
                                                    :flakes   104
                                                    :previous {:id "fluree:db:sha256:bbx44tyzom257xbvlpmzriwnkixuwtzaf7gzg5lebuxjarq3lmkji"}
                                                    :retract  [{:ex/x "foo-3"
                                                                :ex/y "bar-3"
                                                                :id   :ex/alice}]
                                                    :size     9138
                                                    :t        5}
                              :f/message        "meow"
                              :f/previous       {:id "fluree:commit:sha256:biioetuzl6lopu2nef6bvrxdtlab456uqnklpdc54gw3wkny3sdh"}
                              :f/time           720000
                              :f/v              0
                              :id               "fluree:commit:sha256:bn6hlpssv26lj3gbyvkowopjpmcwymwewtstsggy44i65xnnpn4y"}
                    :retract [{:ex/x "foo-3"
                               :ex/y "bar-3"
                               :id   :ex/alice}]
                    :t       5}]
               @(fluree/history ledger {:history        :ex/alice
                                        :commit-details true
                                        :t              {:from 3}}))))
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
                        history-with-commits)))))))))

(deftest loaded-mem-ledger-history-test
  (with-redefs [fluree.db.util.core/current-time-iso (constantly "1970-01-01T00:12:00.00000Z")]
    (testing "history commit details on a loaded memory ledger"
      (let [ledger-name   "loaded-history-mem"
            conn          @(fluree/connect
                            {:method :memory
                             :defaults
                             {:context      test-utils/default-context
                              :context-type :keyword}})
            ledger        @(fluree/create conn ledger-name
                                          {:defaultContext
                                           ["" {:ex "http://example.org/ns/"}]})
            _             @(test-utils/transact ledger {:id   :ex/alice
                                                        :ex/x "foo-1"
                                                        :ex/y "bar-1"})
            _             @(test-utils/transact ledger {:id   :ex/alice
                                                        :ex/x "foo-2"
                                                        :ex/y "bar-2"})
            _             @(test-utils/transact ledger {:id   :ex/alice
                                                        :ex/x "foo-3"
                                                        :ex/y "bar-3"})
            _             @(test-utils/transact ledger {:id   :ex/cat
                                                        :ex/x "foo-cat"
                                                        :ex/y "bar-cat"})
            _             @(test-utils/transact ledger {:id   :ex/alice
                                                        :ex/x "foo-cat"
                                                        :ex/y "bar-cat"}
                                                {:message "meow"})
            loaded-ledger (test-utils/retry-load conn ledger-name 100)]
        (is (= [#:f{:assert  [{:ex/x "foo-3"
                               :ex/y "bar-3"
                               :id   :ex/alice}]
                    :commit  {:f/address        "fluree:memory://79fb3db274b3ac8b04df827f0a7897a9c899f175555081b9a8dc81bedc83491b"
                              :f/alias          ledger-name
                              :f/branch         "main"
                              :f/defaultContext {:id "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}
                              :f/data           #:f{:address  "fluree:memory://ca839a056f6d1e401c3dbfd2c97cd30d0f95b148596ebe7b06fc54c0ce76558b"
                                                    :assert   [{:ex/x "foo-3"
                                                                :ex/y "bar-3"
                                                                :id   :ex/alice}]
                                                    :flakes   62
                                                    :previous {:id "fluree:db:sha256:b7pvwgol6v6y7j2pak5ptdmt6l64esaguo3maevh346utpnj7jxh"}
                                                    :retract  [{:ex/x "foo-2"
                                                                :ex/y "bar-2"
                                                                :id   :ex/alice}]
                                                    :size     5772
                                                    :t        3}
                              :f/previous       {:id "fluree:commit:sha256:b45eltgynxrale2dfzktbnpgb7nzra5knnxq3ahqhq4x63xokbdc"}
                              :f/time           720000
                              :f/v              0
                              :id               "fluree:commit:sha256:bbpzonorhttcrdtfi2ud5ghxgh5p7ikoekq3u3biuarhug3lt4jb7"}
                    :retract [{:ex/x "foo-2"
                               :ex/y "bar-2"
                               :id   :ex/alice}]
                    :t       3}
                #:f{:assert  [{:ex/x "foo-cat"
                               :ex/y "bar-cat"
                               :id   :ex/alice}]
                    :commit  {:f/address        "fluree:memory://b3b6897ce701fa66ed4645d40877f1736ea6d8ab6478a8edc62b00c7ebdc1c50"
                              :f/alias          ledger-name
                              :f/branch         "main"
                              :f/defaultContext {:id "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}
                              :f/data           #:f{:address  "fluree:memory://776ab0c891828a05964716d3630c0fac8b43b9a3c98ea12343318b56931fb81d"
                                                    :assert   [{:ex/x "foo-cat"
                                                                :ex/y "bar-cat"
                                                                :id   :ex/alice}]
                                                    :flakes   99
                                                    :previous {:id "fluree:db:sha256:b7pvwgol6v6y7j2pak5ptdmt6l64esaguo3maevh346utpnj7jxh"}
                                                    :retract  [{:ex/x "foo-3"
                                                                :ex/y "bar-3"
                                                                :id   :ex/alice}]
                                                    :size     8864
                                                    :t        5}
                              :f/message        "meow"
                              :f/previous       {:id "fluree:commit:sha256:bk7i7k5cqddb4quw7ygn75kuprgq2birq4nx6v65dwm7mainv46l"}
                              :f/time           720000
                              :f/v              0
                              :id               "fluree:commit:sha256:brqtcmae4xpty2qtivx4a5csnf5qoqngb7rrgv6ydw6wm7lhjdun"}
                    :retract [{:ex/x "foo-3"
                               :ex/y "bar-3"
                               :id   :ex/alice}]
                    :t       5}]
               @(fluree/history loaded-ledger {:history        :ex/alice
                                               :commit-details true
                                               :t              {:from 3}})))))

    (testing "history commit details on a loaded memory ledger w/ issuer"
      (let [ledger-name   "loaded-history-mem-issuer"
            conn          @(fluree/connect
                            {:method :memory
                             :defaults
                             {:context      test-utils/default-context
                              :context-type :keyword
                              :did          (did/private->did-map
                                             test-utils/default-private-key)}})
            ledger        @(fluree/create conn ledger-name
                                          {:defaultContext
                                           ["" {:ex "http://example.org/ns/"}]})
            _             @(test-utils/transact ledger {:id   :ex/alice
                                                        :ex/x "foo-1"
                                                        :ex/y "bar-1"})
            _             @(test-utils/transact ledger {:id   :ex/alice
                                                        :ex/x "foo-2"
                                                        :ex/y "bar-2"})
            _             @(test-utils/transact ledger {:id   :ex/alice
                                                        :ex/x "foo-3"
                                                        :ex/y "bar-3"})
            _             @(test-utils/transact ledger {:id   :ex/cat
                                                        :ex/x "foo-cat"
                                                        :ex/y "bar-cat"})
            _             @(test-utils/transact ledger {:id   :ex/alice
                                                        :ex/x "foo-cat"
                                                        :ex/y "bar-cat"}
                                                {:message "meow"})
            loaded-ledger (test-utils/retry-load conn ledger-name 100)]
        (is (= [#:f{:assert  [{:ex/x "foo-3"
                               :ex/y "bar-3"
                               :id   :ex/alice}]
                    :commit  {"https://www.w3.org/2018/credentials#issuer"
                              {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                              :f/address        "fluree:memory://a2819e5d9465f67139009752db02680f77c314f180cd9a1d2864ece508531a3a"
                              :f/alias          ledger-name
                              :f/branch         "main"
                              :f/defaultContext {:id "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}
                              :f/data           #:f{:address  "fluree:memory://b5afa93eb5b3e164128a26f141be03599934408566a0ff23ed901eb5b5adcc56"
                                                    :assert   [{:ex/x "foo-3"
                                                                :ex/y "bar-3"
                                                                :id   :ex/alice}]
                                                    :flakes   65
                                                    :previous {:id "fluree:db:sha256:b2sfdas2an3z5u4zunpr77ok64nl4k25xhtc4l22ezgsjhrsipfo"}
                                                    :retract  [{:ex/x "foo-2"
                                                                :ex/y "bar-2"
                                                                :id   :ex/alice}]
                                                    :size     6034
                                                    :t        3}
                              :f/previous       {:id "fluree:commit:sha256:blew52zwjdyhqom6feg7cf2sc4nsv5cqam6azic2jhjvtl6xrw2t"}
                              :f/time           720000
                              :f/v              0
                              :id               "fluree:commit:sha256:bfghczklj2v5bba2zka2rxe2itjhri6xvwkxxqlj42bpx2o72t6c"}
                    :retract [{:ex/x "foo-2"
                               :ex/y "bar-2"
                               :id   :ex/alice}]
                    :t       3}
                #:f{:assert  [{:ex/x "foo-cat"
                               :ex/y "bar-cat"
                               :id   :ex/alice}]
                    :commit  {"https://www.w3.org/2018/credentials#issuer"
                              {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                              :f/address        "fluree:memory://d2d4c0d0f7383376ef8a28a41918bf0c5d0b70d30d7e989344dbb15b2975dc85"
                              :f/alias          ledger-name
                              :f/branch         "main"
                              :f/defaultContext {:id "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}
                              :f/data           #:f{:address  "fluree:memory://9f2ccfa86bfe998c7b4534d8e4ebb7ba11e60c50ce583e6524dd73db3c78dcf6"
                                                    :assert   [{:ex/x "foo-cat"
                                                                :ex/y "bar-cat"
                                                                :id   :ex/alice}]
                                                    :flakes   104
                                                    :previous {:id "fluree:db:sha256:b2sfdas2an3z5u4zunpr77ok64nl4k25xhtc4l22ezgsjhrsipfo"}
                                                    :retract  [{:ex/x "foo-3"
                                                                :ex/y "bar-3"
                                                                :id   :ex/alice}]
                                                    :size     9252
                                                    :t        5}
                              :f/message        "meow"
                              :f/previous       {:id "fluree:commit:sha256:btfmwl6q3hlfwiu376jw6v3mxriq3qbfpj2x32n6gwhv6zbengnt"}
                              :f/time           720000
                              :f/v              0
                              :id               "fluree:commit:sha256:bkxqf3zgxih4ykd5li323es4ghtwshumbn4wnwk6mqyuwap44hoe"}
                    :retract [{:ex/x "foo-3"
                               :ex/y "bar-3"
                               :id   :ex/alice}]
                    :t       5}]
               @(fluree/history loaded-ledger {:history        :ex/alice
                                               :commit-details true
                                               :t              {:from 3}})))))))

(deftest loaded-file-ledger-history-test
  (with-redefs [fluree.db.util.core/current-time-iso (constantly "1970-01-01T00:12:00.00000Z")]
    (testing "history commit details on a loaded file ledger"
      (with-tmp-dir storage-path
        (let [ledger-name   "loaded-history-file"
              conn          @(fluree/connect
                              {:method       :file
                               :storage-path storage-path
                               :defaults
                               {:context      test-utils/default-context
                                :context-type :keyword
                                :did          (did/private->did-map
                                               test-utils/default-private-key)}})
              ledger        @(fluree/create conn ledger-name
                                            {:defaultContext
                                             ["" {:ex "http://example.org/ns/"}]})
              _             @(test-utils/transact ledger {:id   :ex/alice
                                                          :ex/x "foo-1"
                                                          :ex/y "bar-1"})
              _             @(test-utils/transact ledger {:id   :ex/alice
                                                          :ex/x "foo-2"
                                                          :ex/y "bar-2"})
              _             @(test-utils/transact ledger {:id   :ex/alice
                                                          :ex/x "foo-3"
                                                          :ex/y "bar-3"})
              _             @(test-utils/transact ledger {:id   :ex/cat
                                                          :ex/x "foo-cat"
                                                          :ex/y "bar-cat"})
              _             @(test-utils/transact ledger {:id   :ex/alice
                                                          :ex/x "foo-cat"
                                                          :ex/y "bar-cat"}
                                                  {:message "meow"})
              loaded-ledger (test-utils/retry-load conn ledger-name 100)]
          (is (= [#:f{:assert  [{:ex/x "foo-3"
                                 :ex/y "bar-3"
                                 :id   :ex/alice}]
                      :commit  {"https://www.w3.org/2018/credentials#issuer"
                                {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                                :f/address        "fluree:file://loaded-history-file/main/commit/4aa2c05dfb15383c1985789bd2fc25a50c1d0ce43ef7604329f30fb75407fbd6.json"
                                :f/alias          ledger-name
                                :f/branch         "main"
                                :f/defaultContext {:id "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}
                                :f/data           #:f{:address  "fluree:file://loaded-history-file/main/commit/bc064cb4f2a50977b599a2efd2ad45a4a05782fd841d91148942ecf2bec57bc5.json"
                                                      :assert   [{:ex/x "foo-3"
                                                                  :ex/y "bar-3"
                                                                  :id   :ex/alice}]
                                                      :flakes   65
                                                      :previous {:id "fluree:db:sha256:bb2rkmq5eokfvv46sjjfwg6tifoyspl5kcmwx6skzpubxfvhgr37i"}
                                                      :retract  [{:ex/x "foo-2"
                                                                  :ex/y "bar-2"
                                                                  :id   :ex/alice}]
                                                      :size     6368
                                                      :t        3}
                                :f/previous       {:id "fluree:commit:sha256:bbv5nfjt45alrffunszqz2ztvn6hoq4nkulmggsqieryqvxryncss"}
                                :f/time           720000
                                :f/v              0
                                :id               "fluree:commit:sha256:b4sxvlmkflbevjtva3ytcfa4eght7ocnlocachek777g3srhnbbu"}
                      :retract [{:ex/x "foo-2"
                                 :ex/y "bar-2"
                                 :id   :ex/alice}]
                      :t       3}
                  #:f{:assert  [{:ex/x "foo-cat"
                                 :ex/y "bar-cat"
                                 :id   :ex/alice}]
                      :commit  {"https://www.w3.org/2018/credentials#issuer"
                                {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                                :f/address        "fluree:file://loaded-history-file/main/commit/f527d87439243b2c40fbc61af462d9e58d57865bf4dcc597c983ad723447de4d.json"
                                :f/alias          ledger-name
                                :f/branch         "main"
                                :f/defaultContext {:id "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}
                                :f/data           #:f{:address  "fluree:file://loaded-history-file/main/commit/0a48a60e06eb506999a1eac39f95d1cb2c1f19606894f415099c012413f52264.json"
                                                      :assert   [{:ex/x "foo-cat"
                                                                  :ex/y "bar-cat"
                                                                  :id   :ex/alice}]
                                                      :flakes   104
                                                      :previous {:id "fluree:db:sha256:bb2rkmq5eokfvv46sjjfwg6tifoyspl5kcmwx6skzpubxfvhgr37i"}
                                                      :retract  [{:ex/x "foo-3"
                                                                  :ex/y "bar-3"
                                                                  :id   :ex/alice}]
                                                      :size     9846
                                                      :t        5}
                                :f/message        "meow"
                                :f/previous       {:id "fluree:commit:sha256:bbri3m3j4whifm5t6dnyt2qio23ueczghvzmxjmosvauxcsyioykp"}
                                :f/time           720000
                                :f/v              0
                                :id               "fluree:commit:sha256:bb3ozqwbbsvtvhaeu2w5jtxmzrhyucmuk75ou2bhd5c3din727cub"}
                      :retract [{:ex/x "foo-3"
                                 :ex/y "bar-3"
                                 :id   :ex/alice}]
                      :t       5}]
                 @(fluree/history loaded-ledger {:history        :ex/alice
                                                 :commit-details true
                                                 :t              {:from 3}}))))))))
