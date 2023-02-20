(ns fluree.db.query.history-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.core :as util]
            [clojure.string :as str]))

(deftest ^:integration history-query
  (let [ts-primeval (util/current-time-iso)

        conn        (test-utils/create-conn)
        ledger      @(fluree/create conn "historytest" {:context {:ex "http://example.org/ns/"}})

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
            ledger @(fluree/create conn "historycachetest" {:context {:ex "http://example.org/ns/"}})

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
          ledger @(fluree/create conn "committest" {:context {:ex "http://example.org/ns/"}})

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
        (is (= [{:f/commit
                 {:f/address
                  "fluree:memory://3f7ff6df48e007cab36098274fd822ac11c9da4bf8b29762d1de3fdbdd6b6013",
                  :f/v      0,
                  :f/time   720000,
                  :id
                  "fluree:commit:sha256:bsso2btsgd4gsukmqrlmm4gap4gbmk5fkemiht6hlpjkaxzdgmmk",
                  :f/branch "main",
                  :f/data
                  {:f/address "fluree:memory://5d3ce686baa6fd5cc547b5e03e6aca3d92cbce0328c2320a49c514b01e58b4c2",
                   :f/flakes  11,
                   :f/size    996,
                   :f/t       1,
                   :f/assert  [{:ex/x "foo-1", :ex/y "bar-1", :id :ex/alice}],
                   :f/retract []},
                  "https://www.w3.org/2018/credentials#issuer"
                  {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"},
                  :f/alias  "committest",
                  :f/context
                  "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}}]
               @(fluree/history ledger {:commit-details true :t {:from 1 :to 1}})))
        (let [commit-5 {:f/commit
                        {:f/address
                         "fluree:memory://16d376f6cac29e8125ec3beca7aea6f75fb7a4328d73cbe0d629c6132510621c",
                         :f/v       0,
                         :f/previous
                         {:id
                          "fluree:commit:sha256:busykqnrg3i2zdhi7wvlbhretnv32e5cyxcqwvgjtazftkjnpup7"},
                         :f/time    720000,
                         :id
                         "fluree:commit:sha256:bbgkybvkkdwelzlmj6vei7kitm5hvxecotx6imytpta272jwbiaw7",
                         :f/branch  "main",
                         :f/message "meow",
                         :f/data
                         {:f/previous
                          {:id
                           "fluree:db:sha256:bbiioffkgezekkxzojudoqrtk3zmjcxeotecprh2ordafskng7hhc"},
                          :f/address
                          "fluree:memory://783086c375a712eba5d5a74b18882a98a1f77eccf51bddd9b99e2c625e67a088",
                          :f/flakes  102,
                          :f/size    9328,
                          :f/t       5,
                          :f/assert  [{:ex/x "foo-cat", :ex/y "bar-cat", :id :ex/alice}],
                          :f/retract [{:ex/x "foo-3", :ex/y "bar-3", :id :ex/alice}]},
                         "https://www.w3.org/2018/credentials#issuer"
                         {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"},
                         :f/alias   "committest",
                         :f/context
                         "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}}
              commit-4 {:f/commit
                        {:f/address
                         "fluree:memory://f0207c4d1d5b3e9abd71622f72149ec36518abda929aa63812c44d21a0b77b5a",
                         :f/v      0,
                         :f/previous
                         {:id
                          "fluree:commit:sha256:bdsvqh2dyqm7d4y3ou7jteevehp7t6wrfprdhjbuanx43bafyjgd"},
                         :f/time   720000,
                         :id
                         "fluree:commit:sha256:busykqnrg3i2zdhi7wvlbhretnv32e5cyxcqwvgjtazftkjnpup7",
                         :f/branch "main",
                         :f/data
                         {:f/previous
                          {:id
                           "fluree:db:sha256:bbc26gz36q2kxbrwmdo4ryfq4shvbw5b7aloxqsaje3tzd32issul"},
                          :f/address
                          "fluree:memory://c3a54a71c8c554c9d583df9e72a57bf572976e7a20c088f0cf536032cfd6d212",
                          :f/flakes  82,
                          :f/size    7590,
                          :f/t       4,
                          :f/assert  [{:ex/x "foo-cat", :ex/y "bar-cat", :id :ex/cat}],
                          :f/retract []},
                         "https://www.w3.org/2018/credentials#issuer"
                         {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"},
                         :f/alias  "committest",
                         :f/context
                         "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}}]
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
            (is (= {:f/commit
                    {:f/address
                     "fluree:memory://f0207c4d1d5b3e9abd71622f72149ec36518abda929aa63812c44d21a0b77b5a",
                     :f/v      0,
                     :f/previous
                     {:id
                      "fluree:commit:sha256:bdsvqh2dyqm7d4y3ou7jteevehp7t6wrfprdhjbuanx43bafyjgd"},
                     :f/time   720000,
                     :id
                     "fluree:commit:sha256:busykqnrg3i2zdhi7wvlbhretnv32e5cyxcqwvgjtazftkjnpup7",
                     :f/branch "main",
                     :f/data
                     {:f/previous
                      {:id
                       "fluree:db:sha256:bbc26gz36q2kxbrwmdo4ryfq4shvbw5b7aloxqsaje3tzd32issul"},
                      :f/address
                      "fluree:memory://c3a54a71c8c554c9d583df9e72a57bf572976e7a20c088f0cf536032cfd6d212",
                      :f/flakes  82,
                      :f/size    7590,
                      :f/t       4,
                      :f/assert  [{:ex/x "foo-cat", :ex/y "bar-cat", :id :ex/cat}],
                      :f/retract []},
                     "https://www.w3.org/2018/credentials#issuer"
                     {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"},
                     :f/alias  "committest",
                     :f/context
                     "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}}
                   c4)))
          (is (= {:f/commit
                  {:f/address
                   "fluree:memory://3529d447998ec5b2c42ed1336009fdab9388bbd987fade075f1208aa8b7ec44b",
                   :f/v      0,
                   :f/previous
                   {:id
                    "fluree:commit:sha256:bb42ijix6d6tidq4zzc75uymowpx7hazczwhxkuz2u45c2gcwtwvw"},
                   :f/time   720000,
                   :id
                   "fluree:commit:sha256:bdsvqh2dyqm7d4y3ou7jteevehp7t6wrfprdhjbuanx43bafyjgd",
                   :f/branch "main",
                   :f/data
                   {:f/previous
                    {:id
                     "fluree:db:sha256:bba364nb2cbanjsgwk7t7l34m5f667efop4yiu5l5lvxscoggqdgg"},
                    :f/address
                    "fluree:memory://dc0c296d3047963ff807d0f17807c21a582d89ae72faa3d1d3c13d7af3929d22",
                    :f/flakes  63,
                    :f/size    5864,
                    :f/t       3,
                    :f/assert  [{:ex/x "foo-3", :ex/y "bar-3", :id :ex/alice}],
                    :f/retract [{:ex/x "foo-2", :ex/y "bar-2", :id :ex/alice}]},
                   "https://www.w3.org/2018/credentials#issuer"
                   {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"},
                   :f/alias  "committest",
                   :f/context
                   "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}}
                 c3))
          (is (= {:f/commit
                  {:f/address
                   "fluree:memory://08ef0ead00f983ec19d1cabbaa991fe2beba70a8666bda2df6685654a646b332",
                   :f/v      0,
                   :f/previous
                   {:id
                    "fluree:commit:sha256:bsso2btsgd4gsukmqrlmm4gap4gbmk5fkemiht6hlpjkaxzdgmmk"},
                   :f/time   720000,
                   :id
                   "fluree:commit:sha256:bb42ijix6d6tidq4zzc75uymowpx7hazczwhxkuz2u45c2gcwtwvw",
                   :f/branch "main",
                   :f/data
                   {:f/previous
                    {:id
                     "fluree:db:sha256:bwogmajjh3rwlkijfihbcdy4h52qjv4kumctu4mhy27pj3zsxtes"},
                    :f/address
                    "fluree:memory://cffe310ce5b609c281342f812771e204e8789d5b9c6f15dcd19a7e74427d0413",
                    :f/flakes  43,
                    :f/size    4132,
                    :f/t       2,
                    :f/assert  [{:ex/x "foo-2", :ex/y "bar-2", :id :ex/alice}],
                    :f/retract [{:ex/x "foo-1", :ex/y "bar-1", :id :ex/alice}]},
                   "https://www.w3.org/2018/credentials#issuer"
                   {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"},
                   :f/alias  "committest",
                   :f/context
                   "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}}
                 c2))))

      (testing "time range from"
        (is (= [{:f/commit
                 {:f/address
                  "fluree:memory://f0207c4d1d5b3e9abd71622f72149ec36518abda929aa63812c44d21a0b77b5a",
                  :f/v      0,
                  :f/previous
                  {:id
                   "fluree:commit:sha256:bdsvqh2dyqm7d4y3ou7jteevehp7t6wrfprdhjbuanx43bafyjgd"},
                  :f/time   720000,
                  :id
                  "fluree:commit:sha256:busykqnrg3i2zdhi7wvlbhretnv32e5cyxcqwvgjtazftkjnpup7",
                  :f/branch "main",
                  :f/data
                  {:f/previous
                   {:id
                    "fluree:db:sha256:bbc26gz36q2kxbrwmdo4ryfq4shvbw5b7aloxqsaje3tzd32issul"},
                   :f/address
                   "fluree:memory://c3a54a71c8c554c9d583df9e72a57bf572976e7a20c088f0cf536032cfd6d212",
                   :f/flakes  82,
                   :f/size    7590,
                   :f/t       4,
                   :f/assert  [{:ex/x "foo-cat", :ex/y "bar-cat", :id :ex/cat}],
                   :f/retract []},
                  "https://www.w3.org/2018/credentials#issuer"
                  {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"},
                  :f/alias  "committest",
                  :f/context
                  "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}}
                {:f/commit
                 {:f/address
                  "fluree:memory://16d376f6cac29e8125ec3beca7aea6f75fb7a4328d73cbe0d629c6132510621c",
                  :f/v       0,
                  :f/previous
                  {:id
                   "fluree:commit:sha256:busykqnrg3i2zdhi7wvlbhretnv32e5cyxcqwvgjtazftkjnpup7"},
                  :f/time    720000,
                  :id
                  "fluree:commit:sha256:bbgkybvkkdwelzlmj6vei7kitm5hvxecotx6imytpta272jwbiaw7",
                  :f/branch  "main",
                  :f/message "meow",
                  :f/data
                  {:f/previous
                   {:id
                    "fluree:db:sha256:bbiioffkgezekkxzojudoqrtk3zmjcxeotecprh2ordafskng7hhc"},
                   :f/address
                   "fluree:memory://783086c375a712eba5d5a74b18882a98a1f77eccf51bddd9b99e2c625e67a088",
                   :f/flakes  102,
                   :f/size    9328,
                   :f/t       5,
                   :f/assert  [{:ex/x "foo-cat", :ex/y "bar-cat", :id :ex/alice}],
                   :f/retract [{:ex/x "foo-3", :ex/y "bar-3", :id :ex/alice}]},
                  "https://www.w3.org/2018/credentials#issuer"
                  {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"},
                  :f/alias   "committest",
                  :f/context
                  "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}}]
               @(fluree/history ledger {:commit-details true :t {:from 4}}))))

      (testing "time range to"
        (is (= [{:f/commit
                 {:f/address
                  "fluree:memory://3f7ff6df48e007cab36098274fd822ac11c9da4bf8b29762d1de3fdbdd6b6013",
                  :f/v      0,
                  :f/time   720000,
                  :id
                  "fluree:commit:sha256:bsso2btsgd4gsukmqrlmm4gap4gbmk5fkemiht6hlpjkaxzdgmmk",
                  :f/branch "main",
                  :f/data
                  {:f/address
                   "fluree:memory://5d3ce686baa6fd5cc547b5e03e6aca3d92cbce0328c2320a49c514b01e58b4c2",
                   :f/flakes  11,
                   :f/size    996,
                   :f/t       1,
                   :f/assert  [{:ex/x "foo-1", :ex/y "bar-1", :id :ex/alice}],
                   :f/retract []},
                  "https://www.w3.org/2018/credentials#issuer"
                  {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"},
                  :f/alias  "committest",
                  :f/context
                  "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}}]
               @(fluree/history ledger {:commit-details true :t {:to 1}}))))

      (testing "history commit details"
        (is (= [{:f/t       3,
                 :f/assert  [{:ex/x "foo-3", :ex/y "bar-3", :id :ex/alice}],
                 :f/retract [{:ex/x "foo-2", :ex/y "bar-2", :id :ex/alice}],
                 :f/commit
                 {:f/address
                  "fluree:memory://3529d447998ec5b2c42ed1336009fdab9388bbd987fade075f1208aa8b7ec44b",
                  :f/v      0,
                  :f/previous
                  {:id
                   "fluree:commit:sha256:bb42ijix6d6tidq4zzc75uymowpx7hazczwhxkuz2u45c2gcwtwvw"},
                  :f/time   720000,
                  :id
                  "fluree:commit:sha256:bdsvqh2dyqm7d4y3ou7jteevehp7t6wrfprdhjbuanx43bafyjgd",
                  :f/branch "main",
                  :f/data
                  {:f/previous
                   {:id
                    "fluree:db:sha256:bba364nb2cbanjsgwk7t7l34m5f667efop4yiu5l5lvxscoggqdgg"},
                   :f/address
                   "fluree:memory://dc0c296d3047963ff807d0f17807c21a582d89ae72faa3d1d3c13d7af3929d22",
                   :f/flakes  63,
                   :f/size    5864,
                   :f/t       3,
                   :f/assert  [{:ex/x "foo-3", :ex/y "bar-3", :id :ex/alice}],
                   :f/retract [{:ex/x "foo-2", :ex/y "bar-2", :id :ex/alice}]},
                  "https://www.w3.org/2018/credentials#issuer"
                  {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"},
                  :f/alias  "committest",
                  :f/context
                  "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}}
                {:f/t       5,
                 :f/assert  [{:ex/x "foo-cat", :ex/y "bar-cat", :id :ex/alice}],
                 :f/commit
                 {:f/address
                  "fluree:memory://16d376f6cac29e8125ec3beca7aea6f75fb7a4328d73cbe0d629c6132510621c",
                  :f/v       0,
                  :f/previous
                  {:id
                   "fluree:commit:sha256:busykqnrg3i2zdhi7wvlbhretnv32e5cyxcqwvgjtazftkjnpup7"},
                  :f/time    720000,
                  :id
                  "fluree:commit:sha256:bbgkybvkkdwelzlmj6vei7kitm5hvxecotx6imytpta272jwbiaw7",
                  :f/branch  "main",
                  :f/message "meow",
                  :f/data
                  {:f/previous
                   {:id
                    "fluree:db:sha256:bbiioffkgezekkxzojudoqrtk3zmjcxeotecprh2ordafskng7hhc"},
                   :f/address
                   "fluree:memory://783086c375a712eba5d5a74b18882a98a1f77eccf51bddd9b99e2c625e67a088",
                   :f/flakes  102,
                   :f/size    9328,
                   :f/t       5,
                   :f/assert  [{:ex/x "foo-cat", :ex/y "bar-cat", :id :ex/alice}],
                   :f/retract [{:ex/x "foo-3", :ex/y "bar-3", :id :ex/alice}]},
                  "https://www.w3.org/2018/credentials#issuer"
                  {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"},
                  :f/alias   "committest",
                  :f/context
                  "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"},
                 :f/retract [{:ex/x "foo-3", :ex/y "bar-3", :id :ex/alice}]}]
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
