(ns fluree.db.query.history-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.core :as util]
            [clojure.string :as str]))

(deftest ^:integration history-query
  (let [ts-primeval (util/current-time-iso)

        conn (test-utils/create-conn)
        ledger @(fluree/create conn "historytest" {:context {:ex "http://example.org/ns/"}})

        db1 @(test-utils/transact ledger [{:id :ex/dan
                                           :ex/x "foo-1"
                                           :ex/y "bar-1"}
                                          {:id :ex/cat
                                           :ex/x "foo-1"
                                           :ex/y "bar-1"}
                                          {:id :ex/dog
                                           :ex/x "foo-1"
                                           :ex/y "bar-1"}])
        db2 @(test-utils/transact ledger {:id :ex/dan
                                          :ex/x "foo-2"
                                          :ex/y "bar-2"})
        ts2 (-> db2 :commit :time)
        db3 @(test-utils/transact ledger {:id :ex/dan
                                          :ex/x "foo-3"
                                          :ex/y "bar-3"})

        ts3 (-> db3 :commit :time)
        db4 @(test-utils/transact ledger [{:id :ex/cat
                                           :ex/x "foo-cat"
                                           :ex/y "bar-cat"}
                                          {:id :ex/dog
                                           :ex/x "foo-dog"
                                           :ex/y "bar-dog"}])
        db5 @(test-utils/transact ledger {:id :ex/dan
                                          :ex/x "foo-cat"
                                          :ex/y "bar-cat"})]
    (testing "subject history"
      (is (= [{:f/t 1
               :f/assert [{:id :ex/dan :ex/x "foo-1" :ex/y "bar-1"}]
               :f/retract []}
              {:f/t 2
               :f/assert [{:id :ex/dan :ex/x "foo-2" :ex/y "bar-2"}]
               :f/retract [{:id :ex/dan :ex/x "foo-1" :ex/y "bar-1"}]}
              {:f/t 3
               :f/assert [{:id :ex/dan :ex/x "foo-3" :ex/y "bar-3"}]
               :f/retract [{:id :ex/dan :ex/x "foo-2" :ex/y "bar-2"}]}
              {:f/t 5
               :f/assert [{:id :ex/dan :ex/x "foo-cat" :ex/y "bar-cat"}]
               :f/retract [{:id :ex/dan :ex/x "foo-3" :ex/y "bar-3"}]}]
             @(fluree/history ledger {:history :ex/dan :t {:from 1}}))))
    (testing "one-tuple flake history"
      (is (= [{:f/t 1
               :f/assert [{:id :ex/dan :ex/x "foo-1" :ex/y "bar-1"}]
               :f/retract []}
              {:f/t 2
               :f/assert [{:ex/x "foo-2" :ex/y "bar-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :ex/y "bar-1" :id :ex/dan}]}
              {:f/t 3
               :f/assert [{:ex/x "foo-3" :ex/y "bar-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :ex/y "bar-2" :id :ex/dan}]}
              {:f/t 5
               :f/assert [{:ex/x "foo-cat" :ex/y "bar-cat" :id :ex/dan}]
               :f/retract [{:ex/x "foo-3" :ex/y "bar-3" :id :ex/dan}]}]
             @(fluree/history ledger {:history [:ex/dan] :t {:from 1}}))))
    (testing "two-tuple flake history"
      (is (= [{:f/t 1 :f/assert [{:ex/x "foo-1" :id :ex/dan}] :f/retract []}
              {:f/t 2
               :f/assert [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :id :ex/dan}]}
              {:f/t 3
               :f/assert [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}
              {:f/t 5
               :f/assert [{:ex/x "foo-cat" :id :ex/dan}]
               :f/retract [{:ex/x "foo-3" :id :ex/dan}]}]
             @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:from 1}})))

      (is (= [{:f/t 1 :f/assert [{:ex/x "foo-1" :id :ex/dog}
                                 {:ex/x "foo-1" :id :ex/cat}
                                 {:ex/x "foo-1" :id :ex/dan}]
               :f/retract []}
              {:f/t 2
               :f/assert [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :id :ex/dan}]}
              {:f/t 3
               :f/assert [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}
              {:f/t 4
               :f/assert [{:ex/x "foo-dog" :id :ex/dog}
                          {:ex/x "foo-cat" :id :ex/cat}]
               :f/retract [{:ex/x "foo-1" :id :ex/dog}
                           {:ex/x "foo-1" :id :ex/cat}]}
              {:f/t 5
               :f/assert [{:ex/x "foo-cat" :id :ex/dan}]
               :f/retract [{:ex/x "foo-3" :id :ex/dan}]}]
             @(fluree/history ledger {:history [nil :ex/x] :t {:from 1}}))))
    (testing "three-tuple flake history"
      (is (= [{:f/t 4 :f/assert [{:ex/x "foo-cat" :id :ex/cat}] :f/retract []}
              {:f/t 5 :f/assert [{:ex/x "foo-cat" :id :ex/dan}] :f/retract []}]
             @(fluree/history ledger {:history [nil :ex/x "foo-cat"] :t {:from 1}})))
      (is (= [{:f/t 2
               :f/assert [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract []}
              {:f/t 3
               :f/assert []
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}]
             @(fluree/history ledger {:history [nil :ex/x "foo-2"] :t {:from 1}})))
      (is (= [{:f/t 5 :f/assert [{:ex/x "foo-cat" :id :ex/dan}] :f/retract []}]
             @(fluree/history ledger {:history [:ex/dan :ex/x "foo-cat"] :t {:from 1}}))))

    (testing "at-t"
      (let [expected [{:f/t 3
                       :f/assert [{:ex/x "foo-3" :id :ex/dan}]
                       :f/retract [{:ex/x "foo-2" :id :ex/dan}]}]]
        (is (= expected
               @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:from 3 :to 3}})))
        (is (= expected
               @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:at 3}})))))
    (testing "from-t"
      (is (= [{:f/t 3
               :f/assert [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}
              {:f/t 5
               :f/assert [{:ex/x "foo-cat" :id :ex/dan}]
               :f/retract [{:ex/x "foo-3" :id :ex/dan}]}]
             @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:from 3}}))))
    (testing "to-t"
      (is (= [{:f/t 1
               :f/assert [{:ex/x "foo-1" :id :ex/dan}]
               :f/retract []}
              {:f/t 2
               :f/assert [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :id :ex/dan}]}
              {:f/t 3
               :f/assert [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}]
             @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:to 3}}))))
    (testing "t-range"
      (is (= [{:f/t 2
               :f/assert [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :id :ex/dan}]}
              {:f/t 3
               :f/assert [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}
              {:f/t 4
               :f/assert [{:ex/x "foo-dog" :id :ex/dog} {:ex/x "foo-cat" :id :ex/cat}]
               :f/retract [{:ex/x "foo-1" :id :ex/dog} {:ex/x "foo-1" :id :ex/cat}]}]
             @(fluree/history ledger {:history [nil :ex/x] :t {:from 2 :to 4}}))))
    (testing "datetime-t"
      (is (= [{:f/t 2
               :f/assert [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :id :ex/dan}]}
              {:f/t 3
               :f/assert [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}]
             @(fluree/history ledger {:history [nil :ex/x] :t {:from ts2 :to ts3}}))
          "does not include t 1 4 or 5")
      (is (= [{:f/t 5
               :f/assert [{:ex/x "foo-cat" :id :ex/dan}]
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
      (let [conn (test-utils/create-conn)
            ledger @(fluree/create conn "historycachetest" {:context {:ex "http://example.org/ns/"}})

            db1 @(test-utils/transact ledger [{:id :ex/dan
                                               :ex/x "foo-1"
                                               :ex/y "bar-1"}])
            db2 @(test-utils/transact ledger {:id :ex/dan
                                              :ex/x "foo-2"
                                              :ex/y "bar-2"})]
        (testing "no t-range cache collision"
          (is (= [{:f/t 2
                   :f/assert [{:ex/x "foo-2" :ex/y "bar-2" :id :ex/dan}]
                   :f/retract [{:ex/x "foo-1" :ex/y "bar-1" :id :ex/dan}]}]
                 @(fluree/history ledger {:history [:ex/dan] :t {:from 2}}))))))))

(deftest ^:integration commit-details
  (with-redefs [fluree.db.util.core/current-time-iso (fn [] "1970-01-01T00:12:00.00000Z")]
    (let [conn (test-utils/create-conn)
          ledger @(fluree/create conn "committest" {:context {:ex "http://example.org/ns/"}})

          db1 @(test-utils/transact ledger {:id :ex/alice
                                            :ex/x "foo-1"
                                            :ex/y "bar-1"})
          db2 @(test-utils/transact ledger {:id :ex/alice
                                            :ex/x "foo-2"
                                            :ex/y "bar-2"})
          db3 @(test-utils/transact ledger {:id :ex/alice
                                            :ex/x "foo-3"
                                            :ex/y "bar-3"})
          db4 @(test-utils/transact ledger {:id :ex/cat
                                            :ex/x "foo-cat"
                                            :ex/y "bar-cat"})
          db5 @(test-utils/transact ledger {:id :ex/alice
                                            :ex/x "foo-cat"
                                            :ex/y "bar-cat"}
                                    {:message "meow"})]
      (testing "at time t"
        (is (= [{:f/commit
                 {:f/address
                  "fluree:memory://2a2436a01df3343870bca46e3a24c6b57df73f28666fe5247c221ca888abba5e",
                  :f/v 0,
                  :f/time 720000,
                  :id
                  "fluree:commit:sha256:bb7lpextw2b64rq2k3ilcttpb66he3derz4cvnqtu5znptvzfndev",
                  :f/branch "main",
                  :f/data
                  {:f/address "fluree:memory://5d3ce686baa6fd5cc547b5e03e6aca3d92cbce0328c2320a49c514b01e58b4c2",
                   :f/flakes 11,
                   :f/size 996,
                   :f/t 1,
                   :f/assert [{:ex/x "foo-1", :ex/y "bar-1", :id :ex/alice}],
                   :f/retract []},
                  "https://www.w3.org/2018/credentials#issuer"
                  {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"},
                  :f/alias "committest",
                  :f/context
                  "fluree:memory:///contexts/b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}}]
               @(fluree/history ledger {:commit-details true :t {:from 1 :to 1}})))
        (let [commit-5 {:f/commit
                        {:f/address
                         "fluree:memory://14efe5ef1163589f9ee6fcc5af8e2830c3300a80abbbae0aa82194083e93aebe",
                         :f/v 0,
                         :f/previous
                         {:id
                          "fluree:commit:sha256:bbr54svhy4ergg3mmed4eugzljonsl7jlfmadsbcq6b7sr2cs7yyl"},
                         :f/time 720000,
                         :id
                         "fluree:commit:sha256:bb3v3rd7q6lojz5w4gsweglna6afhc6ooh5fgfdhxitdtdwquxwgv",
                         :f/branch "main",
                         :f/message "meow",
                         :f/data
                         {:f/previous
                          {:id
                           "fluree:db:sha256:bt6d6eup2oo2icrjj7ejcmfoa3wsen45y2l2iomlnbqsuoypizgn"},
                          :f/address
                          "fluree:memory://a505e939983b4cfa9325ebe0bbd8615865606c3f4ab9343dd00d594ee722cc10",
                          :f/flakes 102,
                          :f/size 9408,
                          :f/t 5,
                          :f/assert [{:ex/x "foo-cat", :ex/y "bar-cat", :id :ex/alice}],
                          :f/retract [{:ex/x "foo-3", :ex/y "bar-3", :id :ex/alice}]},
                         "https://www.w3.org/2018/credentials#issuer"
                         {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"},
                         :f/alias "committest",
                         :f/context
                         "fluree:memory:///contexts/b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}}
              commit-4 {:f/commit
                        {:f/address
                         "fluree:memory://67df776dd7cee2a21b9c2a513224ad044e2dc613e5be5d6de42478ed69ac7b21",
                         :f/v 0,
                         :f/previous
                         {:id
                          "fluree:commit:sha256:bzlpatyzpsmdyvr4eywvsa2bctleol5tbupsr35ejxntxnjn7l2q"},
                         :f/time 720000,
                         :id
                         "fluree:commit:sha256:bbr54svhy4ergg3mmed4eugzljonsl7jlfmadsbcq6b7sr2cs7yyl",
                         :f/branch "main",
                         :f/data
                         {:f/previous
                          {:id
                           "fluree:db:sha256:blxs4kpvkuwsc76cf2hfgs6housqeoxwvbehect4p47f6ow7fkbb"},
                          :f/address
                          "fluree:memory://85e076bdfe8e5c8d53551d290f9e4e716eab86ba8577238d731c51df4f6effba",
                          :f/flakes 82,
                          :f/size 7650,
                          :f/t 4,
                          :f/assert [{:ex/x "foo-cat", :ex/y "bar-cat", :id :ex/cat}],
                          :f/retract []},
                         "https://www.w3.org/2018/credentials#issuer"
                         {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"},
                         :f/alias "committest",
                         :f/context
                         "fluree:memory:///contexts/b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}}]
          (is (= [commit-4 commit-5]
                 @(fluree/history ledger {:commit-details true :t {:from 4 :to 5}})))
          (is (= [commit-5]
                 @(fluree/history ledger {:commit-details true :t {:at :latest}})))))

      (testing "time range"
        (let [[c2 c3 c4 :as response] @(fluree/history ledger {:commit-details true :t {:from 2 :to 4}})]
          (testing "all commits in time range are returned"
            (is (=  3
                    (count response))))
          (is (=  {:f/commit
                   {:f/address
                    "fluree:memory://67df776dd7cee2a21b9c2a513224ad044e2dc613e5be5d6de42478ed69ac7b21",
                    :f/v 0,
                    :f/previous
                    {:id
                     "fluree:commit:sha256:bzlpatyzpsmdyvr4eywvsa2bctleol5tbupsr35ejxntxnjn7l2q"},
                    :f/time 720000,
                    :id
                    "fluree:commit:sha256:bbr54svhy4ergg3mmed4eugzljonsl7jlfmadsbcq6b7sr2cs7yyl",
                    :f/branch "main",
                    :f/data
                    {:f/previous
                     {:id
                      "fluree:db:sha256:blxs4kpvkuwsc76cf2hfgs6housqeoxwvbehect4p47f6ow7fkbb"},
                     :f/address
                     "fluree:memory://85e076bdfe8e5c8d53551d290f9e4e716eab86ba8577238d731c51df4f6effba",
                     :f/flakes 82,
                     :f/size 7650,
                     :f/t 4,
                     :f/assert [{:ex/x "foo-cat", :ex/y "bar-cat", :id :ex/cat}],
                     :f/retract []},
                    "https://www.w3.org/2018/credentials#issuer"
                    {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"},
                    :f/alias "committest",
                    :f/context
                    "fluree:memory:///contexts/b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}}
                  c4))
          (is (= {:f/commit
                  {:f/address
                   "fluree:memory://c5aeefb071f0c42cdbc64ff531b54137395e074880ff153b8bd8712004c6554a",
                   :f/v 0,
                   :f/previous
                   {:id
                    "fluree:commit:sha256:bbn7ggrkec2gqgkdjh4qisbeklzgx6g5xdxcfepi3mbkresckyncp"},
                   :f/time 720000,
                   :id
                   "fluree:commit:sha256:bzlpatyzpsmdyvr4eywvsa2bctleol5tbupsr35ejxntxnjn7l2q",
                   :f/branch "main",
                   :f/data
                   {:f/previous
                    {:id
                     "fluree:db:sha256:bb3hzx3ibkdumy2qf7qzduwfqsxumucjwc334palggyg6qxhfdszh"},
                    :f/address
                    "fluree:memory://ec0a4c9ec4ce5597425ed8fbfb6fc518918b92b5ec69e3c3c4b05d17f0965590",
                    :f/flakes 63,
                    :f/size 5906,
                    :f/t 3,
                    :f/assert [{:ex/x "foo-3", :ex/y "bar-3", :id :ex/alice}],
                    :f/retract [{:ex/x "foo-2", :ex/y "bar-2", :id :ex/alice}]},
                   "https://www.w3.org/2018/credentials#issuer"
                   {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"},
                   :f/alias "committest",
                   :f/context
                   "fluree:memory:///contexts/b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}}
                 c3))
          (is (= {:f/commit
                  {:f/address
                   "fluree:memory://312ac51a0f0fa28445c482da8f6641f461972d205473bd7cb47804041d66d514",
                   :f/v 0,
                   :f/previous
                   {:id
                    "fluree:commit:sha256:bb7lpextw2b64rq2k3ilcttpb66he3derz4cvnqtu5znptvzfndev"},
                   :f/time 720000,
                   :id
                   "fluree:commit:sha256:bbn7ggrkec2gqgkdjh4qisbeklzgx6g5xdxcfepi3mbkresckyncp",
                   :f/branch "main",
                   :f/data
                   {:f/previous
                    {:id
                     "fluree:db:sha256:bwogmajjh3rwlkijfihbcdy4h52qjv4kumctu4mhy27pj3zsxtes"},
                    :f/address
                    "fluree:memory://0a0e8230fe970a87d930511961b05e9e9ab284903ce16967666e7883f04a7b05",
                    :f/flakes 43,
                    :f/size 4154,
                    :f/t 2,
                    :f/assert [{:ex/x "foo-2", :ex/y "bar-2", :id :ex/alice}],
                    :f/retract [{:ex/x "foo-1", :ex/y "bar-1", :id :ex/alice}]},
                   "https://www.w3.org/2018/credentials#issuer"
                   {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"},
                   :f/alias "committest",
                   :f/context
                   "fluree:memory:///contexts/b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}}
                 c2))))

      (testing "time range from"
        (is (= [{:f/commit
                 {:f/address
                  "fluree:memory://14efe5ef1163589f9ee6fcc5af8e2830c3300a80abbbae0aa82194083e93aebe",
                  :f/v 0,
                  :f/previous
                  {:id
                   "fluree:commit:sha256:bbr54svhy4ergg3mmed4eugzljonsl7jlfmadsbcq6b7sr2cs7yyl"},
                  :f/time 720000,
                  :id
                  "fluree:commit:sha256:bb3v3rd7q6lojz5w4gsweglna6afhc6ooh5fgfdhxitdtdwquxwgv",
                  :f/branch "main",
                  :f/message "meow",
                  :f/data
                  {:f/previous
                   {:id
                    "fluree:db:sha256:bt6d6eup2oo2icrjj7ejcmfoa3wsen45y2l2iomlnbqsuoypizgn"},
                   :f/address
                   "fluree:memory://a505e939983b4cfa9325ebe0bbd8615865606c3f4ab9343dd00d594ee722cc10",
                   :f/flakes 102,
                   :f/size 9408,
                   :f/t 5,
                   :f/assert [{:ex/x "foo-cat", :ex/y "bar-cat", :id :ex/alice}],
                   :f/retract [{:ex/x "foo-3", :ex/y "bar-3", :id :ex/alice}]},
                  "https://www.w3.org/2018/credentials#issuer"
                  {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"},
                  :f/alias "committest",
                  :f/context
                  "fluree:memory:///contexts/b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}}]
               @(fluree/history ledger {:commit-details true :t {:from 5}}))))

      (testing "time range to"
        (is (= [{:f/commit
                 {:f/address
                  "fluree:memory://2a2436a01df3343870bca46e3a24c6b57df73f28666fe5247c221ca888abba5e",
                  :f/v 0,
                  :f/time 720000,
                  :id
                  "fluree:commit:sha256:bb7lpextw2b64rq2k3ilcttpb66he3derz4cvnqtu5znptvzfndev",
                  :f/branch "main",
                  :f/data
                  {:f/address
                   "fluree:memory://5d3ce686baa6fd5cc547b5e03e6aca3d92cbce0328c2320a49c514b01e58b4c2",
                   :f/flakes 11,
                   :f/size 996,
                   :f/t 1,
                   :f/assert [{:ex/x "foo-1", :ex/y "bar-1", :id :ex/alice}],
                   :f/retract []},
                  "https://www.w3.org/2018/credentials#issuer"
                  {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"},
                  :f/alias "committest",
                  :f/context
                  "fluree:memory:///contexts/b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}}]
               @(fluree/history ledger {:commit-details true :t {:to 1}}))))

      (testing "history commit details"
        (is (= [{:f/t 3,
                 :f/assert [{:ex/x "foo-3", :ex/y "bar-3", :id :ex/alice}],
                 :f/retract [{:ex/x "foo-2", :ex/y "bar-2", :id :ex/alice}],
                 :f/commit
                 {:f/address
                  "fluree:memory://c5aeefb071f0c42cdbc64ff531b54137395e074880ff153b8bd8712004c6554a",
                  :f/v 0,
                  :f/previous
                  {:id
                   "fluree:commit:sha256:bbn7ggrkec2gqgkdjh4qisbeklzgx6g5xdxcfepi3mbkresckyncp"},
                  :f/time 720000,
                  :id
                  "fluree:commit:sha256:bzlpatyzpsmdyvr4eywvsa2bctleol5tbupsr35ejxntxnjn7l2q",
                  :f/branch "main",
                  :f/data
                  {:f/previous
                   {:id
                    "fluree:db:sha256:bb3hzx3ibkdumy2qf7qzduwfqsxumucjwc334palggyg6qxhfdszh"},
                   :f/address
                   "fluree:memory://ec0a4c9ec4ce5597425ed8fbfb6fc518918b92b5ec69e3c3c4b05d17f0965590",
                   :f/flakes 63,
                   :f/size 5906,
                   :f/t 3,
                   :f/assert [{:ex/x "foo-3", :ex/y "bar-3", :id :ex/alice}],
                   :f/retract [{:ex/x "foo-2", :ex/y "bar-2", :id :ex/alice}]},
                  "https://www.w3.org/2018/credentials#issuer"
                  {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"},
                  :f/alias "committest",
                  :f/context
                  "fluree:memory:///contexts/b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"}}
                {:f/t 5,
                 :f/assert [{:ex/x "foo-cat", :ex/y "bar-cat", :id :ex/alice}],
                 :f/commit
                 {:f/address
                  "fluree:memory://14efe5ef1163589f9ee6fcc5af8e2830c3300a80abbbae0aa82194083e93aebe",
                  :f/v 0,
                  :f/previous
                  {:id
                   "fluree:commit:sha256:bbr54svhy4ergg3mmed4eugzljonsl7jlfmadsbcq6b7sr2cs7yyl"},
                  :f/time 720000,
                  :id
                  "fluree:commit:sha256:bb3v3rd7q6lojz5w4gsweglna6afhc6ooh5fgfdhxitdtdwquxwgv",
                  :f/branch "main",
                  :f/message "meow",
                  :f/data
                  {:f/previous
                   {:id
                    "fluree:db:sha256:bt6d6eup2oo2icrjj7ejcmfoa3wsen45y2l2iomlnbqsuoypizgn"},
                   :f/address
                   "fluree:memory://a505e939983b4cfa9325ebe0bbd8615865606c3f4ab9343dd00d594ee722cc10",
                   :f/flakes 102,
                   :f/size 9408,
                   :f/t 5,
                   :f/assert [{:ex/x "foo-cat", :ex/y "bar-cat", :id :ex/alice}],
                   :f/retract [{:ex/x "foo-3", :ex/y "bar-3", :id :ex/alice}]},
                  "https://www.w3.org/2018/credentials#issuer"
                  {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"},
                  :f/alias "committest",
                  :f/context
                  "fluree:memory:///contexts/b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"},
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
