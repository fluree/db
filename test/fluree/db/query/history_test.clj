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
      (is (= [{:f/t 5
               :f/assert [{:id :ex/dan :ex/x "foo-cat" :ex/y "bar-cat"}]
               :f/retract [{:id :ex/dan :ex/x "foo-3" :ex/y "bar-3"}]}
              {:f/t 3
               :f/assert [{:id :ex/dan :ex/x "foo-3" :ex/y "bar-3"}]
               :f/retract [{:id :ex/dan :ex/x "foo-2" :ex/y "bar-2"}]}
              {:f/t 2
               :f/assert [{:id :ex/dan :ex/x "foo-2" :ex/y "bar-2"}]
               :f/retract [{:id :ex/dan :ex/x "foo-1" :ex/y "bar-1"}]}
              {:f/t 1
               :f/assert [{:id :ex/dan :ex/x "foo-1" :ex/y "bar-1"}]
               :f/retract []}]
             @(fluree/history ledger {:history :ex/dan}))))
    (testing "one-tuple flake history"
      (is (= [{:f/t 5
               :f/assert [{:ex/x "foo-cat" :ex/y "bar-cat" :id :ex/dan}]
               :f/retract [{:ex/x "foo-3" :ex/y "bar-3" :id :ex/dan}]}
              {:f/t 3
               :f/assert [{:ex/x "foo-3" :ex/y "bar-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :ex/y "bar-2" :id :ex/dan}]}
              {:f/t 2
               :f/assert [{:ex/x "foo-2" :ex/y "bar-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :ex/y "bar-1" :id :ex/dan}]}
              {:f/t 1
               :f/assert [{:id :ex/dan :ex/x "foo-1" :ex/y "bar-1"}]
               :f/retract []}]
             @(fluree/history ledger {:history [:ex/dan]}))))
    (testing "two-tuple flake history"
      (is (= [{:f/t 5
               :f/assert [{:ex/x "foo-cat" :id :ex/dan}]
               :f/retract [{:ex/x "foo-3" :id :ex/dan}]}
              {:f/t 3
               :f/assert [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}
              {:f/t 2
               :f/assert [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :id :ex/dan}]}
              {:f/t 1 :f/assert [{:ex/x "foo-1" :id :ex/dan}] :f/retract []}]
             @(fluree/history ledger {:history [:ex/dan :ex/x]})))

      (is (= [{:f/t 5
               :f/assert [{:ex/x "foo-cat" :id :ex/dan}]
               :f/retract [{:ex/x "foo-3" :id :ex/dan}]}
              {:f/t 4
               :f/assert [{:ex/x "foo-dog" :id :ex/dog}
                          {:ex/x "foo-cat" :id :ex/cat}]
               :f/retract [{:ex/x "foo-1" :id :ex/dog}
                           {:ex/x "foo-1" :id :ex/cat}]}
              {:f/t 3
               :f/assert [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}
              {:f/t 2
               :f/assert [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :id :ex/dan}]}
              {:f/t 1 :f/assert [{:ex/x "foo-1" :id :ex/dog}
                                 {:ex/x "foo-1" :id :ex/cat}
                                 {:ex/x "foo-1" :id :ex/dan}]
               :f/retract []}]
             @(fluree/history ledger {:history [nil :ex/x]}))))
    (testing "three-tuple flake history"
      (is (= [{:f/t 5 :f/assert [{:ex/x "foo-cat" :id :ex/dan}] :f/retract []}
              {:f/t 4 :f/assert [{:ex/x "foo-cat" :id :ex/cat}] :f/retract []}]
             @(fluree/history ledger {:history [nil :ex/x "foo-cat"]})))
      (is (= [{:f/t 3
               :f/assert []
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}
              {:f/t 2
               :f/assert [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract []}]
             @(fluree/history ledger {:history [nil :ex/x "foo-2"]})))
      (is (= [{:f/t 5 :f/assert [{:ex/x "foo-cat" :id :ex/dan}] :f/retract []}]
             @(fluree/history ledger {:history [:ex/dan :ex/x "foo-cat"]}))))

    (testing "at-t"
      (is (= [{:f/t 3
               :f/assert [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}]
             @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:from 3 :to 3}}))))
    (testing "from-t"
      (is (= [{:f/t 5
               :f/assert [{:ex/x "foo-cat" :id :ex/dan}]
               :f/retract [{:ex/x "foo-3" :id :ex/dan}]}
              {:f/t 3
               :f/assert [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}]
             @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:from 3}}))))
    (testing "to-t"
      (is (= [{:f/t 3
               :f/assert [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}
              {:f/t 2
               :f/assert [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :id :ex/dan}]}
              {:f/t 1
               :f/assert [{:ex/x "foo-1" :id :ex/dan}]
               :f/retract []}]
             @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:to 3}}))))
    (testing "t-range"
      (is (= [{:f/t 4
               :f/assert [{:ex/x "foo-dog" :id :ex/dog} {:ex/x "foo-cat" :id :ex/cat}]
               :f/retract [{:ex/x "foo-1" :id :ex/dog} {:ex/x "foo-1" :id :ex/cat}]}
              {:f/t 3
               :f/assert [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}
              {:f/t 2
               :f/assert [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :id :ex/dan}]}]
             @(fluree/history ledger {:history [nil :ex/x] :t {:from 2 :to 4}}))))
    (testing "datetime-t"
      (is (= [{:f/t 3
               :f/assert [{:ex/x "foo-3" :id :ex/dan}]
               :f/retract [{:ex/x "foo-2" :id :ex/dan}]}
              {:f/t 2
               :f/assert [{:ex/x "foo-2" :id :ex/dan}]
               :f/retract [{:ex/x "foo-1" :id :ex/dan}]}]
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
                 {:f/time 720000
                  "https://www.w3.org/2018/credentials#issuer" {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                  :f/v 0
                  :f/address
                  "fluree:memory://72711dc8318d4a0fb198ded6b20dad90a14339b67e52c628767451dfba32d044"
                  :f/flakes 14
                  :f/size 1884
                  :f/t 1
                  :f/data
                  {:id
                   "fluree:db:sha256:bkvjivpfq55d2vfh3ttehckx5fdmkkxb5cqggwhuejfdkm5d6f2j"
                   :f/assert
                   [{:ex/x "foo-1" :ex/y "bar-1" :id :ex/alice}
                    {:rdf/type [:f/Context]
                     :f/context
                     "{\"schema\":\"http://schema.org/\",\"wiki\":\"https://www.wikidata.org/wiki/\",\"xsd\":\"http://www.w3.org/2001/XMLSchema#\",\"type\":\"@type\",\"rdfs\":\"http://www.w3.org/2000/01/rdf-schema#\",\"ex\":\"http://example.org/ns/\",\"id\":\"@id\",\"f\":\"https://ns.flur.ee/ledger#\",\"sh\":\"http://www.w3.org/ns/shacl#\",\"skos\":\"http://www.w3.org/2008/05/skos#\",\"rdf\":\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"}"
                     :id "fluree-default-context"}]
                   :f/retract []}}}]
               @(fluree/commit-details ledger {:commit-details {:from 1 :to 1}})))
        (let [commit-5 {:f/commit
                        {:f/address
                         "fluree:memory://f6fe35ec12acc2b0290e46f7f25c78c324f3a06f25798b5c052c90b42ea4f364"
                         :f/t 5
                         :f/v 0
                         :f/time 720000
                         :f/size 8260
                         :f/message "meow"
                         "https://www.w3.org/2018/credentials#issuer"
                         {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                         :f/flakes 87
                         :f/data
                         {:f/assert [{:ex/x "foo-cat" :ex/y "bar-cat" :id :ex/alice}]
                          :id "fluree:db:sha256:bpld3cjgz6belghbg3hqic4vttng7aumavfd66gkvozfq55sgfid"
                          :f/retract [{:ex/x "foo-3" :ex/y "bar-3" :id :ex/alice}]}}}]
          (is (= [commit-5]
                 @(fluree/commit-details ledger {:commit-details {:from 5 :to 5}})))
          (is (= [commit-5]
                 @(fluree/commit-details ledger {:commit-details :latest})))))

      (testing "time range"
        (let [[c4 c3 c2 :as response] @(fluree/commit-details ledger {:commit-details {:from 2 :to 4}})]
          (testing "all commits in time range are returned"
            (is (=  3
                    (count response))))
          (is (=  {:f/commit
                   {:f/time 720000
                    "https://www.w3.org/2018/credentials#issuer"
                    {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                    :f/v 0
                    :f/address
                    "fluree:memory://f900a0e4e136554c374299235f022030b63817bf2db6e858d8dcc0e5dc7efef0"
                    :f/flakes 73
                    :f/size 7138
                    :f/t 4
                    :f/data
                    {:f/assert [{:ex/x "foo-cat" :ex/y "bar-cat" :id :ex/cat}]
                     :id
                     "fluree:db:sha256:bbw362vfdfw4jsrbsmgqqxsfdex7wj6qqhtqkq4vmc3lbpwah7se6"
                     :f/retract []}}}
                  c4))
          (is (= {:f/commit
                  {:f/time 720000
                   "https://www.w3.org/2018/credentials#issuer"
                   {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                   :f/v 0
                   :f/address
                   "fluree:memory://7dd0b6bedd43e5c6f73a1d46cf9d540fff479a073bd28ac98ebd9d32e498567b"
                   :f/flakes 60
                   :f/size 6028
                   :f/t 3
                   :f/data
                   {:f/assert [{:ex/x "foo-3" :ex/y "bar-3" :id :ex/alice}]
                    :id
                    "fluree:db:sha256:bspt4cqxm2s7qc6gxoiwhahuh2lezq2r5jbnczuv5mn3enqe5kds"
                    :f/retract [{:ex/x "foo-2" :ex/y "bar-2" :id :ex/alice}]}}}
                 c3))
          (is (= {:f/commit
                  {:f/time 720000
                   "https://www.w3.org/2018/credentials#issuer"
                   {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                   :f/v 0
                   :f/address
                   "fluree:memory://e9554d319d7e6efe17aadce955f0f2be02598387caa0e66f1c7f01791e779721"
                   :f/flakes 46
                   :f/size 4914
                   :f/t 2
                   :f/data
                   {:id
                    "fluree:db:sha256:bbxq6tp5acnqfj5hpkfbh35ft6xo4nsjsxrdzrraplamtk4j6xfnc"
                    :f/assert [{:ex/x "foo-2" :ex/y "bar-2" :id :ex/alice}]
                    :f/retract [{:ex/x "foo-1" :ex/y "bar-1" :id :ex/alice}]}}}
                 c2))))

      (testing "time range from"
        (is (= [{:f/commit
                  {:f/address
                   "fluree:memory://f6fe35ec12acc2b0290e46f7f25c78c324f3a06f25798b5c052c90b42ea4f364",
                   :f/t 5,
                   :f/v 0,
                   :f/time 720000,
                   :f/size 8260,
                   :f/message "meow",
                   :f/data
                   {:f/assert [{:ex/x "foo-cat", :ex/y "bar-cat", :id :ex/alice}],
                    :id
                    "fluree:db:sha256:bpld3cjgz6belghbg3hqic4vttng7aumavfd66gkvozfq55sgfid",
                    :f/retract [{:ex/x "foo-3", :ex/y "bar-3", :id :ex/alice}]},
                   "https://www.w3.org/2018/credentials#issuer"
                   {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"},
                   :f/flakes 87}}]
                @(fluree/commit-details ledger {:commit-details {:from 5}}))))

      (testing "time range to"
        (is (= [{:f/commit
                 {:f/time 720000,
                  "https://www.w3.org/2018/credentials#issuer"
                  {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"},
                  :f/v 0,
                  :f/data
                  {:f/assert
                   [{:ex/x "foo-1", :ex/y "bar-1", :id :ex/alice}
                    {:rdf/type [:f/Context],
                     :f/context
                     "{\"schema\":\"http://schema.org/\",\"wiki\":\"https://www.wikidata.org/wiki/\",\"xsd\":\"http://www.w3.org/2001/XMLSchema#\",\"type\":\"@type\",\"rdfs\":\"http://www.w3.org/2000/01/rdf-schema#\",\"ex\":\"http://example.org/ns/\",\"id\":\"@id\",\"f\":\"https://ns.flur.ee/ledger#\",\"sh\":\"http://www.w3.org/ns/shacl#\",\"skos\":\"http://www.w3.org/2008/05/skos#\",\"rdf\":\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"}",
                     :id "fluree-default-context"}],
                   :id
                   "fluree:db:sha256:bkvjivpfq55d2vfh3ttehckx5fdmkkxb5cqggwhuejfdkm5d6f2j",
                   :f/retract []},
                  :f/address
                  "fluree:memory://72711dc8318d4a0fb198ded6b20dad90a14339b67e52c628767451dfba32d044",
                  :f/flakes 14,
                  :f/size 1884,
                  :f/t 1}}]
               @(fluree/commit-details ledger {:commit-details {:to 1}}))))

      (testing "history commit details"
        (is (= [{:f/t 5
                 :f/assert [{:ex/x "foo-cat" :ex/y "bar-cat" :id :ex/alice}]
                 :f/commit
                 {:f/commit
                  {:f/address
                   "fluree:memory://f6fe35ec12acc2b0290e46f7f25c78c324f3a06f25798b5c052c90b42ea4f364"
                   :f/t 5
                   :f/v 0
                   :f/time 720000
                   :f/size 8260
                   :f/flakes 87
                   :f/message "meow"
                   "https://www.w3.org/2018/credentials#issuer"
                   {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"}
                   :f/data
                   {:id "fluree:db:sha256:bpld3cjgz6belghbg3hqic4vttng7aumavfd66gkvozfq55sgfid"
                    :f/assert [{:ex/x "foo-cat" :ex/y "bar-cat" :id :ex/alice}]
                    :f/retract [{:ex/x "foo-3" :ex/y "bar-3" :id :ex/alice}]}}}
                 :f/retract [{:ex/x "foo-3" :ex/y "bar-3" :id :ex/alice}]}]
               @(fluree/history ledger {:history :ex/alice :commit-details true :t {:from 5}})))))))
