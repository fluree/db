(ns fluree.db.query.history-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]))

(deftest ^:integration history-query
  (let [conn (test-utils/create-conn)
        ledger @(fluree/create conn "historytest" {:context {:ex "http://example.org/ns/"}})

        db1 @(test-utils/transact ledger {:id :ex/dan
                                          :ex/x "foo-1"
                                          :ex/y "bar-1"})
        db2 @(test-utils/transact ledger {:id :ex/dan
                                          :ex/x "foo-2"
                                          :ex/y "bar-2"})
        db3 @(test-utils/transact ledger {:id :ex/dan
                                          :ex/x "foo-3"
                                          :ex/y "bar-3"})
        db4 @(test-utils/transact ledger {:id :ex/cat
                                          :ex/x "foo-cat"
                                          :ex/y "bar-cat"})
        db5 @(test-utils/transact ledger {:id :ex/dan
                                          :ex/x "foo-cat"
                                          :ex/y "bar-cat"})]
    (testing "subject history"
      (is (= [{:t 5 :assert {:ex/x "foo-cat" :ex/y "bar-cat"} :retract {:ex/x "foo-3" :ex/y "bar-3"}}
              {:t 3 :assert {:ex/x "foo-3" :ex/y "bar-3"} :retract {:ex/x "foo-2" :ex/y "bar-2"}}
              {:t 2 :assert {:ex/x "foo-2" :ex/y "bar-2"} :retract {:ex/x "foo-1" :ex/y "bar-1"}}
              {:t 1 :assert {:id :ex/dan :ex/x "foo-1" :ex/y "bar-1"}}]
             @(fluree/history ledger {:history :ex/dan}))))
    (testing "one-tuple flake history"
      (is (= [{:t 5,
               :retract #:ex{:x "foo-3", :y "bar-3"},
               :assert #:ex{:x "foo-cat", :y "bar-cat"}}
              {:t 3,
               :assert #:ex{:x "foo-3", :y "bar-3"},
               :retract #:ex{:x "foo-2", :y "bar-2"}}
              {:t 2,
               :assert #:ex{:x "foo-2", :y "bar-2"},
               :retract #:ex{:x "foo-1", :y "bar-1"}}
              {:t 1, :assert {:id :ex/dan, :ex/x "foo-1", :ex/y "bar-1"}}]
             @(fluree/history ledger {:history [:ex/dan]}))))
    (testing "two-tuple flake history"
      (is (= [{:t 5 :assert {:ex/x "foo-cat"} :retract {:ex/x "foo-3"}}
              {:t 3 :assert {:ex/x "foo-3"} :retract {:ex/x "foo-2"}}
              {:t 2 :assert {:ex/x "foo-2"} :retract {:ex/x "foo-1"}}
              {:t 1 :assert {:ex/x "foo-1"}}]
             @(fluree/history ledger {:history [:ex/dan :ex/x]})))

      (is (= [{:t 5 :assert {:ex/x "foo-cat"} :retract {:ex/x "foo-3"}}
              {:t 4 :assert {:ex/x "foo-cat"}}
              {:t 3 :assert {:ex/x "foo-3"} :retract {:ex/x "foo-2"}}
              {:t 2 :assert {:ex/x "foo-2"} :retract {:ex/x "foo-1"}}
              {:t 1 :assert {:ex/x "foo-1"}}]
             @(fluree/history ledger {:history [nil :ex/x]}))))
    (testing "three-tuple flake history"
      (is (= [{:t 5 :assert {:ex/x "foo-cat"}}
              {:t 4 :assert {:ex/x "foo-cat"}}]
             @(fluree/history ledger {:history [nil :ex/x "foo-cat"]})))
      (is (= [{:t 3 :retract {:ex/x "foo-2"}}
              {:t 2 :assert {:ex/x "foo-2"}}]
             @(fluree/history ledger {:history [nil :ex/x "foo-2"]})))
      (is (= [{:t 5 :assert {:ex/x "foo-cat"}}]
             @(fluree/history ledger {:history [:ex/dan :ex/x "foo-cat"]}))))

    (testing "at-t"
      (is (= [{:t 3 :assert {:ex/x "foo-3"}}]
             @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:from 3 :to 3}}))))
    (testing "from-t"
      (is (= [{:t 5 :assert {:ex/x "foo-cat"} :retract {:ex/x "foo-3"}}
              {:t 3 :assert {:ex/x "foo-3"}}]
             @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:from 3}}))))
    (testing "to-t"
      (is (= [{:t 3 :assert {:ex/x "foo-3"} :retract {:ex/x "foo-2"}}
              {:t 2 :assert {:ex/x "foo-2"} :retract {:ex/x "foo-1"}}
              {:t 1 :assert {:ex/x "foo-1"}}]
             @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:to 3}}))))
    (testing "t-range"
      (is (= [{:t 3 :assert {:ex/x "foo-3"} :retract {:ex/x "foo-2"}}
              {:t 2 :assert {:ex/x "foo-2"} :retract {:ex/x "foo-1"}}
              {:t 1 :assert {:ex/x "foo-1"}}]
             @(fluree/history ledger {:history [:ex/dan :ex/x] :t {:from 1 :to 3}}))))

    (testing "invalid query"
      (is (= "History query not properly formatted. Provided {:history []}"
             (-> @(fluree/history ledger {:history []})
                 (Throwable->map)
                 :cause))))))

(deftest ^:integration commit-query
  (with-redefs [fluree.db.util.core/current-time-iso (fn [] "1970-01-01T00:12:00.00000Z")]
    (let [conn (test-utils/create-conn)
          ledger @(fluree/create conn "committest" {:context {:ex "http://example.org/ns/"}})

          db1 @(test-utils/transact ledger {:id :ex/alice
                                            :ex/x "foo-1"
                                            :ex/y "bar-1"})
          db2 @(test-utils/transact ledger {:id :ex/brian
                                            :ex/x "foo-2"
                                            :ex/y "bar-2"})
          db3 @(test-utils/transact ledger {:id :ex/dan
                                            :ex/x "foo-3"
                                            :ex/y "bar-3"})
          db4 @(test-utils/transact ledger {:id :ex/cat
                                            :ex/x "foo-cat"
                                            :ex/y "bar-cat"})
          db5 @(test-utils/transact ledger {:id :ex/dan
                                            :ex/x "foo-cat"
                                            :ex/y "bar-cat"})]
      (testing "at time t"
        (let [[commit] @(fluree/commit-details ledger {:commit-details {:from 1 :to 1}})]
          (is (= {:f/commit	  
                  {:f/time 720000,
                   "https://www.w3.org/2018/credentials#issuer"
                   {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"},
                   :f/v 0,
                   :f/data
                   {:id
                    "fluree:db:sha256:bkvjivpfq55d2vfh3ttehckx5fdmkkxb5cqggwhuejfdkm5d6f2j"}},
                  :f/data
                  {:f/address
                   "fluree:memory://72711dc8318d4a0fb198ded6b20dad90a14339b67e52c628767451dfba32d044",
                   :f/flakes 14,
                   :f/size 1884,
                   :f/t 1,
                   :f/assert
                   [{:rdf/type [:f/Context],
                     :f/context
                     "{\"schema\":\"http://schema.org/\",\"wiki\":\"https://www.wikidata.org/wiki/\",\"xsd\":\"http://www.w3.org/2001/XMLSchema#\",\"type\":\"@type\",\"rdfs\":\"http://www.w3.org/2000/01/rdf-schema#\",\"ex\":\"http://example.org/ns/\",\"id\":\"@id\",\"f\":\"https://ns.flur.ee/ledger#\",\"sh\":\"http://www.w3.org/ns/shacl#\",\"skos\":\"http://www.w3.org/2008/05/skos#\",\"rdf\":\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"}",
                     :id "fluree-default-context"}
                    {:ex/x "foo-1", :ex/y "bar-1", :id :ex/alice}],
                   :f/retract []}}
                 commit)))))))
