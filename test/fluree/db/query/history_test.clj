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
               :f/retract [{:ex/x "foo-1", :id :ex/dog}
                           {:ex/x "foo-1", :id :ex/cat}]}
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
               :f/retract [{:ex/x "foo-2", :id :ex/dan}]}]
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
          "does not include t 1, 4, or 5")
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
          (is (= [#:f{:t 2,
                      :assert [{:ex/x "foo-2", :ex/y "bar-2", :id :ex/dan}],
                      :retract [{:ex/x "foo-1", :ex/y "bar-1", :id :ex/dan}]}]
                 @(fluree/history ledger {:history [:ex/dan] :t {:from 2}}))))))))
