(ns fluree.db.notify-polling-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.connection :as connection]))

(deftest plan-ns-update-polling-semantics-test
  (testing "Polling semantics for keeping a warm cache fresh"
    (let [db {:t 5
              :commit {:index {:address "idx-A"}}}]
      (testing "No-op when t matches and index address matches (or index is absent)"
        (is (= :noop
               (connection/plan-ns-update db {:ns-t 5
                                              :index-address "idx-A"}))))

      (testing "Index update when t unchanged but index address differs"
        (is (= :index
               (connection/plan-ns-update db {:ns-t 5
                                              :index-address "idx-B"}))))

      (testing "Commit update when t advances by exactly 1 and commit address is present"
        (is (= :commit
               (connection/plan-ns-update db {:ns-t 6
                                              :commit-address "commit-X"}))))

      (testing "Stale when t jumps ahead by more than 1"
        (is (= :stale
               (connection/plan-ns-update db {:ns-t 7
                                              :commit-address "commit-Y"})))))))
