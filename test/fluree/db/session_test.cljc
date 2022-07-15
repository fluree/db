(ns fluree.db.session-test
  (:require #?@(:clj  [[clojure.test :refer :all]]
                :cljs [[cljs.test :refer-macros [deftest is testing]]])
            [fluree.db.session :refer [resolve-ledger]]))

(deftest resolve-ledger-test
  (testing "resolves a string ledger name"
    (is (= ["net" "ledger"] (resolve-ledger "net/ledger"))))
  (testing "resolves a namespaced keyword ledger name"
    (is (= ["net" "ledger"] (resolve-ledger :net/ledger))))
  (testing "resolves a vector of [network ledger-id]"
    (is (= ["net" "ledger"] (resolve-ledger ["net" "ledger"])))))
