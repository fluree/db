(ns fluree.db.connection.disconnect-test
  "Tests for connection disconnect, ledger release, and idle cleanup functionality"
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]))

(deftest disconnect-prevents-new-operations-test
  (testing "Connection rejects operations after disconnect begins"
    (let [conn @(fluree/connect-memory)
          _    @(fluree/create conn "test1")
          _    @(fluree/db conn "test1")]

      (fluree/disconnect conn)

      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Connection is disconnecting"
           @(fluree/create conn "test2"))
          "Creating new ledger should fail after disconnect")

      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Connection is disconnecting"
           @(fluree/db conn "test1"))
          "Getting db should fail after disconnect"))))

(deftest disconnect-releases-multiple-ledgers-test
  (testing "Disconnect releases all cached ledgers in parallel"
    (let [conn @(fluree/connect-memory)
          _    @(fluree/create conn "ledger1")
          _    @(fluree/create conn "ledger2")
          _    @(fluree/create conn "ledger3")

          ;; Load all ledgers into cache
          _    @(fluree/db conn "ledger1")
          _    @(fluree/db conn "ledger2")
          _    @(fluree/db conn "ledger3")

          ;; Verify all are cached
          state-before @(:state conn)
          cached-count (count (:ledger state-before))]

      (is (= 3 cached-count) "All three ledgers should be cached")

      @(fluree/disconnect conn)

      (let [state-after @(:state conn)
            remaining   (count (:ledger state-after))]
        (is (= 0 remaining) "All ledgers should be released after disconnect")))))

(deftest idle-cleanup-enabled-test
  (testing "Idle cleanup loop is created when configured"
    (let [conn-with-idle @(fluree/connect-memory
                           {:defaults {:ledger-cache-idle-minutes 15}})
          conn-without-idle @(fluree/connect-memory {})]

      ;; Connection with idle timeout should have cleanup channel
      (is (some? (:idle-cleanup-ch conn-with-idle))
          "Idle cleanup channel should be created when timeout configured")

      ;; Connection without idle timeout should not have cleanup channel
      (is (nil? (:idle-cleanup-ch conn-without-idle))
          "Idle cleanup channel should not be created when timeout not configured")

      @(fluree/disconnect conn-with-idle)
      @(fluree/disconnect conn-without-idle))))

(deftest release-ledger-idempotent-test
  (testing "Releasing the same ledger multiple times is safe (race condition protection)"
    (let [conn @(fluree/connect-memory)
          _    @(fluree/create conn "test1")
          _    @(fluree/db conn "test1")]

      ;; Release the ledger once
      @(fluree/release-ledger conn "test1")

      ;; Release again - should not throw
      (is (= :released @(fluree/release-ledger conn "test1"))
          "Second release should succeed")

      ;; And again for good measure
      (is (= :released @(fluree/release-ledger conn "test1"))
          "Third release should also succeed")

      @(fluree/disconnect conn))))
