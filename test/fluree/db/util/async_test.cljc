(ns fluree.db.util.async-test
  (:require
    #?@(:clj  [[clojure.test :refer :all]
               [clojure.core.async :refer [chan <! >! go]]]
        :cljs [[cljs.test :refer-macros [deftest is testing]]
               [cljs.core.async :refer [chan go put! >! <!]]])
    [test-helpers :refer [test-async]]
    [fluree.db.util.async :as async])
  #?(:clj  (:import (clojure.lang ExceptionInfo))))


(deftest db-util-async-test
  (testing "testing throw-if-exception"
    (testing "string value"
      (let [msg (async/throw-if-exception "string")]
        (is (string? msg))
        (is (= msg "string"))))
    (testing "exception raised"
      (let [msg (ex-info "To access the server, either open-api must be true or a valid auth must be available."
                        {:status 401
                         :error  :db/invalid-request})]
        (is (thrown? #?(:clj ExceptionInfo :cljs js/Error) (async/throw-if-exception msg))))))

  (testing "merge-into?"
    (testing "multiple channels into a vector"
      (let [ch1 (chan 5)
            ch2 (chan 5)]
        (go
          (>! ch1 "clojure")
          (>! ch2 536))
        (test-async
          (go
            (let [res (<! (async/merge-into? [] [ch1 ch2]))]
              (is (vector? res))
              (is (= 2 (count res)))
              (is (some #(= "clojure" %) res))
              (is (some #(= 536 %) res)))))))))
