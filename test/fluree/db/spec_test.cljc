(ns fluree.db.spec-test
  (:require #?(:clj  [clojure.test :refer :all]
               :cljs [cljs.test :refer-macros [deftest is testing]])
            [fluree.db.spec :as s])
  #?(:clj
     (:import (clojure.lang ExceptionInfo)
              (java.lang Double Float Integer))))


(deftest type-check-test
  (testing "double"
    (is (= (double 2.8111111125989) (s/type-check "2.8111111125989" :double)))
    (is (= (double 1.8111111125989) (s/type-check 1.8111111125989 :double)))
    #?(:clj  (is (thrown-with-msg? ExceptionInfo #"Could not conform value to" (s/type-check "jinkies" :double)))
       :cljs (is (js/isNaN (s/type-check "jinkies" :double)))))
  (testing "floating point"
    (is (= (float 3.11112) (s/type-check "3.11112" :float)))
    (is (= (float 4.11112) (s/type-check 4.11112 :float)))
    #?(:clj  (is (thrown-with-msg? ExceptionInfo #"Could not conform value to" (s/type-check "jinkies" :float)))
       :cljs (is (js/isNaN (s/type-check "jinkies" :float))))))

