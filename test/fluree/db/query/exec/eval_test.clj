(ns fluree.db.query.exec.eval-test
  (:require [clojure.test :as t :refer [deftest testing is]]
            [fluree.db.query.exec.eval :as fun]
            [fluree.db.query.exec.where :as where]
            [fluree.db.constants :as const]))


(deftest equality
  (testing "same value, same type"
    (is (= (where/->typed-val true)
           (fun/equal (where/->typed-val "abc")
                      (where/->typed-val "abc"))))
    (is (= (where/->typed-val false)
           (fun/not-equal (where/->typed-val "abc")
                          (where/->typed-val "abc")))))
  (testing "different value, same type"
    (is (= (where/->typed-val false)
           (fun/equal (where/->typed-val "def")
                      (where/->typed-val "abc"))))
    (is (= (where/->typed-val true)
           (fun/not-equal (where/->typed-val "def")
                          (where/->typed-val "abc")))))
  (testing "same value, different type"
    (is (= (where/->typed-val false)
           (fun/equal (where/->typed-val "ex:abc" const/iri-id)
                      (where/->typed-val "ex:abc" const/iri-string))))
    (is (= (where/->typed-val true)
           (fun/not-equal (where/->typed-val "ex:abc" const/iri-id)
                          (where/->typed-val "ex:abc" const/iri-string)))))
  (testing "different value, different type"
    (is (= (where/->typed-val false)
           (fun/equal (where/->typed-val "abc" const/iri-id)
                      (where/->typed-val "abc" const/iri-string))))
    (is (= (where/->typed-val true)
           (fun/not-equal (where/->typed-val "abc" const/iri-id)
                          (where/->typed-val "abc" const/iri-string)))))
  (testing "same value, same type, same lang"
    (is (= (where/->typed-val true)
           (fun/equal (where/->typed-val "abc" const/iri-lang-string "en")
                      (where/->typed-val "abc" const/iri-lang-string "en"))))
    (is (= (where/->typed-val false)
           (fun/not-equal (where/->typed-val "abc" const/iri-lang-string "en")
                          (where/->typed-val "abc" const/iri-lang-string "en")))))
  (testing "same value, same type, different lang"
    (is (= (where/->typed-val false)
           (fun/equal (where/->typed-val "abc" const/iri-lang-string "en")
                      (where/->typed-val "abc" const/iri-lang-string "es"))))
    (is (= (where/->typed-val true)
           (fun/not-equal (where/->typed-val "abc" const/iri-lang-string "en")
                          (where/->typed-val "abc" const/iri-lang-string "es")))))

  (testing "multiple arities"
    (testing "one arg"
      (is (= (where/->typed-val true)
             (fun/equal (where/->typed-val 1))))
      (is (= (where/->typed-val false)
             (fun/not-equal (where/->typed-val 1)))))
    (testing "2+ args"
      (is (= (where/->typed-val true)
             (fun/equal (where/->typed-val 1)
                        (where/->typed-val 1)
                        (where/->typed-val 1)
                        (where/->typed-val 1))))
      (is (= (where/->typed-val false)
             (fun/not-equal (where/->typed-val 1)
                            (where/->typed-val 1)
                            (where/->typed-val 1)
                            (where/->typed-val 1)))))))
