(ns fluree.db.query.exec.eval-test
  (:require [clojure.test :as t :refer [deftest testing is]]
            [fluree.db.query.exec.eval :as fun]
            [fluree.db.query.exec.where :as where]
            [fluree.db.constants :as const]))

(deftest equality
  (testing "type-indifferent equal"
    (testing "same value, same type"
      (is (= (where/->typed-val true)
             (fun/untyped-equal (where/->typed-val "abc")
                                (where/->typed-val "abc"))))
      (is (= (where/->typed-val false)
             (fun/untyped-not-equal (where/->typed-val "abc")
                                    (where/->typed-val "abc")))))
    (testing "same value, different comparable type"
      (is (= (where/->typed-val true)
             (fun/untyped-equal (where/->typed-val "abc")
                                (where/->typed-val "abc" const/iri-lang-string))))
      (is (= (where/->typed-val false)
             (fun/untyped-not-equal (where/->typed-val "abc")
                                    (where/->typed-val "abc" const/iri-lang-string)))))
    (testing "different value, different comparable type"
      (is (= (where/->typed-val false)
             (fun/untyped-equal (where/->typed-val "def")
                                (where/->typed-val "abc" const/iri-lang-string))))
      (is (= (where/->typed-val true)
             (fun/untyped-not-equal (where/->typed-val "def")
                                    (where/->typed-val "abc" const/iri-lang-string)))))
    (testing "different value, different incomparable type"
      (is (= (where/->typed-val false)
             (fun/untyped-equal (where/->typed-val true)
                                (where/->typed-val "abc")))
          "doesn't throw an exception")
      (is (= (where/->typed-val true)
             (fun/untyped-not-equal (where/->typed-val true)
                                    (where/->typed-val "abc")))
          "doesn't throw an exception"))
    (testing "one arg"
      (is (= (where/->typed-val true)
             (fun/untyped-equal (where/->typed-val 1))))
      (is (= (where/->typed-val false)
             (fun/untyped-not-equal (where/->typed-val 1)))))
    (testing "2+ args"
      (is (= (where/->typed-val true)
             (fun/untyped-equal (where/->typed-val 1)
                                (where/->typed-val 1)
                                (where/->typed-val 1))))
      (is (= (where/->typed-val false)
             (fun/untyped-equal (where/->typed-val 1)
                                (where/->typed-val 1)
                                (where/->typed-val 2))))
      (is (= (where/->typed-val false)
             (fun/untyped-not-equal (where/->typed-val 1)
                                    (where/->typed-val 1)
                                    (where/->typed-val 1))))
      (is (= (where/->typed-val true)
             (fun/untyped-not-equal (where/->typed-val 1)
                                    (where/->typed-val 2)
                                    (where/->typed-val 1))))))
  (testing "type-aware equal"
    (testing "same value, same type"
      (is (= (where/->typed-val true)
             (fun/typed-equal (where/->typed-val "abc")
                              (where/->typed-val "abc"))))
      (is (= (where/->typed-val false)
             (fun/typed-not-equal (where/->typed-val "abc")
                                  (where/->typed-val "abc")))))
    (testing "different value, same type"
      (is (= (where/->typed-val false)
             (fun/typed-equal (where/->typed-val "def")
                              (where/->typed-val "abc"))))
      (is (= (where/->typed-val true)
             (fun/typed-not-equal (where/->typed-val "def")
                                  (where/->typed-val "abc")))))
    (testing "same value, different comparable type"
      (is (= (where/->typed-val true)
             (fun/typed-equal (where/->typed-val "ex:abc" const/iri-id)
                              (where/->typed-val "ex:abc" const/iri-string))))
      (is (= (where/->typed-val false)
             (fun/typed-not-equal (where/->typed-val "ex:abc" const/iri-id)
                                  (where/->typed-val "ex:abc" const/iri-string)))))
    (testing "different value, different comparable type"
      (is (= (where/->typed-val false)
             (fun/typed-equal (where/->typed-val "abc" const/iri-id)
                              (where/->typed-val "def" const/iri-string))))
      (is (= (where/->typed-val true)
             (fun/typed-not-equal (where/->typed-val "abc" const/iri-id)
                                  (where/->typed-val "def" const/iri-string)))))
    (testing "different value, different incomparable type"
      (is (= ["Incomparable datatypes: http://www.w3.org/2001/XMLSchema#integer and http://www.w3.org/2001/XMLSchema#string"
              {:a 1, :a-dt "http://www.w3.org/2001/XMLSchema#integer",
               :b "abc", :b-dt "http://www.w3.org/2001/XMLSchema#string",
               :status 400,
               :error :db/invalid-query}]
             (try
               (fun/typed-equal (where/->typed-val 1)
                                (where/->typed-val "abc" const/iri-string))
               (catch Exception e
                 [(ex-message e)
                  (ex-data e)]))))
      (is (= ["Incomparable datatypes: http://www.w3.org/2001/XMLSchema#integer and http://www.w3.org/2001/XMLSchema#string"
              {:a 1, :a-dt "http://www.w3.org/2001/XMLSchema#integer",
               :b "abc", :b-dt "http://www.w3.org/2001/XMLSchema#string",
               :status 400,
               :error :db/invalid-query}]
             (try
               (fun/typed-not-equal (where/->typed-val 1)
                                    (where/->typed-val "abc" const/iri-string))
               (catch Exception e
                 [(ex-message e)
                  (ex-data e)])))))
    (testing "same value, same type, same lang"
      (is (= (where/->typed-val true)
             (fun/typed-equal (where/->typed-val "abc" const/iri-lang-string "en")
                              (where/->typed-val "abc" const/iri-lang-string "en"))))
      (is (= (where/->typed-val false)
             (fun/typed-not-equal (where/->typed-val "abc" const/iri-lang-string "en")
                                  (where/->typed-val "abc" const/iri-lang-string "en")))))
    (testing "same value, same type, different lang"
      (is (= (where/->typed-val true)
             (fun/typed-equal (where/->typed-val "abc" const/iri-lang-string "en")
                              (where/->typed-val "abc" const/iri-lang-string "es"))))
      (is (= (where/->typed-val false)
             (fun/typed-not-equal (where/->typed-val "abc" const/iri-lang-string "en")
                                  (where/->typed-val "abc" const/iri-lang-string "es")))))

    (testing "multiple arities"
      (testing "one arg"
        (is (= (where/->typed-val true)
               (fun/typed-equal (where/->typed-val 1))))
        (is (= (where/->typed-val false)
               (fun/typed-not-equal (where/->typed-val 1)))))
      (testing "2+ args"
        (is (= (where/->typed-val true)
               (fun/typed-equal (where/->typed-val 1)
                                (where/->typed-val 1)
                                (where/->typed-val 1))))
        (is (= (where/->typed-val false)
               (fun/typed-equal (where/->typed-val 1)
                                (where/->typed-val 1)
                                (where/->typed-val 2))))
        (is (= (where/->typed-val false)
               (fun/typed-not-equal (where/->typed-val 1)
                                    (where/->typed-val 1)
                                    (where/->typed-val 1))))
        (is (= (where/->typed-val true)
               (fun/typed-not-equal (where/->typed-val 1)
                                    (where/->typed-val 2)
                                    (where/->typed-val 1))))))))
