(ns fluree.db.dbfunctions.internal-test
  (:require #?@(:clj  [[clojure.test :refer :all]]
                :cljs [[cljs.test :refer-macros [deftest is testing]]])
            [fluree.db.dbfunctions.internal :as f]))

(deftest db-functions-internal-test
  (testing "boolean"
    (testing "falsey values"
      (is (false? (f/boolean nil))
          "nil is false")
      (is (false? (f/boolean false))
          "boolean false is false"))
    (testing "truthy values"
      (is (true? (f/boolean "a string"))
          "a string is true")
      (is (true? (f/boolean true))
          "boolean true is true")
      (is (true? (f/boolean 0))
          "number zero is true")
      (is (true? (f/boolean 42))
          "non-zero number is true")))
  (testing "subs"
    (let [test-str "hello world"
          len      (count test-str)]
      (is (= "ello world" (f/subs test-str 1))
          "single-arity call acts as start position")
      (is (= "ello worl" (f/subs test-str 1 (dec len)))
          "two-arity call properly does start & end position")))
  (testing "not="
    (is (true? (f/not= 4 42))
        "not equal integers")
    (is (true? (f/not= "abc" "def"))
        "not equal strings")
    (is (true? (f/not= 42 "def"))
        "not equal different types")
    (is (true? (f/not= 42 nil))
        "not equal with one nil")
    (is (false? (f/not= 42 42))
        "values are equal")
    (is (false? (f/not= nil nil))
        "nil is equal to nil"))
  (testing "unreverse-var"
    (is (= "my/reverse-ref" (f/unreverse-var "my/_reverse-ref"))
        "reverse ref underscore removed")))






