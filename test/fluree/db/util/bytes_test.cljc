(ns fluree.db.util.bytes-test
  (:require
    #?(:clj  [clojure.test :refer :all]
        :cljs [cljs.test :refer-macros [deftest is testing]])
    [fluree.db.util.bytes :as bytes])
  #?(:clj (:import (java.io Reader))))


(deftest db-util-bytes-test
  (testing "string<->UTF8 conversion"
    (let [x  "Fluree rocks!"
          x' (bytes/string->UTF8 x)]
      (is (= x (bytes/UTF8->string x')))))

  (testing "to-reader"
    #?(:clj
       (is (instance? Reader (bytes/to-reader "Fluree rocks!")))

       :cljs
       (is (thrown? js/Error (bytes/to-reader "Not available in ClojureScript"))))))
