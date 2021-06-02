(ns fluree.db.util.bytes-test
  (:require
    #?@(:clj  [[clojure.test :refer :all]
               [clojure.core.async :refer [chan <! >! go]]]
        :cljs [[cljs.test :refer-macros [deftest is testing]]
               [cljs.core.async :refer [chan go put! >!]]])
    [test-helpers :refer [test-async]]
    [fluree.db.util.bytes :as bytes]))


(deftest db-util-bytes-test
  (testing "string<->UTF8 conversion"
    (let [x  "Fluree rocks!"
          x' (bytes/string->UTF8 x)]
      (is (= x (bytes/UTF8->string x')))))

  (testing "to-reader"
    #?(:clj
       (is (instance? java.io.Reader (bytes/to-reader "Fluree rocks!")))

       :cljs
       (is (thrown? js/Error (bytes/to-reader "Not available in ClojureScript"))))))
