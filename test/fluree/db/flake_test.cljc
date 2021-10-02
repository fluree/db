(ns fluree.db.flake-test
  (:require #?(:clj  [clojure.test :refer :all]
               :cljs [cljs.test :refer-macros [deftest is testing]])
            [fluree.db.flake :as flake #?@(:cljs [:refer [Flake]])])
  #?(:clj
     (:import (fluree.db.flake Flake))))

(def test-flakes [(flake/->Flake 1000000 1001 "p 1001" -42 true nil)
                  (flake/->Flake 1000000 1002 1002 -42 true nil)
                  (flake/->Flake 1000000 1003 1003.1 -10 true nil)
                  (flake/->Flake 1000000 1004 2000000 -10 true nil)
                  (flake/->Flake 2000000 1001 "p 1001-1" -11 true nil)
                  (flake/->Flake 2000000 1001 "p 1001-2" -11 true nil)
                  (flake/->Flake 2000000 1001 "p 1001-3" -12 true nil)
                  (flake/->Flake 3000000 1002 10020 -42 true nil)
                  (flake/->Flake 3000000 1002 10021 -42 true nil)
                  (flake/->Flake 4000000 1001 "hello" -42 true nil)
                  (flake/->Flake 4000000 1002 10021 -42 true nil)
                  (flake/->Flake 4000000 1003 1003.2 -42 true nil)
                  (flake/->Flake 4000000 1003 1003.3 -42 true nil)
                  (flake/->Flake 5000000 1001 "p 1001" -42 true nil)
                  (flake/->Flake 5000000 1004 2000000 -10 true nil)
                  (flake/->Flake 5000000 1004 3000000 -10 true nil)])

;; refs point to other flake subjects (have long integer object values)
(def ref-property #{1004})

(defn filter-refs
  [flakes]
  (filter #(ref-property (.-p ^Flake %)) flakes))

(def test-ref-flakes (filter-refs test-flakes))

(deftest flake-parts-correct
  (testing "Flakes parts end up in correct Flake attribute vals."
    (let [s      98076742
          p      1001
          o      "hello"
          t      -42
          op     true
          flake  ^Flake (flake/parts->Flake [s p o t op nil])
          flake2 ^Flake (flake/->Flake s p o t op nil)]
      (is (= flake flake2))
      (is (= s (.-s flake)))
      (is (= p (.-p flake)))
      (is (= o (.-o flake)))
      (is (= t (.-t flake)))
      (is (= op (.-op flake)))
      (is (nil? (.-m flake))))))


(deftest basic-sorting
  (testing "Sorting with different comparators works as intended"
    (let [spot (apply flake/sorted-set-by flake/cmp-flakes-spot test-flakes)
          psot (apply flake/sorted-set-by flake/cmp-flakes-psot test-flakes)
          post (apply flake/sorted-set-by flake/cmp-flakes-post test-flakes)
          opst (apply flake/sorted-set-by flake/cmp-flakes-opst test-ref-flakes)]

      ;; subject is sorted in reverse order
      (is (= (flake/->Flake 5000000 1001 "p 1001" -42 true nil)
             (first spot)))
      (is (= (flake/->Flake 1000000 1004 2000000 -10 true nil)
             (last spot)))

      ;; subject sorted in reverse order
      (is (= (flake/->Flake 5000000 1001 "p 1001" -42 true nil)
             (first psot)))
      (is (= (flake/->Flake 1000000 1004 2000000 -10 true nil)
             (last psot)))

      (is (= (flake/->Flake 4000000 1001 "hello" -42 true nil)
             (first post)))
      (is (= (flake/->Flake 5000000 1001 "p 1001" -42 true nil)
             (second post)))
      (is (= (flake/->Flake 5000000 1004 3000000 -10 true nil)
             (last post)))

      (is (= (flake/->Flake 5000000 1004 3000000 -10 true nil)
             (first opst)))
      (is (= (flake/->Flake 1000000 1004 2000000 -10 true nil)
             (last opst))))))


#?(:clj
   (deftest multi-type-obj-vals
     (testing "Multi-type object values sort correctly"
       (let [flakes   (conj test-flakes
                            (flake/->Flake 4000000 2000 "hello 1" -1000 true nil) ;; class java.lang.String
                            (flake/->Flake 4000000 2000 (long 500000000) -1000 true nil) ;; class java.lang.Long
                            (flake/->Flake 4000000 2000 (int 1234) -1000 true nil) ;; class java.lang.Integer
                            (flake/->Flake 4000000 2000 (bigdec 12345) -1000 true nil) ;; class java.math.BigDecimal
                            ;; extras of same types
                            (flake/->Flake 4000000 2000 "hello 2" -1000 true nil)
                            (flake/->Flake 4000000 2000 (int 2021) -1000 true nil)
                            (flake/->Flake 4000000 2000 (bigdec 10) -1000 true nil)
                            (flake/->Flake 4000000 2000 (long 900000000) -1000 true nil))
             flakes-n (count flakes)
             spot     (apply flake/sorted-set-by flake/cmp-flakes-spot flakes)
             psot     (apply flake/sorted-set-by flake/cmp-flakes-psot flakes)
             post     (apply flake/sorted-set-by flake/cmp-flakes-post flakes)]

         (is (= flakes-n (count spot)))
         (is (= flakes-n (count psot)))
         (is (= flakes-n (count post)))

         ;; will sort first based on data type (stringified), then value
         (is (= (into []
                      (flake/slice spot
                                   (flake/->Flake 4000000 2000 (int 1234) nil nil nil)
                                   (flake/->Flake 4000000 2000 (bigdec 12345) nil nil nil)))
                [(flake/->Flake 4000000 2000 (int 1234) -1000 true nil)
                 (flake/->Flake 4000000 2000 (int 2021) -1000 true nil)
                 (flake/->Flake 4000000 2000 (long 500000000) -1000 true nil)
                 (flake/->Flake 4000000 2000 (long 900000000) -1000 true nil)
                 (flake/->Flake 4000000 2000 "hello 1" -1000 true nil)
                 (flake/->Flake 4000000 2000 "hello 2" -1000 true nil)
                 (flake/->Flake 4000000 2000 (bigdec 10) -1000 true nil)
                 (flake/->Flake 4000000 2000 (bigdec 12345) -1000 true nil)]))

         ;; just like last test except one less of both boundaries
         (is (= (into []
                      (flake/slice spot
                                   (flake/->Flake 4000000 2000 (int 1235) nil nil nil)
                                   (flake/->Flake 4000000 2000 (bigdec 12344) nil nil nil)))
                [(flake/->Flake 4000000 2000 (int 2021) -1000 true nil)
                 (flake/->Flake 4000000 2000 (long 500000000) -1000 true nil)
                 (flake/->Flake 4000000 2000 (long 900000000) -1000 true nil)
                 (flake/->Flake 4000000 2000 "hello 1" -1000 true nil)
                 (flake/->Flake 4000000 2000 "hello 2" -1000 true nil)
                 (flake/->Flake 4000000 2000 (bigdec 10) -1000 true nil)]))

         (is (= (into []
                      (flake/slice spot
                                   (flake/->Flake 4000000 2000 (int 2021) nil nil nil)
                                   (flake/->Flake 4000000 2000 "hello 1" nil nil nil)))
                [(flake/->Flake 4000000 2000 (int 2021) -1000 true nil)
                 (flake/->Flake 4000000 2000 (long 500000000) -1000 true nil)
                 (flake/->Flake 4000000 2000 (long 900000000) -1000 true nil)
                 (flake/->Flake 4000000 2000 "hello 1" -1000 true nil)]))))))