(ns fluree.db.util.json-test
  (:require #?@(:clj  [[clojure.test :refer [deftest is testing]]
                       [fluree.db.util.bytes :as u-bytes]
                       [fluree.db.util.json :as json]]
                :cljs [[cljs.test :refer-macros [deftest is testing]]]))
  #?(:clj
     (:import (java.io ByteArrayInputStream)
              (java.math BigDecimal)
              (java.lang Double Float Integer))))

;; Clojure-specific
#?(:clj
   (defn string->stream
     ([s] (string->stream s "UTF-8"))
     ([^String s ^String encoding]
      (-> s
          (.getBytes encoding)
          (ByteArrayInputStream.)))))

;; General Comments
;; all java.lang.Float, java.lang.Double and java.math.BigDecimal values
;; are parsed as BigDecimals when jsonista's bigdecimals option is true.
;; Fluree engine handles this by looking up the schema & coercing the
;; input value into the appropriate type.
;; ----------
;; CLojure: Float/JSON Parse: The Float max value (3.4028235E+38) is cast
;; to a double, unless explicitly set using Float/MAX_VALUE. So, the test
;; comparisons for these values use a Double, with a conversion delta of
;; 3.3614711319430846E30
#?(:clj
   (deftest db-util-json-test
     (testing ":fdb-json-bigdec-string: true"
       (json/encode-BigDecimal-as-string true)
       (testing "parse stream"
         (testing "normal values"
           (let [x  {:_id "parser"
                     :name "test-01"
                     :fv   (float 3.11112)
                     :dv   (double 1.8111111125989)
                     :bdv  (bigdec 1.8333333333333332593184650249895639717578887939453125)
                     :biv  (bigint 1.8374872394873333e+89)
                     :iv   (int 72356)}
                 x' (-> x (json/stringify) (string->stream))
                 x* (json/parse x')]
             (is (instance? ByteArrayInputStream x'))
             (is (map? x*))
             (is (string? (:bdv x*)))
             (is (number? (:fv x*)))
             (is (= (:name x) (:name x*)))
             (is (= (:fv x) (-> x* :fv float)))
             (is (= (:dv x) (-> x* :dv double)))
             (is (= (:bdv x) (-> x* :bdv bigdec)))
             (is (= (:biv x) (:biv x*)))
             (is (= (:iv x) (:iv x*)))))
         (testing "minimum values"
           (let [x  {:_id "parser"
                     :name "test-02"
                     :fv   Float/MIN_VALUE
                     :dv   Double/MIN_VALUE
                     :iv   Integer/MIN_VALUE}
                 x' (-> x (json/stringify) (string->stream))
                 x* (json/parse x')]
             (is (instance? ByteArrayInputStream x'))
             (is (map? x*))
             (is (= (:name x) (:name x*)))
             (is (= (:fv x) (-> x* :fv float)))
             (is (= (:dv x) (-> x* :dv double)))
             (is (= (:iv x) (:iv x*)))))
         (testing "maximum values"
           (let [x  {:_id "parser"
                     :name "test-03"
                     :fv   Float/MAX_VALUE
                     :dv   Double/MAX_VALUE
                     :iv   Integer/MAX_VALUE}
                 x' (-> x (json/stringify) (string->stream))
                 x* (json/parse x')]
             (is (instance? ByteArrayInputStream x'))
             (is (map? x*))
             (is (= (:name x) (:name x*)))
             (is (= (:fv x) (.floatValue ^BigDecimal (:fv x*))))
             (is (= (:dv x) (-> x* :dv double)))
             (is (= (:iv x) (:iv x*))))))
       (testing "parse json stringify"
         (testing "normal values"
           (let [x  {:_id "parser"
                     :name "test-04"
                     :fv   (float 3.11112)
                     :dv   (double 1.8111111125989)
                     :bdv  (bigdec 1.8333333333333332593184650249895639717578887939453125)
                     :biv  (bigint 1.8374872394873333e+89)
                     :iv   (int 72356)}
                 x' (json/stringify x)
                 x* (json/parse x')]
             (is (string? x'))
             (is (map? x*))
             (is (string? (:bdv x*)))
             (is (number? (:fv x*)))
             (is (= (:name x) (:name x*)))
             (is (= (:fv x) (-> x* :fv float)))
             (is (= (:dv x) (-> x* :dv double)))
             (is (= (:bdv x) (-> x* :bdv bigdec)))
             (is (= (:biv x) (:biv x*)))
             (is (= (:iv x) (:iv x*)))))
         (testing "minimum values"
           (let [x  {:_id "parser"
                     :name "test-05"
                     :fv   Float/MIN_VALUE
                     :dv   Double/MIN_VALUE
                     :iv   Integer/MIN_VALUE}
                 x' (json/stringify x)
                 x* (json/parse x')]
             (is (string? x'))
             (is (map? x*))
             (is (= (:name x) (:name x*)))
             (is (= (:fv x) (-> x* :fv float)))
             (is (= (:dv x) (-> x* :dv double)))
             (is (= (:iv x) (:iv x*)))))
         (testing "maximum values"
           (let [x  {:_id "parser"
                     :name "test-06"
                     :fv   Float/MAX_VALUE
                     :dv   Double/MAX_VALUE
                     :iv   Integer/MAX_VALUE}
                 x' (json/stringify x)
                 x* (json/parse x')]
             (is (string? x'))
             (is (map? x*))
             (is (= (:name x) (:name x*)))
             (is (= (:fv x) (.floatValue ^BigDecimal (:fv x*))))
             (is (= (:dv x) (-> x* :dv double)))
             (is (= (:iv x) (:iv x*))))))
       (testing "parse byte-array"
         (testing "normal values"
           (let [x  {:_id "parser"
                     :name "test-07"
                     :fv   (float 3.11112)
                     :dv   (double 1.8111111125989)
                     :bdv  (bigdec 1.8333333333333332593184650249895639717578887939453125)
                     :biv  (bigint 1.8374872394873333e+89)
                     :iv   (int 72356)}
                 x' (-> x json/stringify u-bytes/string->UTF8)
                 x* (json/parse x')]
             (is (bytes? x'))
             (is (map? x*))
             (is (string? (:bdv x*)))
             (is (number? (:fv x*)))
             (is (= (:name x) (:name x*)))
             (is (= (:fv x) (-> x* :fv float)))
             (is (= (:dv x) (-> x* :dv double)))
             (is (= (:bdv x) (-> x* :bdv bigdec)))
             (is (= (:biv x) (:biv x*)))
             (is (= (:iv x) (:iv x*)))))
         (testing "minimum values"
           (let [x  {:_id "parser"
                     :name "test-08"
                     :fv   Float/MIN_VALUE
                     :dv   Double/MIN_VALUE
                     :iv   Integer/MIN_VALUE}
                 x' (-> x json/stringify u-bytes/string->UTF8)
                 x* (json/parse x')]
             (is (bytes? x'))
             (is (map? x*))
             (is (= (:name x) (:name x*)))
             (is (= (:fv x) (-> x* :fv float)))
             (is (= (:dv x) (-> x* :dv double)))
             (is (= (:iv x) (:iv x*)))))
         (testing "maximum values"
           (let [x  {:_id "parser"
                     :name "test-09"
                     :fv   Float/MAX_VALUE
                     :dv   Double/MAX_VALUE
                     :iv   Integer/MAX_VALUE}
                 x' (-> x json/stringify u-bytes/string->UTF8)
                 x* (json/parse x')]
             (is (bytes? x'))
             (is (map? x*))
             (is (= (:name x) (:name x*)))
             (is (= (:fv x) (.floatValue ^BigDecimal (:fv x*))))
             (is (= (:dv x) (-> x* :dv double)))
             (is (= (:iv x) (:iv x*)))))))
     (testing ":fdb-json-bigdec-string: false"
       (json/encode-BigDecimal-as-string false)
       (testing "parse stream"
         (testing "normal values"
           (let [x  {:_id "parser"
                     :name "test-01"
                     :fv   (float 3.11112)
                     :dv   (double 1.8111111125989)
                     :bdv  (bigdec 1.8333333333333332593184650249895639717578887939453125)
                     :biv  (bigint 1.8374872394873333e+89)
                     :iv   (int 72356)}
                 x' (-> x (json/stringify) (string->stream))
                 x* (json/parse x')]
             (is (instance? ByteArrayInputStream x'))
             (is (map? x*))
             (is (number? (:bdv x*)))
             (is (number? (:fv x*)))
             (is (= (:name x) (:name x*)))
             (is (= (:fv x) (-> x* :fv float)))
             (is (= (:dv x) (-> x* :dv double)))
             (is (= (:bdv x) (:bdv x*)))
             (is (= (:biv x) (:biv x*)))
             (is (= (:iv x) (:iv x*)))))
         (testing "minimum values"
           (let [x  {:_id "parser"
                     :name "test-02"
                     :fv   Float/MIN_VALUE
                     :dv   Double/MIN_VALUE
                     :iv   Integer/MIN_VALUE}
                 x' (-> x (json/stringify) (string->stream))
                 x* (json/parse x')]
             (is (instance? ByteArrayInputStream x'))
             (is (map? x*))
             (is (= (:name x) (:name x*)))
             (is (= (:fv x) (-> x* :fv float)))
             (is (= (:dv x) (-> x* :dv double)))
             (is (= (:iv x) (:iv x*)))))
         (testing "maximum values"
           (let [x  {:_id "parser"
                     :name "test-03"
                     :fv   Float/MAX_VALUE
                     :dv   Double/MAX_VALUE
                     :iv   Integer/MAX_VALUE}
                 x' (-> x (json/stringify) (string->stream))
                 x* (json/parse x')]
             (is (instance? ByteArrayInputStream x'))
             (is (map? x*))
             (is (= (:name x) (:name x*)))
             (is (= (:fv x) (.floatValue ^BigDecimal (:fv x*))))
             (is (= (:dv x) (-> x* :dv double)))
             (is (= (:iv x) (:iv x*))))))
       (testing "parse json stringify"
         (testing "normal values"
           (let [x  {:_id "parser"
                     :name "test-04"
                     :fv   (float 3.11112)
                     :dv   (double 1.8111111125989)
                     :bdv  (bigdec 1.8333333333333332593184650249895639717578887939453125)
                     :biv  (bigint 1.8374872394873333e+89)
                     :iv   (int 72356)}
                 x' (json/stringify x)
                 x* (json/parse x')]
             (is (string? x'))
             (is (map? x*))
             (is (number? (:bdv x*)))
             (is (number? (:fv x*)))
             (is (= (:name x) (:name x*)))
             (is (= (:fv x) (-> x* :fv float)))
             (is (= (:dv x) (-> x* :dv double)))
             (is (= (:bdv x) (:bdv x*)))
             (is (= (:biv x) (:biv x*)))
             (is (= (:iv x) (:iv x*)))))
         (testing "minimum values"
           (let [x  {:_id "parser"
                     :name "test-05"
                     :fv   Float/MIN_VALUE
                     :dv   Double/MIN_VALUE
                     :iv   Integer/MIN_VALUE}
                 x' (json/stringify x)
                 x* (json/parse x')]
             (is (string? x'))
             (is (map? x*))
             (is (= (:name x) (:name x*)))
             (is (= (:fv x) (-> x* :fv float)))
             (is (= (:dv x) (-> x* :dv double)))
             (is (= (:iv x) (:iv x*)))))
         (testing "maximum values"
           (let [x  {:_id "parser"
                     :name "test-06"
                     :fv   Float/MAX_VALUE
                     :dv   Double/MAX_VALUE
                     :iv   Integer/MAX_VALUE}
                 x' (json/stringify x)
                 x* (json/parse x')]
             (is (string? x'))
             (is (map? x*))
             (is (= (:name x) (:name x*)))
             (is (= (:fv x) (.floatValue ^BigDecimal (:fv x*))))
             (is (= (:dv x) (-> x* :dv double)))
             (is (= (:iv x) (:iv x*))))))
       (testing "parse byte-array"
         (testing "normal values"
           (let [x  {:_id "parser"
                     :name "test-07"
                     :fv   (float 3.11112)
                     :dv   (double 1.8111111125989)
                     :bdv  (bigdec 1.8333333333333332593184650249895639717578887939453125)
                     :biv  (bigint 1.8374872394873333e+89)
                     :iv   (int 72356)}
                 x' (-> x json/stringify u-bytes/string->UTF8)
                 x* (json/parse x')]
             (is (bytes? x'))
             (is (map? x*))
             (is (number? (:bdv x*)))
             (is (number? (:fv x*)))
             (is (= (:name x) (:name x*)))
             (is (= (:fv x) (-> x* :fv float)))
             (is (= (:dv x) (-> x* :dv double)))
             (is (= (:bdv x) (:bdv x*)))
             (is (= (:biv x) (:biv x*)))
             (is (= (:iv x) (:iv x*)))))
         (testing "minimum values"
           (let [x  {:_id "parser"
                     :name "test-08"
                     :fv   Float/MIN_VALUE
                     :dv   Double/MIN_VALUE
                     :iv   Integer/MIN_VALUE}
                 x' (-> x json/stringify u-bytes/string->UTF8)
                 x* (json/parse x')]
             (is (bytes? x'))
             (is (map? x*))
             (is (= (:name x) (:name x*)))
             (is (= (:fv x) (-> x* :fv float)))
             (is (= (:dv x) (-> x* :dv double)))
             (is (= (:iv x) (:iv x*)))))
         (testing "maximum values"
           (let [x  {:_id "parser"
                     :name "test-09"
                     :fv   Float/MAX_VALUE
                     :dv   Double/MAX_VALUE
                     :iv   Integer/MAX_VALUE}
                 x' (-> x json/stringify u-bytes/string->UTF8)
                 x* (json/parse x')]
             (is (bytes? x'))
             (is (map? x*))
             (is (= (:name x) (:name x*)))
             (is (= (:fv x) (.floatValue ^BigDecimal (:fv x*))))
             (is (= (:dv x) (-> x* :dv double)))
             (is (= (:iv x) (:iv x*)))))))))

#?(:cljs
   (deftest db-util-json-test
     (testing "boolean"
       (is (= (float 0) (double 0))))))
