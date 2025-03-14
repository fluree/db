(ns fluree.db.datatype-test
  (:require [clojure.test :refer [deftest testing are is]]
            [fluree.db.constants :as const]
            [fluree.db.datatype :refer [coerce]])
  #?(:clj (:import (java.time LocalDate LocalTime OffsetDateTime OffsetTime
                              LocalDateTime ZoneOffset))))

(deftest coerce-test
  (testing "strings"
    (is (= "foo" (coerce "foo" const/$xsd:string)))
    (is (= nil (coerce 42 const/$xsd:string))))

  (testing "@id"
    (is (= "foo" (coerce "foo" const/$id)))
    (is (= 42 (coerce 42 const/$id))))
  (testing "boolean"
    (is (= true (coerce "true" const/$xsd:boolean)))
    (is (= false (coerce "false" const/$xsd:boolean)))
    (is (= true (coerce true const/$xsd:boolean)))
    (is (= false (coerce false const/$xsd:boolean)))
    (is (= nil (coerce "foo" const/$xsd:boolean))))

  (testing "date"
    (is (= nil
           (coerce "1980-10-5Z" const/$xsd:date)))
    (is (= nil
           (coerce "1980-10-5" const/$xsd:date)))
    (is (= #?(:clj
              (OffsetDateTime/of 2022 1 5 0 0 0 0 (ZoneOffset/of "Z"))
              :cljs
              #inst "2022-01-05T00:00:00.000-00:00")
           (coerce "2022-01-05Z" const/$xsd:date)))

    (is (= #?(:clj
              (LocalDate/of 2022 1 5)
              :cljs
              (js/Date. "2022-01-05T00:00:00"))
           (coerce "2022-01-05" const/$xsd:date)))

    (is (= nil
           (coerce "foo" const/$xsd:date))))

  (testing "time"
    (is (= #?(:clj
              (LocalTime/of 12 42 0)
              :cljs
              (js/Date. "1970-01-01T12:42:00"))
           (coerce "12:42:00" const/$xsd:time)))
    (is (= #?(:clj
              (OffsetTime/of 12 42 0 0 (ZoneOffset/of "Z"))
              :cljs
              #inst "1970-01-01T12:42:00.000-00:00")
           (coerce "12:42:00Z" const/$xsd:time)))
    (is (= #?(:clj
              (OffsetTime/of 9 30 10 0 (ZoneOffset/of "-06:00"))
              :cljs
              #inst "1970-01-01T09:30:10.000-06:00")
           (coerce "09:30:10-06:00" const/$xsd:time)))
    (is (= nil
           (coerce "12:42:5" const/$xsd:time)))
    (is (= nil
           (coerce "12:42:5Z" const/$xsd:time)))
    (testing "nanoseconds"
      (is (= #?(:clj
                (OffsetTime/of 11 14 32 833000000 (ZoneOffset/of "Z"))
                :cljs
                #inst "1970-01-01T11:14:32.833-00:00")
             (coerce "11:14:32.833Z" const/$xsd:time)))
      (is (= #?(:clj
                (OffsetTime/of 11 14 32 830000000 (ZoneOffset/of "Z"))
                :cljs
                #inst "1970-01-01T11:14:32.83-00:00")
             (coerce "11:14:32.83Z" const/$xsd:time)))
      (is (= #?(:clj
                (OffsetTime/of 11 14 32 833100000 (ZoneOffset/of "Z"))
                ;;only  3 digits of ms allowed in js
                :cljs
                nil)
             (coerce "11:14:32.8331Z" const/$xsd:time)))
      (is (= #?(:clj
                (LocalTime/of 12 42 0 430000000)
                :cljs
                (js/Date. "1970-01-01T12:42:00.43"))
             (coerce "12:42:00.43" const/$xsd:time))))
    (is (= #?(:clj
              (LocalTime/of 12 42 0 433100000)
              ;;only  3 digits of ms allowed in js
              :cljs
              nil)
           (coerce "12:42:00.4331" const/$xsd:time)))
    (is (= nil (coerce "foo" const/$xsd:time))))

  (testing "datetime"
    (is (= nil
           (coerce "1980-10-5T11:23:00Z" const/$xsd:dateTime))
        "don't accept non-8601 timestamps")
    (is (= #?(:clj
              (OffsetDateTime/of 1980 10 5 11 23 0 0 (ZoneOffset/of "-06:00"))
              :cljs
              #inst "1980-10-05T17:23:00.000-00:00")
           (coerce "1980-10-05T11:23:00-06:00" const/$xsd:dateTime))
        "offset")
    (is (= #?(:clj
              (OffsetDateTime/of 1980 10 5 11 23 0 0 (ZoneOffset/of "Z"))
              :cljs
              #inst "1980-10-05T11:23:00.000-00:00")
           (coerce "1980-10-05T11:23:00Z" const/$xsd:dateTime))
        "z")
    #?(:clj (testing "nanoseconds"
              (is (= (OffsetDateTime/of 2021 9 24 11 14 32 833000000 (ZoneOffset/of "Z"))
                     (coerce "2021-09-24T11:14:32.833Z" const/$xsd:dateTime)))
              (is (= (OffsetDateTime/of 2021 9 24 11 14 32 833100000 (ZoneOffset/of "Z"))
                     (coerce "2021-09-24T11:14:32.8331Z" const/$xsd:dateTime)))
              (is (= (OffsetDateTime/of 2021 9 24 11 14 32 830000000 (ZoneOffset/of "Z"))
                     (coerce "2021-09-24T11:14:32.83Z" const/$xsd:dateTime)))
              (is (= (LocalDateTime/of 2021 9 24 11 14 32 833000000)
                     (coerce "2021-09-24T11:14:32.833" const/$xsd:dateTime)))
              (is (= (LocalDateTime/of 2021 9 24 11 14 32 833100000)
                     (coerce "2021-09-24T11:14:32.8331" const/$xsd:dateTime)))
              (is (= (LocalDateTime/of 2021 9 24 11 14 32 830000000)
                     (coerce "2021-09-24T11:14:32.83" const/$xsd:dateTime)))))
    (is (= nil (coerce "foo" const/$xsd:dateTime))))

  (testing "decimal"
    (is (= #?(:clj (bigdec "3.14") :cljs 3.14)
           (coerce 3.14 const/$xsd:decimal)))
    (is (= #?(:clj (bigdec "3.14") :cljs 3.14)
           (coerce "3.14" const/$xsd:decimal)))
    (is (= #?(:clj (bigdec "42.0") :cljs 42)
           (coerce 42 const/$xsd:decimal)))
    (is (= #?(:clj (bigdec 99.99) :cljs 99.99)
           (coerce #?(:clj (bigdec 99.99) :cljs 99.99) const/$xsd:decimal)))
    (is (= nil
           (coerce "foo" const/$xsd:decimal))))

  (testing "double"
    (is (= #?(:clj Double/POSITIVE_INFINITY
              :cljs js/Number.POSITIVE_INFINITY)
           (coerce "INF" const/$xsd:double)))
    (is (= #?(:clj Double/NEGATIVE_INFINITY
              :cljs js/Number.NEGATIVE_INFINITY)
           (coerce "-INF" const/$xsd:double)))
    (is (= 3.14
           (coerce 3.14 const/$xsd:double)))
    (is (= 3.0
           (coerce 3 const/$xsd:double)))
    (is (= nil
           (coerce "foo" const/$xsd:double))))

  (testing "float"
    (is (= #?(:clj Float/POSITIVE_INFINITY
              :cljs js/Number.POSITIVE_INFINITY)
           (coerce "INF" const/$xsd:float)))
    (is (= #?(:clj Float/NEGATIVE_INFINITY
              :cljs js/Number.NEGATIVE_INFINITY)
           (coerce "-INF" const/$xsd:float)))
    (is (= 3.14
           (coerce 3.14 const/$xsd:float)))
    (is (= 3.0
           (coerce 3 const/$xsd:float)))
    (is (= nil
           (coerce "foo" const/$xsd:float))))

  (testing "integer"
    (is (= 42 (coerce 42 const/$xsd:integer)))
    (is (= 42 (coerce "42" const/$xsd:integer)))
    (is (= -42 (coerce -42 const/$xsd:integer)))
    (is (= 0 (coerce 0 const/$xsd:integer)))
    (is (= nil (coerce 3.14 const/$xsd:integer)))
    (is (= nil (coerce "3.14" const/$xsd:integer))))

  (testing "int"
    (is (= 42 (coerce 42 const/$xsd:int)))
    (is (= 42 (coerce "42" const/$xsd:int)))
    (is (= -42 (coerce -42 const/$xsd:int)))
    (is (= 0 (coerce 0 const/$xsd:int)))
    (is (= nil (coerce 3.14 const/$xsd:int)))
    (is (= nil (coerce "3.14" const/$xsd:int)))
    (is (= #?(:clj nil :cljs 2147483648) (coerce 2147483648 const/$xsd:int)))
    (is (= #?(:clj nil :cljs 2147483648) (coerce "2147483648" const/$xsd:int)))
    (is (= #?(:clj nil :cljs -2147483649) (coerce -2147483649 const/$xsd:int)))
    (is (= #?(:clj nil :cljs -2147483649) (coerce "-2147483649" const/$xsd:int))))

  (testing "unsignedInt"
    (is (= 42 (coerce 42 const/$xsd:unsignedInt)))
    (is (= 0 (coerce 0 const/$xsd:unsignedInt)))
    (is (= 42 (coerce "42" const/$xsd:unsignedInt)))
    (is (= nil (coerce -42 const/$xsd:unsignedInt)))
    (is (= nil (coerce "-42" const/$xsd:unsignedInt)))
    (is (= nil (coerce 3.14 const/$xsd:unsignedInt)))
    (is (= nil (coerce "3.14" const/$xsd:unsignedInt))))

  (testing "natural integer"
    (is (= 42 (coerce 42 const/$xsd:nonNegativeInteger)))
    (is (= 0 (coerce 0 const/$xsd:nonNegativeInteger)))
    (is (= 42 (coerce "42" const/$xsd:nonNegativeInteger)))
    (is (= 0 (coerce "0" const/$xsd:nonNegativeInteger)))
    (is (= nil (coerce -42 const/$xsd:nonNegativeInteger)))
    (is (= nil (coerce "-42" const/$xsd:nonNegativeInteger)))
    (is (= nil (coerce 3.14 const/$xsd:nonNegativeInteger)))
    (is (= nil (coerce "3.14" const/$xsd:nonNegativeInteger))))

  (testing "positive integer"
    (is (= 42 (coerce 42 const/$xsd:positiveInteger)))
    (is (= 42 (coerce "42" const/$xsd:positiveInteger)))
    (is (= nil (coerce 0 const/$xsd:positiveInteger)))
    (is (= nil (coerce "0" const/$xsd:positiveInteger)))
    (is (= nil (coerce -42 const/$xsd:positiveInteger)))
    (is (= nil (coerce "-42" const/$xsd:positiveInteger)))
    (is (= nil (coerce 3.14 const/$xsd:positiveInteger)))
    (is (= nil (coerce "3.14" const/$xsd:positiveInteger))))

  (testing "negative integer"
    (is (= nil (coerce 42 const/$xsd:negativeInteger)))
    (is (= nil (coerce "42" const/$xsd:negativeInteger)))
    (is (= nil (coerce 0 const/$xsd:negativeInteger)))
    (is (= nil (coerce "0" const/$xsd:negativeInteger)))
    (is (= -42 (coerce -42 const/$xsd:negativeInteger)))
    (is (= -42 (coerce "-42" const/$xsd:negativeInteger)))
    (is (= nil (coerce -3.14 const/$xsd:negativeInteger)))
    (is (= nil (coerce "-3.14" const/$xsd:negativeInteger))))

  (testing "non-positive integer"
    (is (= nil (coerce 42 const/$xsd:nonPositiveInteger)))
    (is (= nil (coerce "42" const/$xsd:nonPositiveInteger)))
    (is (= 0 (coerce 0 const/$xsd:nonPositiveInteger)))
    (is (= 0 (coerce "0" const/$xsd:nonPositiveInteger)))
    (is (= -42 (coerce -42 const/$xsd:nonPositiveInteger)))
    (is (= -42 (coerce "-42" const/$xsd:nonPositiveInteger)))
    (is (= nil (coerce -3.14 const/$xsd:nonPositiveInteger)))
    (is (= nil (coerce "-3.14" const/$xsd:nonPositiveInteger))))

  (testing "long"
    (is (= 42 (coerce 42 const/$xsd:long)))
    (is (= 42 (coerce "42" const/$xsd:long)))
    (is (= -42 (coerce -42 const/$xsd:long)))
    (is (= -42 (coerce "-42" const/$xsd:long)))
    (is (= nil (coerce 3.14 const/$xsd:long)))
    (is (= nil (coerce "3.14" const/$xsd:long))))

  (testing "unsigned long"
    (is (= 42 (coerce 42 const/$xsd:unsignedLong)))
    (is (= 42 (coerce "42" const/$xsd:unsignedLong)))
    (is (= nil (coerce -42 const/$xsd:unsignedLong)))
    (is (= nil (coerce "-42" const/$xsd:unsignedLong)))
    (is (= nil (coerce 3.14 const/$xsd:unsignedLong)))
    (is (= nil (coerce "3.14" const/$xsd:unsignedLong))))

  (testing "short"
    (is (= 42 (coerce 42 const/$xsd:short)))
    (is (= 42 (coerce "42" const/$xsd:short)))
    (is (= -42 (coerce -42 const/$xsd:short)))
    (is (= -42 (coerce "-42" const/$xsd:short)))
    (is (= nil (coerce 3.14 const/$xsd:short)))
    (is (= nil (coerce "3.14" const/$xsd:short)))
    (is (= #?(:clj nil :cljs 32768) (coerce "32768" const/$xsd:short)))
    (is (= #?(:clj nil :cljs 32768) (coerce 32768 const/$xsd:short)))
    (is (= #?(:clj nil :cljs -32769) (coerce "-32769" const/$xsd:short)))
    (is (= #?(:clj nil :cljs -32769) (coerce -32769 const/$xsd:short))))

  (testing "unsigned short"
    (is (= 42 (coerce 42 const/$xsd:unsignedShort)))
    (is (= 42 (coerce "42" const/$xsd:unsignedShort)))
    (is (= nil (coerce -42 const/$xsd:unsignedShort)))
    (is (= nil (coerce "-42" const/$xsd:unsignedShort)))
    (is (= nil (coerce 3.14 const/$xsd:unsignedShort)))
    (is (= nil (coerce "3.14" const/$xsd:unsignedShort)))
    (is (= 32768 (coerce 32768 const/$xsd:unsignedShort)))
    (is (= 32768 (coerce "32768" const/$xsd:unsignedShort)))
    (is (= 65535 (coerce "65535" const/$xsd:unsignedShort)))
    (is (= nil (coerce "65536" const/$xsd:unsignedShort))))

  (testing "byte"
    (is (= 42 (coerce 42 const/$xsd:byte)))
    (is (= 42 (coerce "42" const/$xsd:byte)))
    (is (= -42 (coerce -42 const/$xsd:byte)))
    (is (= -42 (coerce "-42" const/$xsd:byte)))
    (is (= nil (coerce 3.14 const/$xsd:byte)))
    (is (= nil (coerce "3.14" const/$xsd:byte)))
    (is (= #?(:clj nil :cljs 128) (coerce 128 const/$xsd:byte)))
    (is (= #?(:clj nil :cljs 128) (coerce "128" const/$xsd:byte)))
    (is (= #?(:clj nil :cljs -129) (coerce -129 const/$xsd:byte)))
    (is (= #?(:clj nil :cljs -129) (coerce "-129" const/$xsd:byte))))

  (testing "unsigned byte"
    (is (= 42 (coerce 42 const/$xsd:unsignedByte)))
    (is (= 42 (coerce "42" const/$xsd:unsignedByte)))
    (is (= nil (coerce -42 const/$xsd:unsignedByte)))
    (is (= nil (coerce "-42" const/$xsd:unsignedByte)))
    (is (= nil (coerce 3.14 const/$xsd:unsignedByte)))
    (is (= nil (coerce "3.14" const/$xsd:unsignedByte)))
    (is (= #?(:clj nil :cljs 32768) (coerce 32768 const/$xsd:unsignedByte)))
    (is (= #?(:clj nil :cljs 32768) (coerce "32768" const/$xsd:unsignedByte))))

  (testing "normalized string"
    (is (= "foo  bar  baz" (coerce "foo  bar \tbaz" const/$xsd:normalizedString)))
    (is (= "foo     bar     baz" (coerce "foo
    bar     baz" const/$xsd:normalizedString)))
    (is (= " foo   bar  baz " (coerce " foo   bar  baz " const/$xsd:normalizedString))))

  (testing "token"
    (is (= "foo bar baz" (coerce "  foo    bar \t\t\t baz    " const/$xsd:token)))
    (is (= "foo bar baz" (coerce "foo
    bar          baz" const/$xsd:token))))

  (testing "language"
    (is (= "en" (coerce "en " const/$xsd:language)))
    (is (= "en-US" (coerce " en-US" const/$xsd:language)))
    (is (= "es-MX" (coerce "\tes-MX" const/$xsd:language))))

  (testing "json"
    (is (= "{\":edn\":\"data\",\":is\":[\"cool\",\"right?\",1,false,1]}"
           (coerce {:edn "data" :is ["cool" "right?" 1 false 1.0]} const/$rdf:json)))
    (is (= "{\"is\":[\"cool\",\"right?\",1,false,1],\"json\":\"data\"}"
           (coerce {"json" "data" "is" ["cool" "right?" 1 false 1.0]} const/$rdf:json))))

  (testing "non-coerced datatypes"
    (is (= "whatever" (coerce "whatever" const/$xsd:hexBinary)))
    (is (= "thingy" (coerce "thingy" const/$xsd:duration)))))
