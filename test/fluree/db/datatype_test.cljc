(ns fluree.db.datatype-test
  (:require #?(:clj  [clojure.test :refer [deftest are]]
               :cljs [cljs.test :refer-macros [deftest are]])
            [fluree.db.constants :as const]
            [fluree.db.datatype :refer [coerce]])
  #?(:clj (:import (java.time LocalDate LocalTime OffsetDateTime OffsetTime
                              ZoneOffset))))

(deftest coerce-test
  (are [coerced-value value datatype]
    (= coerced-value (coerce value datatype))

    ;; test format:
    ;; expected input const/$xsd:type

    ;; string
    "foo" "foo" const/$xsd:string
    nil 42 const/$xsd:string

    ;; anyURI
    "foo" "foo" const/$xsd:anyURI
    nil 42 const/$xsd:anyURI

    ;; boolean
    true "true" const/$xsd:boolean
    false "false" const/$xsd:boolean
    true true const/$xsd:boolean
    false false const/$xsd:boolean
    nil "foo" const/$xsd:boolean

    ;; date
    #?(:clj
       (OffsetDateTime/of 1980 10 5 0 0 0 0
                          (ZoneOffset/of "Z"))
       :cljs
       #inst "1980-10-05T00:00:00.000-00:00") "1980-10-5Z" const/$xsd:date
    #?(:clj
       (LocalDate/of 1980 10 5)
       :cljs
       (js/Date. "1980-10-05T00:00:00")) "1980-10-5" const/$xsd:date
    #?(:clj
       (OffsetDateTime/of 2022 1 5 0 0 0 0
                          (ZoneOffset/of "Z"))
       :cljs
       #inst "2022-01-05T00:00:00.000-00:00") "2022-01-05Z" const/$xsd:date
    #?(:clj
       (LocalDate/of 2022 1 5)
       :cljs
       (js/Date. "2022-01-05T00:00:00")) "2022-01-05" const/$xsd:date

    nil "foo" const/$xsd:date

    ;; time
    #?(:clj
       (LocalTime/of 12 42 0)
       :cljs
       (js/Date. "1970-01-01T12:42:00")) "12:42:00" const/$xsd:time

    #?(:clj
       (OffsetTime/of 12 42 0 0
                      (ZoneOffset/of "Z"))
       :cljs
       #inst "1970-01-01T12:42:00.000-00:00") "12:42:00Z" const/$xsd:time

    #?(:clj
       (LocalTime/of 12 42 5)
       :cljs
       (js/Date. "1970-01-01T12:42:05")) "12:42:5" const/$xsd:time

    #?(:clj
       (OffsetTime/of 12 42 5 0
                      (ZoneOffset/of "Z"))
       :cljs
       #inst "1970-01-01T12:42:05.000-00:00") "12:42:5Z" const/$xsd:time

    nil "foo" const/$xsd:time

    ;; datetime
    #?(:clj
       (OffsetDateTime/of 1980 10 5 11 23 0 0
                          (ZoneOffset/of "Z"))
       :cljs
       #inst "1980-10-05T11:23:00.000-00:00") "1980-10-5T11:23:00Z" const/$xsd:dateTime

    nil "foo" const/$xsd:dateTime

    ;; decimal
    #?(:clj (BigDecimal. "3.14") :cljs 3.14) 3.14 const/$xsd:decimal
    #?(:clj (BigDecimal. "3.14") :cljs 3.14) "3.14" const/$xsd:decimal
    #?(:clj (BigDecimal. "42.0") :cljs 42) 42 const/$xsd:decimal
    nil "foo" const/$xsd:decimal

    ;; double
    #?(:clj Double/POSITIVE_INFINITY
       :cljs js/Number.POSITIVE_INFINITY) "INF" const/$xsd:double
    #?(:clj Double/NEGATIVE_INFINITY
       :cljs js/Number.NEGATIVE_INFINITY) "-INF" const/$xsd:double
    3.14 3.14 const/$xsd:double
    3.0 3 const/$xsd:double
    nil "foo" const/$xsd:double

    ;; float
    #?(:clj Float/POSITIVE_INFINITY
       :cljs js/Number.POSITIVE_INFINITY) "INF" const/$xsd:float
    #?(:clj Float/NEGATIVE_INFINITY
       :cljs js/Number.NEGATIVE_INFINITY) "-INF" const/$xsd:float
    3.14 3.14 const/$xsd:float
    3.0 3 const/$xsd:float
    nil "foo" const/$xsd:float

    ;; integer / int / unsignedInt / etc.
    42 42 const/$xsd:integer
    42 "42" const/$xsd:integer
    -42 -42 const/$xsd:integer
    0 0 const/$xsd:integer
    nil 3.14 const/$xsd:integer
    nil "3.14" const/$xsd:integer

    42 42 const/$xsd:int
    42 "42" const/$xsd:int
    -42 -42 const/$xsd:int
    0 0 const/$xsd:int
    nil 3.14 const/$xsd:int
    nil "3.14" const/$xsd:int
    #?(:clj nil :cljs 2147483648) 2147483648 const/$xsd:int
    #?(:clj nil :cljs 2147483648) "2147483648" const/$xsd:int
    #?(:clj nil :cljs -2147483649) -2147483649 const/$xsd:int
    #?(:clj nil :cljs -2147483649) "-2147483649" const/$xsd:int

    42 42 const/$xsd:unsignedInt
    0 0 const/$xsd:unsignedInt
    42 "42" const/$xsd:unsignedInt
    nil -42 const/$xsd:unsignedInt
    nil "-42" const/$xsd:unsignedInt
    nil 3.14 const/$xsd:unsignedInt
    nil "3.14" const/$xsd:unsignedInt

    42 42 const/$xsd:nonNegativeInteger
    0 0 const/$xsd:nonNegativeInteger
    42 "42" const/$xsd:nonNegativeInteger
    0 "0" const/$xsd:nonNegativeInteger
    nil -42 const/$xsd:nonNegativeInteger
    nil "-42" const/$xsd:nonNegativeInteger
    nil 3.14 const/$xsd:nonNegativeInteger
    nil "3.14" const/$xsd:nonNegativeInteger

    42 42 const/$xsd:positiveInteger
    42 "42" const/$xsd:positiveInteger
    nil 0 const/$xsd:positiveInteger
    nil "0" const/$xsd:positiveInteger
    nil -42 const/$xsd:positiveInteger
    nil "-42" const/$xsd:positiveInteger
    nil 3.14 const/$xsd:positiveInteger
    nil "3.14" const/$xsd:positiveInteger

    nil 42 const/$xsd:negativeInteger
    nil "42" const/$xsd:negativeInteger
    nil 0 const/$xsd:negativeInteger
    nil "0" const/$xsd:negativeInteger
    -42 -42 const/$xsd:negativeInteger
    -42 "-42" const/$xsd:negativeInteger
    nil -3.14 const/$xsd:negativeInteger
    nil "-3.14" const/$xsd:negativeInteger

    nil 42 const/$xsd:nonPositiveInteger
    nil "42" const/$xsd:nonPositiveInteger
    0 0 const/$xsd:nonPositiveInteger
    0 "0" const/$xsd:nonPositiveInteger
    -42 -42 const/$xsd:nonPositiveInteger
    -42 "-42" const/$xsd:nonPositiveInteger
    nil -3.14 const/$xsd:nonPositiveInteger
    nil "-3.14" const/$xsd:nonPositiveInteger

    nil -42 const/$xsd:nonNegativeInteger
    nil "-42" const/$xsd:nonNegativeInteger
    0 0 const/$xsd:nonNegativeInteger
    0 "0" const/$xsd:nonNegativeInteger
    42 42 const/$xsd:nonNegativeInteger
    42 "42" const/$xsd:nonNegativeInteger
    nil 3.14 const/$xsd:nonNegativeInteger
    nil "3.14" const/$xsd:nonNegativeInteger

    ;; long & unsignedLong
    42 42 const/$xsd:long
    42 "42" const/$xsd:long
    -42 -42 const/$xsd:long
    -42 "-42" const/$xsd:long
    nil 3.14 const/$xsd:long
    nil "3.14" const/$xsd:long

    42 42 const/$xsd:unsignedLong
    42 "42" const/$xsd:unsignedLong
    nil -42 const/$xsd:unsignedLong
    nil "-42" const/$xsd:unsignedLong
    nil 3.14 const/$xsd:unsignedLong
    nil "3.14" const/$xsd:unsignedLong

    ;; short & unsignedShort
    42 42 const/$xsd:short
    42 "42" const/$xsd:short
    -42 -42 const/$xsd:short
    -42 "-42" const/$xsd:short
    nil 3.14 const/$xsd:short
    nil "3.14" const/$xsd:short
    #?(:clj nil :cljs 32768) "32768" const/$xsd:short
    #?(:clj nil :cljs 32768) 32768 const/$xsd:short
    #?(:clj nil :cljs -32769) "-32769" const/$xsd:short
    #?(:clj nil :cljs -32769) -32769 const/$xsd:short

    42 42 const/$xsd:unsignedShort
    42 "42" const/$xsd:unsignedShort
    nil -42 const/$xsd:unsignedShort
    nil "-42" const/$xsd:unsignedShort
    nil 3.14 const/$xsd:unsignedShort
    nil "3.14" const/$xsd:unsignedShort
    #?(:clj nil :cljs 32768) 32768 const/$xsd:unsignedShort
    #?(:clj nil :cljs 32768) "32768" const/$xsd:unsignedShort

    ;; byte & unsignedByte
    42 42 const/$xsd:byte
    42 "42" const/$xsd:byte
    -42 -42 const/$xsd:byte
    -42 "-42" const/$xsd:byte
    nil 3.14 const/$xsd:byte
    nil "3.14" const/$xsd:byte
    #?(:clj nil :cljs 128) 128 const/$xsd:byte
    #?(:clj nil :cljs 128) "128" const/$xsd:byte
    #?(:clj nil :cljs -129) -129 const/$xsd:byte
    #?(:clj nil :cljs -129) "-129" const/$xsd:byte

    42 42 const/$xsd:unsignedByte
    42 "42" const/$xsd:unsignedByte
    nil -42 const/$xsd:unsignedByte
    nil "-42" const/$xsd:unsignedByte
    nil 3.14 const/$xsd:unsignedByte
    nil "3.14" const/$xsd:unsignedByte
    #?(:clj nil :cljs 32768) 32768 const/$xsd:unsignedByte
    #?(:clj nil :cljs 32768) "32768" const/$xsd:unsignedByte

    ;; normalizedString
    "foo  bar  baz" "foo  bar \tbaz" const/$xsd:normalizedString
    "foo     bar     baz" "foo
    bar     baz" const/$xsd:normalizedString
    " foo   bar  baz " " foo   bar  baz " const/$xsd:normalizedString

    ;; token
    "foo bar baz" "  foo    bar \t\t\t baz    " const/$xsd:token
    "foo bar baz" "foo
    bar          baz" const/$xsd:token

    ;; language
    "en" "en " const/$xsd:language
    "en-US" " en-US" const/$xsd:language
    "es-MX" "\tes-MX" const/$xsd:language

    ;; non-coerced datatypes
    "whatever" "whatever" const/$xsd:hexBinary
    "thingy" "thingy" const/$xsd:duration))
