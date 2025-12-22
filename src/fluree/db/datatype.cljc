(ns fluree.db.datatype
  (:require #?@(:clj  [[fluree.db.util.clj-const :as uc]
                       [time-literals.read-write :as time-literals]]
                :cljs [[fluree.db.util.cljs-const :as uc]])
            [clojure.string :as str]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [fluree.db.vector.scoring :as vector.score]
            [fluree.json-ld :as json-ld])
  #?(:clj (:import (java.time LocalDate LocalTime LocalDateTime
                              OffsetDateTime OffsetTime ZoneOffset))))

#?(:clj (set! *warn-on-reflection* true))

#?(:clj (time-literals/print-time-literals-clj!))

(def default-data-types
  {const/iri-id                     const/$id
   const/iri-string                 const/$xsd:string
   const/iri-xsd-boolean            const/$xsd:boolean
   const/iri-xsd-date               const/$xsd:date
   const/iri-xsd-dateTime           const/$xsd:dateTime
   const/iri-xsd-decimal            const/$xsd:decimal
   const/iri-xsd-double             const/$xsd:double
   const/iri-xsd-integer            const/$xsd:integer
   const/iri-long                   const/$xsd:long
   const/iri-xsd-int                const/$xsd:int
   const/iri-xsd-short              const/$xsd:short
   const/iri-xsd-float              const/$xsd:float
   const/iri-xsd-unsignedLong       const/$xsd:unsignedLong
   const/iri-xsd-unsignedInt        const/$xsd:unsignedInt
   const/iri-xsd-unsignedShort      const/$xsd:unsignedShort
   const/iri-xsd-positiveInteger    const/$xsd:positiveInteger
   const/iri-xsd-nonPositiveInteger const/$xsd:nonPositiveInteger
   const/iri-xsd-negativeInteger    const/$xsd:negativeInteger
   const/iri-xsd-nonNegativeInteger const/$xsd:nonNegativeInteger
   const/iri-xsd-duration           const/$xsd:duration
   const/iri-xsd-gDay               const/$xsd:gDay
   const/iri-xsd-gMonth             const/$xsd:gMonth
   const/iri-xsd-gMonthDay          const/$xsd:gMonthDay
   const/iri-xsd-gYear              const/$xsd:gYear
   const/iri-xsd-gYearMonth         const/$xsd:gYearMonth
   const/iri-xsd-time               const/$xsd:time
   const/iri-xsd-normalizedString   const/$xsd:normalizedString
   const/iri-xsd-token              const/$xsd:token
   const/iri-xsd-language           const/$xsd:language
   const/iri-xsd-byte               const/$xsd:byte
   const/iri-xsd-unsignedByte       const/$xsd:unsignedByte
   const/iri-xsd-hexBinary          const/$xsd:hexBinary
   const/iri-xsd-base64Binary       const/$xsd:base64Binary
   const/iri-anyURI                 const/$xsd:anyURI
   const/iri-lang-string            const/$rdf:langString
   const/iri-rdf-json               const/$rdf:json
   const/iri-vector                 const/$fluree:vector})

(def JSON-LD-inferable-types
  "Note these are inferable types for JSON-LD.
  Turtle (ttl) also has inferable types that differ
  slightly. In ttl, a normal fraction number (e.g. 1.23)
  would translate into xsd:decimal, and a number that uses
  'e' notation would translate into xsd:double (e.g. 1.23e0):
  https://www.w3.org/TR/turtle/#abbrev"
  #{const/$xsd:string
    const/$xsd:boolean
    const/$xsd:integer
    const/$xsd:double})

(def JSON-LD-inferable-iris
  #{const/iri-string
    const/iri-xsd-boolean
    const/iri-xsd-integer
    const/iri-xsd-double})

(defn inferable?
  "Returns true if the provided data type is one that can be inferred from the value.
  Note this is for JSON-LD inferable types only:
  https://www.w3.org/TR/json-ld11/#conversion-of-native-data-types
  Includes: xsd:string, xsd:boolean, xsd:integer, xsd:double "
  [dt]
  (contains? JSON-LD-inferable-types dt))

(defn inferable-iri?
  [dt-iri]
  (contains? JSON-LD-inferable-iris dt-iri))

(def iso8601-offset-pattern
  "(Z|(?:[+-][0-9]{2}:[0-9]{2}))?")

(def iso8601-date-component-pattern
  "This is slightly more forgiving than the xsd:date spec:
  http://books.xmlschemata.org/relaxng/ch19-77041.html
  Note there is no need to be extra strict with the numeric ranges in here as
  the java.time constructors will take care of that for us."
  "((?:-)?[0-9]{4})-([0-9]{2})-([0-9]{2})")

(def iso8601-date-pattern
  "Defines the pattern for dates w/o times where an offset is still allowed on
  the end."
  (str iso8601-date-component-pattern iso8601-offset-pattern))

(def iso8601-date-re
  (re-pattern iso8601-date-pattern))

(def iso8601-time-pattern
  (str #?(:clj  "([0-9]{2}):([0-9]{2}):([0-9]{2})(?:\\.([0-9]{1,9}))?"
          :cljs "([0-9]{2}):([0-9]{2}):([0-9]{2})(?:\\.([0-9]{1,3}))?")
       iso8601-offset-pattern))

(def iso8601-time-re
  (re-pattern iso8601-time-pattern))

(def iso8601-datetime-pattern
  "JS: https://tc39.es/ecma262/#sec-date-time-string-format simplified ISO8601 HH:mm:ss.sssZ
   JVM: ISO8601 that supports nanosecond resolution."
  (str iso8601-date-component-pattern "T" iso8601-time-pattern))

(def iso8601-datetime-re
  (re-pattern iso8601-datetime-pattern))

(defn infer
  "Infers a default data type if not otherwise provided."
  ([x]
   (infer x nil))
  ([x lang]
   (cond
     (string? x)  (if lang
                    const/$rdf:langString
                    const/$xsd:string)
     (integer? x) const/$xsd:integer
     (number? x)  const/$xsd:double
     (boolean? x) const/$xsd:boolean)))

(defn infer-iri
  ([x]
   (some-> x infer iri/sid->iri))
  ([x lang]
   (some-> x (infer lang) iri/sid->iri)))

#?(:cljs
   (defn- left-pad
     [s pad len]
     (let [diff (- len (count s))]
       (if (< 0 diff)
         (-> diff
             (repeat pad)
             (concat s)
             (->> (apply str)))
         s))))

(defn- parse-iso8601-date
  "Parses string s into one of the following if it can (returns nil o/w):
  - JVM: either a java.time.OffsetDateTime (with time set to
         midnight) or a java.time.LocalDate if no timezone offset is present.
  - JS: a Javascript Date object with time set to midnight. NB: If you don't
        supply a timezone JS will assume it's in your current, local timezone
        according to your device."
  [s]
  (when-let [matches (re-matches iso8601-date-re s)]
    (let [date   (-> matches rest butlast)
          offset (last matches)
          [year month day] (map #?(:clj  #(Integer/parseInt %)
                                   :cljs #(left-pad % "0" 2))
                                date)]
      #?(:clj  (if offset
                 (OffsetDateTime/of year month day 0 0 0 0
                                    (ZoneOffset/of ^String offset))
                 (LocalDate/of ^int year ^int month ^int day))
         :cljs (js/Date. (str year "-" month "-" day "T00:00:00" offset))))))

(defn- parse-iso8601-time
  "Parses string s into one of the following if it can (returns nil o/w):
  - JVM: either a java.time.OffsetTime of a java.time.LocalTime
         if no timezone offset is present.
  - JS: a Javascript Date object with the date values set to January 1, 1970.
        NB: If you don't supply a timezone JS will assume it's in your current,
        local timezone according to your device."
  [s]
  #?(:clj
     (when-let [matches (re-matches iso8601-time-re s)]
       (if (peek matches)
         (OffsetTime/parse s)
         (LocalTime/parse s)))

     :cljs
     (when (re-matches iso8601-time-re s)
       (js/Date. (str "1970-01-01T" s)))))

(defn- parse-iso8601-datetime
  "Parses string s into one of the following:
  - JVM: either a java.time.OffsetDateTime or a java.time.LocalDateTime if no
         timezone offset if present.
  - JS: a Javascript Date object. NB: If you don't supply a timezone JS will
        assume it's in your current, local timezone according to your device."
  [s]
  #?(:clj
     (when-let [matches (re-matches iso8601-datetime-re s)]
       (if (peek matches)
         (OffsetDateTime/parse s)
         (LocalDateTime/parse s)))

     :cljs
     (when (re-matches iso8601-datetime-re s)
       (js/Date. s))))

(defn- coerce-boolean
  [value]
  (cond
    (boolean? value)
    value

    (string? value)
    (cond
      (= "true" (str/lower-case value))
      true

      (= "false" (str/lower-case value))
      false

      :else
      nil)))

(defn- coerce-decimal
  [value]
  (cond
    (string? value)
    #?(:clj  (try (bigdec value) (catch Exception _ nil))
       :cljs (let [n (js/parseFloat value)] (if (js/Number.isNaN n) nil n)))

    (integer? value)
    #?(:clj  (bigdec value)
       :cljs value)

    (float? value)
    ;; convert to string first to keep float precision explosion at bay
    #?(:clj  (bigdec (Float/toString value))
       :cljs value)

    (number? value)
    #?(:clj  (bigdec value)
       :cljs value)

    :else nil))

(defn- coerce-double
  [value]
  (cond
    (string? value)
    (case value
      "INF" #?(:clj  Double/POSITIVE_INFINITY
               :cljs js/Number.POSITIVE_INFINITY)
      "-INF" #?(:clj  Double/NEGATIVE_INFINITY
                :cljs js/Number.NEGATIVE_INFINITY)
      #?(:clj  (try (Double/parseDouble value) (catch Exception _ nil))
         :cljs (let [n (js/parseFloat value)] (if (js/Number.isNaN n) nil n))))

    (float? value)
    ;; convert to string first to keep float precision explosion at bay
    #?(:clj (Double/parseDouble (Float/toString value))
       :cljs value)

    (double? value)
    value

    (integer? value)
    #?(:clj  (Double/parseDouble (str value ".0"))
       :cljs value)

    :else
    #?(:clj (when (decimal? value) ;; our json parsing library turns decimals into BigDecimal
              (try (double value)
                   (catch Exception _
                     (throw (ex-info (str "xsd:double value exceeds maximum 64-bit float range: " value)
                                     {:status 400
                                      :error  :db/invalid-value})))))
       :cljs nil)))

(defn- coerce-float
  [value]
  (cond
    (string? value)
    (case value
      "INF" #?(:clj  Float/POSITIVE_INFINITY
               :cljs js/Number.POSITIVE_INFINITY)
      "-INF" #?(:clj  Float/NEGATIVE_INFINITY
                :cljs js/Number.NEGATIVE_INFINITY)
      #?(:clj  (try (Float/parseFloat value) (catch Exception _ nil))
         :cljs (let [n (js/parseFloat value)] (if (js/Number.isNaN n) nil n))))

    (float? value)
    value

    (double? value)
    #?(:clj (try (float value)
                 (catch Exception _
                   (throw (ex-info (str "xsd:float value exceeds maximum 32-bit float range: " value)
                                   {:status 400
                                    :error  :db/invalid-value}))))
       :cljs value)

    (integer? value)
    #?(:clj  (Float/parseFloat (str value ".0"))
       :cljs value)

    :else
    #?(:clj (when (decimal? value) ;; our json parsing library turns decimals into BigDecimal
              (try (float value)
                   (catch Exception _
                     (throw (ex-info (str "xsd:float value exceeds maximum 32-bit float range: " value)
                                     {:status 400
                                      :error  :db/invalid-value})))))
       :cljs nil)))

#?(:clj
   (defn- coerce-int-fn
     "Returns a fn for coercing int-like values (e.g. short, long) from strings and
     integers. Arguments are CLJ-only parse-str and cast-num fns (CLJS is always
     the same because in JS it's all just Numbers)."
     [parse-str cast-num]
     (fn [value]
       (cond
         (string? value)
         (try (parse-str value)
              (catch Exception _
                nil))

         (integer? value)
         (try (cast-num value)
              (catch Exception _
                nil))

         :else nil)))

   :cljs
   (defn- coerce-int
     [value]
     (cond
       (string? value)
       (when-not (str/includes? value ".")
         (let [n (js/parseInt value)]
           (when-not (js/Number.isNaN n)
             n)))

       (integer? value)
       value

       :else nil)))

(defn- coerce-integer
  [value]
  #?(:clj
     (let [coerce-fn (coerce-int-fn #(Integer/parseInt %) int)]
       (coerce-fn value))

     :cljs
     (coerce-int value)))

(defn- coerce-long
  [value]
  #?(:clj
     (let [coerce-fn (coerce-int-fn #(Long/parseLong %) long)]
       (coerce-fn value))

     :cljs
     (coerce-int value)))

(defn- coerce-short
  [value]
  #?(:clj
     (let [coerce-fn (coerce-int-fn #(Short/parseShort %) short)]
       (coerce-fn value))

     :cljs
     (coerce-int value)))

(defn- coerce-byte
  [value]
  #?(:clj
     (let [coerce-fn (coerce-int-fn  #(Byte/parseByte %) byte)]
       (coerce-fn value))

     :cljs
     (coerce-int value)))

(defn- coerce-normalized-string
  [value]
  (when (string? value)
    (str/replace value #"\s" " ")))

(defn- coerce-token
  [value]
  (when (string? value)
    (-> value
        (str/replace #"\s+" " ")
        str/trim)))

(defn- coerce-json
  [value]
  (try*
    (if (string? value)
      value
      (json-ld/normalize-data value))
    (catch* e
      (throw (ex-info (str "Unable to normalize value to json" value)
                      {:status 400
                       :error  :db/invalid-json}
                      e)))))

(defn- coerce-dense-vector
  [value]
  (try*
    (if (string? value)
      (-> (json/parse value nil)
          (vector.score/vectorize))
      (vector.score/vectorize value))
    (catch* e
      (log/error e "Unrecognized value for dense vector: " value)
      (throw (ex-info (str "Unrecognized value for dense vector: " value)
                      {:status 400
                       :error  :db/invalid-dense-vector}
                      e)))))

(defn- check-signed
  "Returns nil if required-type and n conflict in terms of signedness
  (e.g. unsignedInt but n is negative, nonPositiveInteger but n is greater than
  zero). Returns n otherwise."
  [n required-type]
  (when (number? n) ; these are all integer types, but this fn shouldn't care
    (uc/case required-type
      const/$xsd:positiveInteger
      (if (>= 0 n) nil n)

      (const/$xsd:nonNegativeInteger const/$xsd:unsignedInt
                                     const/$xsd:unsignedLong const/$xsd:unsignedByte)
      (if (> 0 n) nil n)

      const/$xsd:negativeInteger
      (if (<= 0 n) nil n)

      const/$xsd:nonPositiveInteger
      (if (< 0 n) nil n)

      const/$xsd:unsignedShort
      (when (>= 65535 n 0) n)

      ;; else
      n)))

(defn coerce
  "Given a value and required type, attempts to return a coerced value or nil (not coercible).
  We should be cautious about what we coerce, it is really a judgement decision in some
  circumstances. While we could coerce, e.g. numbers to strings, an exception is likely the most ideal behavior.
  Examples of things that seem OK to coerce are:
   - a string type to a date and/or time, assuming it meets the formatting
   - numbers in strings
   - the strings 'true' or 'false' to a boolean"
  [value required-type]
  (uc/case required-type
    (const/$xsd:string
     const/iri-string
     const/$rdf:langString
     const/iri-lang-string)
    (when (string? value)
      value)

    (const/$xsd:boolean
     const/iri-xsd-boolean)
    (coerce-boolean value)

    (const/$xsd:date
     const/iri-xsd-date)
    (cond (string? value)
          (parse-iso8601-date value)
          #?(:clj
             (instance? LocalDate value)
             :cljs
             (instance? js/Date value))
          value)

    (const/$xsd:dateTime
     const/iri-xsd-dateTime)
    (cond (string? value)
          (parse-iso8601-datetime value)
          ;; these values don't need coercion
          #?(:clj
             (or (instance? OffsetDateTime value)
                 (instance? LocalDateTime value))
             :cljs (instance? js/Date value))
          value)

    (const/$xsd:time
     const/iri-xsd-time)
    (cond (string? value)
          (parse-iso8601-time value)
          #?(:clj
             (or (instance? OffsetTime value)
                 (instance? LocalTime value))
             :cljs
             (instance? js/Date value))
          value)

    (const/$xsd:decimal
     const/iri-xsd-decimal)
    (coerce-decimal value)

    (const/$xsd:double
     const/iri-xsd-double)
    (coerce-double value)

    (const/$xsd:float
     const/iri-xsd-float)
    (coerce-float value)

    ;; ·maxInclusive· to be 2147483647 and ·minInclusive· to be -2147483648
    ;; https://www.w3.org/TR/xmlschema-2/#int
    (const/$xsd:int
     const/iri-xsd-int
     const/$xsd:unsignedShort ;; unsigned short will be outside of 'Short' value range
     const/iri-xsd-unsignedShort)
    (-> value coerce-integer (check-signed required-type))

    ;; xsd:integer and parent of long and others - different from xsd:int which is 32-bit
    (const/$xsd:integer
     const/iri-xsd-integer
     const/$xsd:long
     const/iri-long
     const/$xsd:nonNegativeInteger
     const/iri-xsd-nonNegativeInteger
     const/$xsd:unsignedLong
     const/iri-xsd-unsignedLong
     const/$xsd:positiveInteger
     const/iri-xsd-positiveInteger
     const/$xsd:unsignedInt ;; unsigned int can be outside of xsd:int max range
     const/iri-xsd-unsignedInt
     const/$xsd:nonPositiveInteger
     const/iri-xsd-nonPositiveInteger
     const/$xsd:negativeInteger
     const/iri-xsd-negativeInteger)
    (-> value coerce-long (check-signed required-type))

    (const/$xsd:short
     const/iri-xsd-short)
    (-> value coerce-short (check-signed required-type))

    (const/$xsd:byte
     const/iri-xsd-byte
     const/$xsd:unsignedByte
     const/iri-xsd-unsignedByte)
    (-> value coerce-byte (check-signed required-type))

    (const/$xsd:normalizedString
     const/iri-xsd-normalizedString)
    (coerce-normalized-string value)

    (const/$xsd:token
     const/iri-xsd-token
     const/$xsd:language
     const/iri-xsd-language)
    (coerce-token value)

    (const/$rdf:json
     const/iri-rdf-json)
    (coerce-json value)

    (const/$fluree:vector
     const/iri-vector)
    (coerce-dense-vector value)

    ;; else
    (if (or (string? value)
            (number? value)
            (boolean? value))
      value
      (throw (ex-info (str "Custom data types must be a string, number or boolean per JSON-LD spec. "
                           "Attempted custom datatype value of: " value)
                      {:status 400
                       :error :db/invalid-datatype})))))

(defn from-expanded
  "Returns a tuple of the value (possibly coerced from string) and the data type sid from
  an expanded json-ld value map. If type is defined but not a predefined data type, will
  return nil prompting downstream process to look up (or create) a custom data
  type. Value coercion is only attempted when a required-type is supplied."
  [db value-map]
  (let [value   (util/get-value value-map)
        type    (util/get-types value-map)
        type-id (if type
                  (or (get default-data-types type)
                      (iri/encode-iri db type))
                  (infer value))
        value*  (coerce value type-id)]
    (if (nil? value*)
      (throw (ex-info (str "Data type " (iri/sid->iri type-id)
                           " cannot be coerced from provided value: " value ".")
                      {:status 400 :error, :db/value-coercion}))
      [value* type-id])))

(defn coerce-value
  "Attempt to coerce the value into an in-memory instance of the supplied datatype. If no
  coercion for the datatype is known, nothing will be done. If a coercion does exist but
  fails, an error will be thrown."
  [value datatype]
  (let [value* (coerce value datatype)]
    (if (nil? value*)
      (let [dt-iri (iri/sid->iri datatype)]
        (throw (ex-info (str "Value " value " cannot be coerced to provided datatype: " (or dt-iri datatype) ".")
                        {:status 400 :error, :db/value-coercion})))
      value*)))
