(ns fluree.db.datatype
  (:require [fluree.db.constants :as const]
            [clojure.string :as str]
            [fluree.db.util.core :as util]))

#?(:clj (set! *warn-on-reflection* true))

(def default-data-types
  {"http://www.w3.org/2001/XMLSchema#anyURI"             const/$xsd:anyURI
   "http://www.w3.org/2001/XMLSchema#string"             const/$xsd:string
   "http://www.w3.org/2001/XMLSchema#boolean"            const/$xsd:boolean
   "http://www.w3.org/2001/XMLSchema#date"               const/$xsd:date
   "http://www.w3.org/2001/XMLSchema#dateTime"           const/$xsd:dateTime
   "http://www.w3.org/2001/XMLSchema#decimal"            const/$xsd:decimal
   "http://www.w3.org/2001/XMLSchema#double"             const/$xsd:double
   "http://www.w3.org/2001/XMLSchema#integer"            const/$xsd:integer
   "http://www.w3.org/2001/XMLSchema#long"               const/$xsd:long
   "http://www.w3.org/2001/XMLSchema#int"                const/$xsd:int
   "http://www.w3.org/2001/XMLSchema#short"              const/$xsd:short
   "http://www.w3.org/2001/XMLSchema#float"              const/$xsd:float
   "http://www.w3.org/2001/XMLSchema#unsignedLong"       const/$xsd:unsignedLong
   "http://www.w3.org/2001/XMLSchema#unsignedInt"        const/$xsd:unsignedInt
   "http://www.w3.org/2001/XMLSchema#unsignedShort"      const/$xsd:unsignedShort
   "http://www.w3.org/2001/XMLSchema#positiveInteger"    const/$xsd:positiveInteger
   "http://www.w3.org/2001/XMLSchema#nonPositiveInteger" const/$xsd:nonPositiveInteger
   "http://www.w3.org/2001/XMLSchema#negativeInteger"    const/$xsd:negativeInteger
   "http://www.w3.org/2001/XMLSchema#nonNegativeInteger" const/$xsd:nonNegativeInteger
   "http://www.w3.org/2001/XMLSchema#duration"           const/$xsd:duration
   "http://www.w3.org/2001/XMLSchema#gDay"               const/$xsd:gDay
   "http://www.w3.org/2001/XMLSchema#gMonth"             const/$xsd:gMonth
   "http://www.w3.org/2001/XMLSchema#gMonthDay"          const/$xsd:gMonthDay
   "http://www.w3.org/2001/XMLSchema#gYear"              const/$xsd:gYear
   "http://www.w3.org/2001/XMLSchema#gYearMonth"         const/$xsd:gYearMonth
   "http://www.w3.org/2001/XMLSchema#time"               const/$xsd:time
   "http://www.w3.org/2001/XMLSchema#normalizedString"   const/$xsd:normalizedString
   "http://www.w3.org/2001/XMLSchema#token"              const/$xsd:token
   "http://www.w3.org/2001/XMLSchema#language"           const/$xsd:language
   "http://www.w3.org/2001/XMLSchema#byte"               const/$xsd:byte
   "http://www.w3.org/2001/XMLSchema#unsignedByte"       const/$xsd:unsignedByte
   "http://www.w3.org/2001/XMLSchema#hexBinary"          const/$xsd:hexBinary
   "http://www.w3.org/2001/XMLSchema#base64Binary"       const/$xsd:base64Binary})


(defn infer
  "Infers a default data type if not otherwise provided."
  [x]
  (cond
    (string? x) const/$xsd:string
    (integer? x) const/$xsd:integer
    (number? x) const/$xsd:decimal
    (boolean? x) const/$xsd:boolean))

(defn coerced
  "Given a value and required type, attempts to return a coerced value or nil (not coercable).
  We should be cautious about what we coerce, it is really a judgement decision in some
  circumstances. While we could coerce, e.g. numbers to strings, an exception is likely the most ideal behavior.
  Examples of things that seem OK to coerce are:
   - a string type to a date, assuming it meets the formatting
   - a decimal like 3.0 to an integer
   - the strings 'true' or 'false' to a boolean"
  [value required-type]
  (util/case+ (int required-type)
    const/$xsd:string
    (if (string? value)
      value
      nil)

    const/$xsd:anyURI
    (if (string? value)
      value
      nil)

    const/$xsd:boolean
    (when (string? value)
      (cond
        (= "true" (str/lower-case value))
        true

        (= "false" (str/lower-case value))
        false

        :else
        nil))

    ;; TODO - other data types!
    ;; else
    value))

(defn from-expanded
  "Returns a data type sid from an expanded json-ld value map.
  If type is defined but not a predefined data type, will return nil
  prompting downstream process to look up (or create) a custom data type."
  [{:keys [type value] :as _value-map} required-type]
  (if type
    (let [type-id (get default-data-types type)]
      (when (and required-type
                 (not= required-type type-id))
        (throw (ex-info (str "Required data type " required-type
                             " does not match provided data type: " type ".")
                        {:status 400 :error :db/shacl-validation})))
      (let [value* (coerced value type-id)]
        (throw (ex-info (str "Provided data type " type
                             " does not match provided value: " value ".")
                        {:status 400 :error :db/shacl-validation}))
        [value* type-id]))
    (let [inferred-type (infer value)]
      (if required-type
        (if (= inferred-type required-type)
          [value inferred-type]
          (if-some [value* (coerced value required-type)]
            [value* required-type]
            (throw (ex-info (str "Required data type " required-type
                                 " cannot be coerced from provided value: " value ".")
                            {:status 400 :error :db/shacl-validation}))))
        [value inferred-type]))))