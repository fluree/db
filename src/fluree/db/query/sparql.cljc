(ns fluree.db.query.sparql
  (:require #?(:clj  [instaparse.core :as insta :refer [defparser]]
               :cljs [instaparse.core :as insta :refer-macros [defparser]])
            #?(:clj [clojure.java.io :as io])
            #?(:cljs [fluree.db.util.cljs-shim :refer-macros [inline-resource]])
            [fluree.db.query.sparql.translator :as sparql.translator]
            [fluree.db.util.docs :as docs]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(def PN_CHARS_BASE
  "CLJ and CLJS use different (and incompatible) unicode character syntax, so in the regex
  for PN_CHARS_BASE needs to be platform specific."
  #?(:clj
     (slurp (io/resource "sparql.pn_chars_base.jvm.bnf"))
     :cljs
     (inline-resource "sparql.pn_chars_base.js.bnf")))

(def grammar
  (str
   #?(:clj  (slurp (io/resource "sparql.bnf"))
      :cljs (inline-resource "sparql.bnf"))
   PN_CHARS_BASE))

(def property-path-grammar
  (str
   #?(:clj  (slurp (io/resource "sparql-property-path.bnf"))
      :cljs (inline-resource "sparql-property-path.bnf"))
   PN_CHARS_BASE))

(defparser parser grammar)

(defparser path-parser property-path-grammar)

(defn parse-path-expr
  [s]
  (let [parsed (path-parser s)]
    (if (insta/failure? parsed)
      (do (log/debug (insta/get-failure parsed) "Property path expression failed to parse.")
          (throw (ex-info "Invalid property path expression. Must be a valid SPARQL property path expression, see https://www.w3.org/TR/sparql11-query/#pp-language"
                          {:status 400
                           :error :db/invalid-property-path
                           :path s})))
      parsed)))

(defn parse
  [sparql]
  (let [parsed (parser sparql)]
    (if (insta/failure? parsed)
      (do
        (log/debug (insta/get-failure parsed) "SPARQL query failed to parse")
        (throw (ex-info (str "Improperly formatted SPARQL query: " sparql " "
                             "Note: Fluree does not support all SPARQL features. "
                             "See here for more information: "
                             docs/error-codes-page "#query-sparql-improper")
                        {:status   400
                         :error    :db/invalid-query})))
      (do
        (log/trace "Parsed SPARQL query:" parsed)
        parsed))))

(defn ->fql
  [sparql]
  (let [parsed (parse sparql)]
    (sparql.translator/translate parsed)))

(defn sparql-format?
  [opts]
  (= :sparql (:format opts)))
