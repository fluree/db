(ns fluree.db.query.sparql
  (:require #?(:clj [clojure.java.io :as io])
            #?(:clj  [instaparse.core :as insta :refer [defparser]]
               :cljs [instaparse.core :as insta :refer-macros [defparser]])
            #?(:cljs [fluree.db.util.cljs-shim :refer-macros [inline-resource]])
            [fluree.db.query.sparql.translator :as sparql.translator]
            [fluree.db.util.docs :as docs]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(def grammar #?(:clj  (io/resource "sparql.bnf")
                :cljs (inline-resource "sparql.js.bnf")))

(defparser parser grammar)

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
