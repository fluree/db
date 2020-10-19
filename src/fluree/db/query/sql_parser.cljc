(ns fluree.db.query.sql-parser
  (:require [clojure.string :as str]
    #?(:clj [clojure.java.io :as io])
    #?(:clj [instaparse.core :as insta]
       :cljs [instaparse.core :as insta :refer-macros [defparser]])))


(def sql (insta/parser (io/resource "sql-92.bnf") :input-format :ebnf :start :query-specification))

(defn select-query?
  [parsed]
  (and (= (first parsed) :query-specification)
       (= (second parsed) "SELECT")))

(defn select-list?
  [parsed-segment]
  (= (first parsed-segment) :select-list))

(defn select-distinct?
  [parsed-segment]
  (= (second parsed-segment) "DISTINCT"))

(defn parse-select-sublist
  [parsed-segment]
  "[:select-sublist [:column-name \"i\" \"d\"]]"
  (->> parsed-segment second rest (str/join "")))

(defn parse-select-list
  [parsed-segment]
  (loop [[seg & r] (rest parsed-segment)
         select-vec []]
    (if seg
      (if (= (first seg) :asterisk)
        [:*]
        (let [add-item (parse-select-sublist seg)]
          (recur r (conj select-vec add-item))))
      select-vec)))

(defn parse-table-reference
  [parsed-seg]
  (->> parsed-seg second rest (str/join "")))

(defn table-exp?
  [parsed-seg]
  (= (first parsed-seg) :table-expression))

(defn parse-search-condition
  [parsed-seg]
  ["?1" "for/example" "?2"])


(defn parse-table-expression
  [parsed-segment]
  (loop [[seg & r] (rest parsed-segment)
         res {}]
    (if seg
      (condp = (first seg)
        :from-clause (recur r (assoc res :from (parse-table-reference (-> seg second))))
        :search-condition (recur r (update res :where conj (parse-search-condition (-> seg second))))
        ;else
        (throw (ex-info (str "Provided SQL format currently supported by the transpiler. Provided: " parsed-segment)
                        {:status 400
                         :error  :db/invalid-query}))) res)))

(defn sql->analytical
  [query]
  (let [parsed           (sql query)
        _                (when-not (select-query? parsed)
                           (throw (ex-info (str "Non-select SQL queries are not currently supported by the transpiler. Provided: " query)
                                           {:status 400
                                            :error  :db/invalid-query})))
        ;; Check if select distinct
        parsed-n-2       (nth parsed 2)
        set-quantifier?  (= (first parsed-n-2) :set-quantifier)
        select           (if (select-distinct? parsed-n-2) :selectDistinct :select)
        ;; Then, parse select list
        select-list      (if set-quantifier? (nth parsed 3) parsed-n-2)
        _                (when-not (select-list? select-list)
                           (throw (ex-info (str "Provided SQL format currently supported by the transpiler. Provided: " query)
                                           {:status 400
                                            :error  :db/invalid-query})))
        select-list-vec  (parse-select-list select-list)
        table-expression (if set-quantifier? (nth parsed 4) (nth parsed 3))
        _                (when-not (table-exp? table-expression)
                           (throw (ex-info (str "Provided SQL format currently supported by the transpiler. Provided: " query)
                                           {:status 400
                                            :error  :db/invalid-query})))
        from+where       (parse-table-expression table-expression)]
    (merge {select select-list-vec} from+where)))


(comment


  (sql "SELECT DISTINCT id, apple FROM Customer WHERE country = \"Sweden\" AND apple = 3")
  (sql "SELECT * FROM fruit")
  (sql "SELECT DISTINCT Id, apple FROM Customer")


  (select-list? select-list)

  (sql->analytical "SELECT * FROM fruit")
  (sql->analytical "SELECT DISTINCT Id, apple FROM Customer ")
  (sql->analytical "SELECT DISTINCT id, apple FROM Customer WHERE country = \"Sweden\" AND apple = 3")


  (def res (insta/parses sql "SELECT DISTINCT * FROM fruit"))
  res
  (count (first res))

  (sql "SELECT DISTINCT id, apple FROM Customer")
  (sql "SELECT DISTINCT * FROM fruit")

  (count res)
  (def four-parter (filter #(when (= 4 (count %)) %) res))
  (first four-parter))



