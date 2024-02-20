(ns fluree.db.query.parse.aggregate
  (:require [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [clojure.string :as str]
            [fluree.db.util.log :as log :include-macros true]))

#?(:clj (set! *warn-on-reflection* true))

(def read-str-fn #?(:clj read-string :cljs cljs.reader/read-string))

(defn safe-read-fn
  [code-str]
  (when-not (string? code-str)
    (throw (ex-info (code-str "Invalid function: " code-str)
                    {:status 400 :error :db/invalid-query})))
  (try*
    (let [code-str* (if (str/starts-with? code-str "#")
                      (subs code-str 1)
                      code-str)
          res       (read-str-fn code-str*)]
      (when-not (list? res)
        (throw (ex-info (code-str "Invalid function: " code-str)
                        {:status 400 :error :db/invalid-query})))
      res)
    (catch* e
            (log/warn "Invalid query function attempted: " code-str " with error message: " (ex-message e))
            (throw (ex-info (code-str "Invalid query function: " code-str)
                            {:status 400 :error :db/invalid-query})))))


(def built-in-aggregates
  (letfn [(sum [coll] (reduce + 0 coll))
          (avg [coll] (/ (sum coll) (count coll)))
          (median
            [coll]
            (let [terms (sort coll)
                  size  (count coll)
                  med   (bit-shift-right size 1)]
              (cond-> (nth terms med)
                      (even? size)
                      (-> (+ (nth terms (dec med)))
                          (/ 2)))))
          (variance
            [coll]
            (let [mean (avg coll)
                  sum  (sum (for [x coll
                                  :let [delta (- x mean)]]
                              (* delta delta)))]
              (/ sum (count coll))))
          (stddev
            [coll]
            (Math/sqrt (variance coll)))]
    {'abs            (fn [n] (max n (- n)))
     'avg            avg
     'ceil           (fn [n] (cond (= n (int n)) n
                                   (> n 0) (-> n int inc)
                                   (< n 0) (-> n int)))
     'count          count
     'count-distinct (fn [coll] (count (distinct coll)))
     'distinct       (fn [coll] (distinct coll))
     'floor          (fn [n]
                       (cond (= n (int n)) n
                             (> n 0) (-> n int)
                             (< n 0) (-> n int dec)))
     'groupconcat    concat
     'median         median
     'min            (fn
                       ([coll] (reduce (fn [acc x]
                                         (if (neg? (compare x acc))
                                           x acc))
                                       (first coll) (next coll)))
                       ([n coll]
                        (vec
                          (reduce (fn [acc x]
                                    (cond
                                      (< (count acc) n)
                                      (sort compare (conj acc x))
                                      (neg? (compare x (last acc)))
                                      (sort compare (conj (butlast acc) x))
                                      :else acc))
                                  [] coll))))
     'max            (fn
                       ([coll] (reduce (fn [acc x]
                                         (if (pos? (compare x acc))
                                           x acc))
                                       (first coll) (next coll)))
                       ([n coll]
                        (vec
                          (reduce (fn [acc x]
                                    (cond
                                      (< (count acc) n)
                                      (sort compare (conj acc x))
                                      (pos? (compare x (first acc)))
                                      (sort compare (conj (next acc) x))
                                      :else acc))
                                  [] coll))))
     'rand           (fn
                       ([coll] (rand-nth coll))
                       ([n coll] (vec (repeatedly n #(rand-nth coll)))))
     'sample         (fn [n coll]
                       (vec (take n (shuffle coll))))
     'stddev         stddev
     'str            str
     'sum            sum
     'variance       variance}))


(defn extract-aggregate-as
  "Returns as var symbol if 'as' function is used in an aggregate,
  e.g. (as (sum ?nums) ?sum).

  Checks that has 3 elements to the form, and the last element
  is a symbol that starts with a '?'. Else will throw."
  [as-fn-parsed]
  (when-not (and (= 3 (count as-fn-parsed))                 ;; e.g. (as (sum ?nums) ?sum) - will always have 3 elements
                 (symbol? (last as-fn-parsed)))
    (throw (ex-info (str "Invalid aggregate function using 'as': " (pr-str as-fn-parsed))
                    {:status 400 :error :db/invalid-query})))
  (last as-fn-parsed))


(defn parse-aggregate*
  [fn-parsed fn-str as]
  (let [list-count (count fn-parsed)
        [fun arg var] (cond (= 3 list-count)
                            [(first fn-parsed) (second fn-parsed) (last fn-parsed)]

                            (and (= 2 list-count) (= 'sample (first fn-parsed)))
                            (throw (ex-info (str "The sample aggregate function takes two arguments: n and a variable, provided: " fn-str)
                                            {:status 400 :error :db/invalid-query}))

                            (= 2 list-count)
                            [(first fn-parsed) nil (last fn-parsed)]

                            :else
                            (throw (ex-info (str "Invalid aggregate selection, provided: " fn-str)
                                            {:status 400 :error :db/invalid-query})))
        agg-fn     (if-let [agg-fn (built-in-aggregates fun)]
                     (if arg (fn [coll] (agg-fn arg coll)) agg-fn)
                     (throw (ex-info (str "Invalid aggregate selection function, provided: " fn-str)
                                     {:status 400 :error :db/invalid-query})))
        [agg-fn variable] (let [distinct? (and (coll? var) (= (first var) 'distinct))
                                variable  (if distinct? (second var) var)
                                agg-fn    (if distinct? (fn [coll] (-> coll distinct agg-fn))
                                                        agg-fn)]
                            [agg-fn variable])
        as'        (or as (symbol (str variable "-" fun)))]
    (when-not (and (symbol? variable)
                   (= \? (first (name variable))))
      (throw (ex-info (str "Variables used in aggregate functions must start with a '?'. Provided: " fn-str)
                      {:status 400 :error :db/invalid-query})))
    {:variable variable
     :as       as'
     :fn-str   fn-str
     :function agg-fn}))


(defn parse-aggregate
  "Parses an aggregate function string and returns map with keys:
  :variable - input variable symbol
  :as - return variable/binding name
  :fn-str - original function string, for use in reporting errors
  :function - executable function."
  [aggregate-input]
  (let [list-agg  (if (string? aggregate-input)
                    (safe-read-fn aggregate-input)
                    aggregate-input)
        as?       (= 'as (first list-agg))
        func-list (if as?
                    (second list-agg)
                    list-agg)
        _         (when-not (coll? func-list)
                    (throw (ex-info (str "Invalid aggregate selection. As can only be used in conjunction with other functions. Provided: " aggregate-input)
                                    {:status 400 :error :db/invalid-query})))
        as        (when as?
                    (extract-aggregate-as list-agg))]
    (parse-aggregate* func-list aggregate-input as)))
