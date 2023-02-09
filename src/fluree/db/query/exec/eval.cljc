(ns fluree.db.query.exec.eval
  (:refer-clojure :exclude [compile rand])
  (:require [fluree.db.query.exec.group :as group]
            [fluree.db.query.exec.where :as where]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :refer [postwalk]])
  #?(:clj (:import (java.time Instant))))

(defn sum
  [coll]
  (reduce + 0 coll))

(defn avg
  [coll]
  (/ (sum coll)
     (count coll)))

(defn median
  [coll]
  (let [terms (sort coll)
        size  (count coll)
        med   (bit-shift-right size 1)]
    (cond-> (nth terms med)
      (even? size)
      (-> (+ (nth terms (dec med)))
          (/ 2)))))

(defn variance
  [coll]
  (let [mean (avg coll)
        sum  (sum (for [x coll
                        :let [delta (- x mean)]]
                    (* delta delta)))]
    (/ sum (count coll))))

(defn stddev
  [coll]
  (Math/sqrt (variance coll)))

(defn ceil
  [n]
  (cond (= n (int n)) n
        (> n 0) (-> n int inc)
        (< n 0) (-> n int)))

(def count-distinct
  (comp count distinct))

(defn floor
  [n]
  (cond (= n (int n)) n
        (> n 0) (-> n int)
        (< n 0) (-> n int dec)))

(def groupconcat concat)

(defn rand
  ([coll]
   (rand-nth coll))
  ([n coll]
   (vec (repeatedly n #(rand-nth coll)))))

(defn sample
  [n coll]
  (->> coll
       shuffle
       (take n)
       vec))

(def allowed-aggregates
  '#{abs as avg ceil count count-distinct distinct floor groupconcat
     median max min rand sample stddev str sum variance})

(defmacro coalesce
  "Evaluates args in order. The result of the first arg not to return error gets returned."
  ([] (throw (ex-info "COALESCE evaluation failed on all forms." {:status 400 :error :db/invalid-query})))
  ([arg] `(let [res# (try ~arg (catch Exception e# nil))]
            (if (nil? res#)
              (throw (ex-info "Coalesce evaluation failed on all forms." {:status 400 :error :db/invalid-query})) res#)))
  ([arg & args]
   `(let [res# (try ~arg (catch Exception e# nil))]
      (if (nil? res#)
        (coalesce ~@args) res#))))

(def bound some?)

(def ! not)

(defmacro &&
  "Equivalent to and"
  ([] true)
  ([x] x)
  ([x & next]
   `(let [and# ~x]
      (if and# (and ~@next) and#))))

(defmacro ||
  "Equivalent to or"
  ([] nil)
  ([x] x)
  ([x & next]
   `(let [or# ~x]
      (if or# or# (or ~@next)))))

(defn now
  []
  #?(:clj (.toEpochMilli (Instant/now))))

(defn strStarts
  [s substr]
  (str/starts-with? s substr))

(defn strEnds
  [s substr]
  (str/ends-with? s substr))

(def allowed-filters
  '#{&& || ! > < >= <= = + - * / and bound coalesce if nil?
     not not= now or re-find re-pattern strStarts strEnds})

(def allowed-symbols
  (set/union allowed-aggregates allowed-filters))

(defn variable?
  [sym]
  (and (symbol? sym)
       (-> sym
           name
           first
           (= \?))))

(defn symbols
  [code]
  (postwalk (fn [x]
              (if (coll? x)
                (apply set/union x)
                (if (symbol? x)
                  #{x}
                  #{})))
            code))

(defn variables
  "Returns the set of items within the arbitrary data structure `code` that
  are variables."
  [code]
  (->> code
       symbols
       (filter variable?)))

(def qualified-symbols
  '{abs         fluree.db.query.exec.eval/abs
    avg         fluree.db.query.exec.eval/avg
    bound       fluree.db.query.exec.eval/bound
    ceil        fluree.db.query.exec.eval/ceil
    coalesce    fluree.db.query.exec.eval/coalesce
    count       fluree.db.query.exec.eval/count-distinct
    floor       fluree.db.query.exec.eval/floor
    groupconcat fluree.db.query.exec.eval/groupconcat
    median      fluree.db.query.exec.eval/median
    now         fluree.db.query.exec.eval/now
    rand        fluree.db.query.exec.eval/rand
    sample      fluree.db.query.exec.eval/sample
    stddev      fluree.db.query.exec.eval/stddev
    strStarts   fluree.db.query.exec.eval/strStarts
    strEnds     fluree.db.query.exec.eval/strEnds
    sum         fluree.db.query.exec.eval/sum
    variance    fluree.db.query.exec.eval/variance
    !           fluree.db.query.exec.eval/!
    &&          fluree.db.query.exec.eval/&&
    ||          fluree.db.query.exec.eval/||})

(defn qualify
  [sym]
  (if (contains? allowed-symbols sym)
    (get qualified-symbols sym sym)
    (throw (ex-info (str "Query function references illegal symbol: " sym)
                    {:status 400, :error :db/invalid-query}))))

(defn coerce
  [code]
  (postwalk (fn [x]
              (if (and (symbol? x)
                       (not (variable? x)))
                (qualify x)
                x))
            code))



(defn bind-variables
  [soln-sym var-syms]
  (->> var-syms
       (mapcat (fn [v]
                 [v `(let [mch# (get ~soln-sym (quote ~v))
                           val# (::where/val mch#)
                           dt#  (::where/datatype mch#)]
                       (cond->> val#
                         (= dt# ::group/grouping)
                         (mapv ::where/val)))]))
       (into [])))

(defn compile
  [code]
  (let [qualified-code (coerce code)
        vars           (variables qualified-code)
        soln-sym       'solution
        bdg            (bind-variables soln-sym vars)]
    (eval `(fn [~soln-sym]
             (let ~bdg
               ~qualified-code)))))

(defn compile-filter
  [code var]
  (let [f        (compile code)
        soln-sym 'solution]
    (eval `(fn [~soln-sym ~var]
             (-> ~soln-sym
                 (assoc (quote ~var) {::where/val ~var})
                 ~f)))))
